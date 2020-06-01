"""
Module provides tools for extracting device data
"""
import os
import json
import logging
import boto3

from datetime import datetime
from typing import Iterator, Dict, Optional, List, Tuple
from io import BytesIO
from gzip import GzipFile
from dateutil.parser import parse


class MissingFieldException(Exception):
    '''
    Exception for missing field exception
    '''
    pass


class Auditor(object):
    '''
    Class used as utility for extracting thermostat data from local drive
    '''

    def __init__(self, path: str, process_date: datetime):
        self.path = path
        self.process_date = process_date

    @property
    def process_folder(self) -> str:
        ''' Top level directory for files '''
        return self.path

    def parse_bytes(self, file_buffer: BytesIO) -> Iterator[Dict]:
        ''' Returns a dict representation of individual records in a particular thermostat file '''
        with GzipFile(fileobj=file_buffer) as gz_bytes:
            for record in gz_bytes:
                data = json.loads(record.decode('utf-8').strip())
                yield data

    def get_audit_data(self, audit_file: str) -> Iterator[Dict]:
        ''' Get thermostat data for a local file '''
        with open(audit_file, 'rb') as fd:
            file_buffer = BytesIO(fd.read())

        return self.parse_bytes(file_buffer)

    def _has_field(self, field: str, record: Dict) -> bool:
        ''' Check if record has the field '''
        return field in record.get('before', {}) or field in record.get('after', {})

    def _get_smallest_delta(self, r1: Dict, r2: Dict) -> Dict:
        ''' Returns record with the smallest time detla between the changeTime and the process_date '''
        r1_delta = (parse(r1['changeTime']) - self.process_date).total_seconds()
        r2_delta = (parse(r2['changeTime']) - self.process_date).total_seconds()

        smallest = r1
        if abs(r1_delta) > abs(r2_delta):
            smallest = r2

        return smallest

    def find_closest_record(self, field: str, file_names: List) -> Optional[Dict]:
        ''' Tries to find the record with the closest changeTime to the process date that contains the field '''
        saved_record = None
        for fn in file_names:
            for record in self.get_audit_data(fn):
                if self._has_field(field, record):
                    saved_record = self._get_smallest_delta(record, saved_record or record)
            if saved_record is not None:
                break

        return saved_record

    def get_state(self, field: str) -> Dict:
        ''' Get the state of a field in an audit file
        based on the record that has the closest changeTime relative
        to the process_date
        '''
        main_file, before_files, after_files = self.get_traversal_files()

        saved_record = self.find_closest_record(field, [main_file])

        # if saved_records is None the field was not found in the main file so check previous files
        if saved_record is None:
            saved_record = self.find_closest_record(field, before_files)

        # if saved_records is None the field was still not found check the files that came after the main file
        if saved_record is None:
            saved_record = self.find_closest_record(field, after_files)

        # if saved_records is None at this point the field could not be found
        if saved_record is None:
            raise MissingFieldException(f'The field "{field}" could not be found.')

        if self.process_date >= parse(saved_record['changeTime']):
            field_value = saved_record.get('after', {}).get(field, None)
            if field_value is None:
                raise MissingFieldException(f'The field "{field}" was deleted before the process date.')
        else:
            field_value = saved_record.get('before', {}).get(field, None)
            if field_value is None:
                raise MissingFieldException(f'The field "{field}" had not yet been created at the time of the process date.')

        return field_value

    def get_audit_files(self) -> List:
        ''' Get list of all available audit files '''
        file_paths = []

        def build_file_paths(path: str):
            for child in os.listdir(path):
                child_path = os.path.join(path, child)
                if os.path.isdir(child_path):
                    build_file_paths(child_path)
                elif path != self.path and child_path.endswith('.jsonl.gz'):
                    file_paths.append(child_path)

        build_file_paths(self.path)
        return sorted(file_paths)

    def get_traversal_files(self) -> Tuple:
        '''
        Generate 3 items
        main file - file that is closest to the process date
        before files - files (in decending order) that are before the process date
        after files - files that are after the process date
        '''
        audit_files = self.get_audit_files()
        min_diff = float('inf')
        main_idx = 0

        for i, file_name in enumerate(audit_files):
            file_date = parse(file_name.replace(self.process_folder, '').replace('/', '').replace('.jsonl.gz', ''))
            date_diff = abs((self.process_date - file_date).total_seconds())
            if date_diff <= min_diff:
                min_diff = date_diff
                main_idx = i
            else:
                break  # because audit files are sorted no need to continue search if date_diff is greater than min_diff

        main_file = audit_files[main_idx]
        before_files = sorted(audit_files[:main_idx], reverse=True)
        after_files = audit_files[1 + main_idx:]

        return main_file, before_files, after_files


class S3Auditor(Auditor):
    '''
    Class used as utility for extracting thermostat data from s3 location
    '''

    def __init__(self, path: str, process_date: datetime):
        super().__init__(path, process_date)
        self.client = boto3.client('s3')

    @property
    def process_folder(self) -> str:
        ''' Top level directory for files '''
        return self.s3_path

    @property
    def s3_bucket(self) -> str:
        ''' S3 bucket for audit files '''
        return self.path[5:].split('/')[0]

    @property
    def s3_path(self) -> str:
        ''' S3 path for audit files '''
        path_attributes = self.path[5:].split('/')
        return '/'.join(path_attributes[1:])

    def get_audit_data(self, audit_file: str) -> Iterator[Dict]:
        ''' Get thermostat data from s3 file '''
        stream = self.client.get_object(
            Bucket=self.s3_bucket,
            Key=audit_file
        )['Body']

        file_buffer = BytesIO(stream.read())

        return self.parse_bytes(file_buffer)

    def get_audit_files(self) -> List:
        ''' Get list of all available audit files '''
        response = self.client.list_objects_v2(
            Bucket=self.s3_bucket,
            Prefix=self.s3_path
        )

        return sorted(c['Key'] for c in response['Contents'] if c['Key'].endswith('.jsonl.gz'))


def replay(fields: List[str], source_folder: str, process_date_str: str) -> Dict:
    ''' Returns the state of one or more top level fields at a given moment in time '''
    process_date = parse(process_date_str)

    if not source_folder.startswith('s3'):
        auditor = Auditor(source_folder, process_date)
    else:
        auditor = S3Auditor(source_folder, process_date)

    state = {}
    for fld in fields:
        try:
            field_value = auditor.get_state(fld)
            state[fld] = field_value
        except MissingFieldException as ex:
            logging.warning(ex)

    return {'state': state, 'ts': process_date_str}
