"""
Unit test module for audit
"""

import unittest
import audit

from unittest.mock import Mock
from datetime import datetime


class TestAuditor(unittest.TestCase):
    '''
    Main test class test majority of the scenarios for the audit process
    '''

    def setUp(self):
        self.auditor = audit.Auditor('/some_dir/folder', datetime(2016, 1, 9, 5))

    def test_get_state_scenario1(self):
        ''' Test get state will pull the correct value for a field when process date is previous to changeTime '''
        audit_data = [
            {"changeTime": "2016-01-09T20:44:00.001145", "after": {"ambientTemp": 81.0}, "before": {"ambientTemp": 82.0}}
        ]
        audit_files = ['/some_dir/folder/2016/01/09.jsonl.gz']

        # Mock functions to prevent code from trying to access local data
        self.auditor.get_audit_data = Mock()
        self.auditor.get_audit_files = Mock()
        self.auditor.get_audit_data.return_value = (x for x in audit_data)
        self.auditor.get_audit_files.return_value = audit_files

        state = self.auditor.get_state('ambientTemp')
        self.assertEqual(state, 82.0)

    def test_get_state_scenario2(self):
        ''' Test get state will pull the correct value for a field when process date is after changeTime '''
        audit_data = [
            {"changeTime": "2016-01-09T00:44:00.001145", "after": {"ambientTemp": 81.0}, "before": {"ambientTemp": 82.0}}
        ]
        audit_files = ['/some_dir/folder/2016/01/09.jsonl.gz']

        # Mock functions to prevent code from trying to access local data
        self.auditor.get_audit_data = Mock()
        self.auditor.get_audit_files = Mock()
        self.auditor.get_audit_data.return_value = (x for x in audit_data)
        self.auditor.get_audit_files.return_value = audit_files

        state = self.auditor.get_state('ambientTemp')
        self.assertEqual(state, 81.0)

    def test_get_state_scenario3(self):
        ''' Test failure when field has been deleted before the process date '''
        failure_caught = False
        audit_data = [
            {"changeTime": "2016-01-09T00:44:00.001145", "after": {}, "before": {"ambientTemp": 82.0}}
        ]
        audit_files = ['/some_dir/folder/2016/01/09.jsonl.gz']

        # Mock functions to prevent code from trying to access local data
        self.auditor.get_audit_data = Mock()
        self.auditor.get_audit_files = Mock()
        self.auditor.get_audit_data.return_value = (x for x in audit_data)
        self.auditor.get_audit_files.return_value = audit_files

        try:
            state = self.auditor.get_state('ambientTemp')
        except audit.MissingFieldException:
            failure_caught = True

        self.assertTrue(failure_caught)

    def test_get_state_scenario4(self):
        ''' Test failure when field has been created after the process date '''
        failure_caught = False
        audit_data = [
            {"changeTime": "2016-01-09T20:44:00.001145", "after": {"ambientTemp": 81.0}, "before": {}}
        ]
        audit_files = ['/some_dir/folder/2016/01/09.jsonl.gz']

        # Mock functions to prevent code from trying to access local data
        self.auditor.get_audit_data = Mock()
        self.auditor.get_audit_files = Mock()
        self.auditor.get_audit_data.return_value = (x for x in audit_data)
        self.auditor.get_audit_files.return_value = audit_files

        try:
            state = self.auditor.get_state('ambientTemp')
        except audit.MissingFieldException:
            failure_caught = True

        self.assertTrue(failure_caught)

    def test_get_state_scenario5(self):
        ''' Test case where field does not exist in process date file so data must be extracted from previous file '''
        audit_data1 = [
            {"changeTime": "2016-01-09T20:44:00.001145", "after": {"schedule": True}, "before": {"schedule": False}}
        ]
        audit_data2 = [
            {"changeTime": "2016-01-08T20:44:00.001145", "after": {"ambientTemp": 81.0}, "before": {"ambientTemp": 88.0}}
        ]
        audit_files = ['/some_dir/folder/2016/01/08.jsonl.gz', '/some_dir/folder/2016/01/09.jsonl.gz']

        # Mock functions to prevent code from trying to access local data
        self.auditor.get_audit_data = Mock()
        self.auditor.get_audit_files = Mock()
        self.auditor.get_audit_data.side_effect = [(x for x in audit_data1), (y for y in audit_data2)]
        self.auditor.get_audit_files.return_value = audit_files
        state = self.auditor.get_state('ambientTemp')
        self.assertEqual(81.0, state)
        

class TestS3Auditor(unittest.TestCase):
    '''
    Test class for s3 auditor specific scenarios 
    '''

    def setUp(self):
        self.auditor = audit.S3Auditor('s3://some-bucket/some_path/sub_folder', datetime(2016, 1, 9, 5))

    def test_bucket_name(self):
        ''' Confirm that the bucket name is as expected '''
        self.assertEqual(self.auditor.s3_bucket, 'some-bucket')

    def test_s3_path(self):
        ''' Confirm s3 path is as expected '''
        self.assertEqual(self.auditor.s3_path, 'some_path/sub_folder')


if __name__ == "__main__":
    unittest.main()
