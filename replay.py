'''
Command line module
'''
from argparse import ArgumentParser

import audit
import logging


def configure_logging() -> None:
    ''' Configure logging for replay output '''
    FORMAT = '{%(filename)s:%(lineno)d} - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT)


if __name__ == "__main__":
    configure_logging()

    parser = ArgumentParser(
        prog="replay",
        description="Prints the state of one or more top level fields"
    )

    parser.add_argument(
        '--field',
        help="audit field to check against",
        action='append'
    )

    parser.add_argument(
        'source_path',
        help="location of audit file"
    )

    parser.add_argument(
        'date_str',
        help="date string for process date"
    )

    args = parser.parse_args()

    audit_details = audit.replay(args.field, args.source_path, args.date_str)
    print(audit_details)
