"""
# get a listing by matching key prefix
# might be good to use prefix for YYYY or YYYYMM or YYYMMDD
aws s3api list-objects-v2 --bucket opera-ecmwf --prefix raw/20230201

# start the listing after a given key
aws s3api list-objects-v2 --bucket opera-ecmwf --prefix raw/20230201 --start-after raw/20230201/A2/A2D02010000020100001

# return a filtered response. only the keys as a list[str] JSON
aws s3api list-objects-v2 --bucket opera-ecmwf --prefix raw/20230201 --query Contents[].Key

# final
aws s3api list-objects-v2 --bucket opera-ecmwf --prefix raw/20230201 --start-after raw/20230201/A2/A2D02010000020100001 --query Contents[].Key

"""
import argparse
import io
import json
import logging.handlers
import sys
from typing import Optional, Literal

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--target-bucket", required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--s3-keys", nargs="+", default=[], help="list of S3 object keys.")
    group.add_argument("--s3-keys-file", "-f", type=argparse.FileType(), help="file housing a list of S3 object keys. See `--s3-keys`. Supports JSON and plain text formats (1 S3 key per line).")

    parser.add_argument('--log-level', default='INFO', choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'))

    parser.add_argument('--smoke-test', default=False, action='store_true')
    parser.add_argument('--dev-test', default=False, action='store_true')

    return parser


def init_logging(level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']):
    log_file_format = "%(asctime)s %(levelname)7s %(name)13s:%(filename)19s:%(funcName)22s:%(lineno)3s - %(message)s"
    log_format = "%(levelname)s: %(relativeCreated)7d %(process)d %(processName)s %(thread)d %(threadName)s %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s"
    logging.basicConfig(level=level, format=log_format, force=True)

    rfh1 = logging.handlers.RotatingFileHandler('subsetter.log', mode='a', maxBytes=100 * 2 ** 20, backupCount=10)
    rfh1.setLevel(logging.INFO)
    rfh1.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh1)

    rfh2 = logging.handlers.RotatingFileHandler('subsetter-error.log', mode='a', maxBytes=100 * 2 ** 20, backupCount=10)
    rfh2.setLevel(logging.ERROR)
    rfh2.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh2)


def to_s3_keys(*, args):
    s3_keys: list[str]
    s3_keys_file: Optional[io.TextIOWrapper]

    if args.s3_keys:
        s3_keys = args.s3_keys
    elif args.s3_keys_file:
        s3_keys_file = args.s3_keys_file
        with s3_keys_file:
            try:
                s3_keys = json.load(s3_keys_file)
            except Exception:
                s3_keys_file.seek(0)
                s3_keys = [line.strip() for line in s3_keys_file.readlines() if line.strip()]
    else:
        raise AssertionError()
    return s3_keys


if __name__ == '__main__':
    logger.info("START")

    logger.info(f"{sys.argv=}")

    parser = create_parser()
    args = parser.parse_args(sys.argv[1:])
    log_level = args.log_level

    init_logging(log_level)

    bucket_name = args.bucket
    target_bucket_name = args.target_bucket
    s3_keys = to_s3_keys(args=args)

    import subsetter
    subsetter.main(bucket_name=bucket_name, target_bucket_name=target_bucket_name, s3_keys=s3_keys)

    print("END")
