import argparse
import asyncio
import logging
import sys

from data_subscriber.rtc import evaluator

parser = argparse.ArgumentParser()
parser.add_argument("verbose", action="store_true", default=False)
args = parser.parse_args(sys.argv[1:])

loglevel = "DEBUG" if args.verbose else "INFO"
logging.basicConfig(level=loglevel)
logger = logging.getLogger(__name__)
logger.info("Log level set to " + loglevel)

if __name__ == '__main__':
    asyncio.run(evaluator.run(sys.argv))
