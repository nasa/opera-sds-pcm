import argparse
import logging
import sys


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true", default=False)

    parser.add_argument("--coverage-target", type=int, default=100)
    parser.add_argument("--required-min-age-minutes-for-partial-burstsets", type=int, default=0)
    parser.add_argument("--mgrs-set_id-acquisition-ts-cycle-indexes", nargs="*")

    args = parser.parse_args(sys.argv[1:])

    loglevel = "DEBUG" if args.verbose else "INFO"
    logging.basicConfig(level=loglevel, format="%(asctime)s %(levelname)7s %(name)13s:%(filename)19s:%(funcName)22s:%(lineno)3s - %(message)s")
    logger = logging.getLogger(__name__)
    logger.info("Log level set to " + loglevel)

    from data_subscriber.rtc import evaluator
    results = evaluator.main(**vars(args))
    from pprint import pprint
    pprint(results)
