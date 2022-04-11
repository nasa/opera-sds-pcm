import logging

from dotenv import dotenv_values

logging.getLogger("elasticsearch").setLevel("WARN")
logging.getLogger("botocore").setLevel("WARN")

config = dotenv_values(".env")
