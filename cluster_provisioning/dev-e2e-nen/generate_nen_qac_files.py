import sys
import os
import logging
import re
import hashlib
import boto3


cur_file_base = __file__.split(".")[0]
logging.basicConfig(
    filename="%s.log" % cur_file_base,
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)


class QacNenHandler:
    def __init__(self, src_bucket, dest_bucket):
        self.s3 = boto3.resource("s3")
        self.src_bucket = self.s3.Bucket(src_bucket)
        self.dest_bucket = self.s3.Bucket(dest_bucket)

    def list_files(self, prefix):
        return self.src_bucket.objects.filter(Prefix=prefix)

    def download_nen_from_s3(self, key, filename):
        """
        :param key: str
        :param filename: str name of file downloaded locally
        :return: None
        """
        self.src_bucket.download_file(key, filename)

    def upload_qac_to_s3(self, local, s3_path):
        self.dest_bucket.upload_file(local, s3_path)


def generate_md5_qac_file(file_name):
    """
    generates md5 hash given a file
    :param file_name:
    :return: str, the generated ms5 hash
    """
    hash_md5 = hashlib.md5()
    with open(file_name, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    qac_file = file_name + ".qac"
    with open(qac_file, "w") as f:
        f.write(hash_md5.hexdigest())

    return qac_file, hash_md5.hexdigest()


if __name__ == "__main__":
    DEST_BUCKET = "opera-sds-testdata"
    PREFIX = "LSAR/nen/2020/168"

    env = sys.argv[1]  # opera-$1-cc-fwd-$2
    venue = sys.argv[2]

    UPLOAD_BUCKET = "opera-%s-isl-fwd-%s" % (env, venue)
    UPLOAD_PREFIX = "qac"

    handler = QacNenHandler(DEST_BUCKET, UPLOAD_BUCKET)

    counter = 0
    for o in handler.list_files(PREFIX):
        m = re.match(r".+.vc[0-9]+", o.key)
        if m is not None:
            nen_file = o.key.split("/")[-1]

            handler.download_nen_from_s3(o.key, nen_file)  # download nen file to local
            qac_file_name, hash_val = generate_md5_qac_file(nen_file)

            dest_path = os.path.join(UPLOAD_PREFIX, qac_file_name)

            handler.upload_qac_to_s3(qac_file_name, dest_path)  # upload qac file to s3
            os.remove(nen_file)  # remove local nen file, too much space
            os.remove(qac_file_name)

            logging.info("%s - %s" % (qac_file_name, hash_val))

            counter += 1
            if counter % 100 == 0:
                print("%d qac files generated" % counter)
