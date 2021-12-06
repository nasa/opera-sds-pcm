import sys
import os
import re
import boto3


if __name__ == "__main__":
    bucket_name = sys.argv[1]  # $1

    SRC_BUCKET = "opera-sds-testdata"
    UPLOAD_BUCKET = bucket_name
    UPLOAD_PREFIX = "tlm"

    s3 = boto3.resource("s3")
    testdata_bucket = s3.Bucket(name="opera-sds-testdata")

    target_bucket = s3.Bucket(UPLOAD_BUCKET)

    count = 0
    for o in testdata_bucket.objects.filter(Prefix="LSAR/nen/2020/168"):
        key = o.key
        m = re.match(r".+.vc[0-9]{2,}$", key)
        if m is not None:
            obj = key.split("/")[-1]
            vc_type = obj.split(".")[-1]

            copy_src = {"Bucket": SRC_BUCKET, "Key": o.key}
            target_vc_key = os.path.join(UPLOAD_PREFIX, obj)
            target_bucket.copy(copy_src, target_vc_key)

            target_qac_file = os.path.join(UPLOAD_PREFIX, obj + ".qac")
            copy_src = {"Bucket": SRC_BUCKET, "Key": o.key + ".qac"}
            target_bucket.copy(copy_src, target_qac_file)

            count += 1
            if count % 25 == 0:
                print("copied %d .vc and .qac files" % count)
