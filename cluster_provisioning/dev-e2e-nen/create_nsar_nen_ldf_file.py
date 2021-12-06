import sys
import os
import re
import json
import datetime
import boto3


def create_ldf_file_name():
    now = datetime.datetime.now()
    year = now.year
    tt = now.timetuple()
    doy = tt.tm_yday

    # ASF_[mission]_dayofyear_hh_mm_ss.ldf
    ts = now.time()
    hr = ts.hour
    minute = ts.minute
    sec = ts.second

    ldf_file_name = "ASF_NISAR_%d_%03d_%02d_%02d_%02d.ldf" % (
        year,
        doy,
        hr,
        minute,
        sec,
    )
    print("LDF file name: %s" % ldf_file_name)
    return ldf_file_name


if __name__ == "__main__":
    bucket_name = sys.argv[1]  # opera-$1-cc-fwd-$2

    UPLOAD_BUCKET = bucket_name
    UPLOAD_PREFIX = "ldf"

    files = []
    vc_file_types = set()

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(name="opera-sds-testdata")  # opera-dev-isl-fwd-dustinlo

    for o in bucket.objects.filter(Prefix="LSAR/nen/2020/168"):
        key = o.key
        m = re.match(r".+.vc[0-9]{2,}$", key)
        if m is not None:
            obj = key.split("/")[-1]
            vc_type = obj.split(".")[-1]
            vc_file_types.add(vc_type)
            files.append(obj)

    ldf_file_name = create_ldf_file_name()

    ldf = {"id": ldf_file_name, "rrst_files": list(files)}

    with open(ldf_file_name, "w") as f:
        json.dump(ldf, f, indent=2)

    print("generated %s" % ldf_file_name)

    isl_bucket = s3.Bucket(name=UPLOAD_BUCKET)
    dest = os.path.join(UPLOAD_PREFIX, ldf_file_name)

    print("uploading %s to %s" % (ldf_file_name, dest))
    isl_bucket.upload_file(ldf_file_name, dest)

    print("generated and uploaded ldf file %s" % ldf_file_name)
