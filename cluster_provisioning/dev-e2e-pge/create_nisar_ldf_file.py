import sys
import os
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

    ldf_file_name = (
        "NISAR_S198_ASF_AS4_M00_P00102_R00_C00_G00_%d_%03d_%02d_%02d_%02d_000000000.ldf"
        % (year, doy, hr, minute, sec,)
    )
    print("LDF file name: %s" % ldf_file_name)
    return ldf_file_name


if __name__ == "__main__":
    bucket_name = sys.argv[1]  # opera-$1-cc-fwd-$2
    dir = sys.argv[2]

    UPLOAD_BUCKET = bucket_name
    UPLOAD_PREFIX = "ldf"

    files = []
    vc_file_types = set()

    s3 = boto3.resource("s3")

    ldf_file_name = create_ldf_file_name()

    for filename in os.listdir(dir):
        if not filename.endswith(".yaml"):
            files.append(filename)

    ldf = {
        "mission": "opera",
        "scid": 198,
        "station": "asf",
        "antenna": "as4",
        "pass": 114,
        "mode": 0,
        "receiver": 10,
        "channel": 10,
        "group": 10,
        "numFiles": 4,
        "files": {},
    }

    vc_dict = {
        "name": "file_name",
        "md5": "ye1d87/BnXXsbBgmR9x/Yg==",
        "size": 207400000,
        "status": "success",
    }

    for f in files:
        vc_dict["name"] = f
        ldf["files"][f] = vc_dict
    with open(ldf_file_name, "w") as f:
        json.dump(ldf, f, indent=2)

    print("generated %s" % ldf_file_name)

    isl_bucket = s3.Bucket(name=UPLOAD_BUCKET)
    dest = os.path.join(UPLOAD_PREFIX, ldf_file_name)

    print("uploading %s to %s" % (ldf_file_name, dest))
    isl_bucket.upload_file(ldf_file_name, dest)

    print("generated and uploaded ldf file %s" % ldf_file_name)
