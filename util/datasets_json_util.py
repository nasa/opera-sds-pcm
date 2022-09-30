from pathlib import PurePath


def find_publish_location_s3(datasets_json, dataset_type):
    """Example location: "s3://{{ DATASET_S3_ENDPOINT }}:80/{{ DATASET_BUCKET }}/products/{id}"
    """
    publish_location = None
    for dataset in datasets_json["datasets"]:
        if dataset["type"] == dataset_type:
            # Example location: "s3://{{ DATASET_S3_ENDPOINT }}:80/{{ DATASET_BUCKET }}/products/{id}"
            publish_location = dataset["publish"]["location"]
            break

    if publish_location is None:
        raise Exception("s3 bucket not found")
    return PurePath(publish_location)


def find_dataset_s3_endpoint(datasets_json, dataset_type):
    """Example location: "s3://{{ DATASET_S3_ENDPOINT }}:80/{{ DATASET_BUCKET }}/products/{id}"
    """
    publish_location = find_publish_location_s3(datasets_json, dataset_type)
    return PurePath(publish_location).parts[1]


def find_s3_bucket(datasets_json, dataset_type):
    """Example location: "s3://{{ DATASET_S3_ENDPOINT }}:80/{{ DATASET_BUCKET }}/products/{id}"
    """
    publish_location = find_publish_location_s3(datasets_json, dataset_type)
    return PurePath(publish_location).parts[2]


def find_s3_url(datasets_json, dataset_type):
    """Example url: "http://{{ DATASET_BUCKET }}.{{ DATASET_S3_WEBSITE_ENDPOINT }}/products/{id}"
    """
    s3_publish_url = None
    for dataset in datasets_json["datasets"]:
        if dataset["type"] == dataset_type:
            url: str
            for url in dataset["publish"]["urls"]:
                if url.startswith("http"):
                    s3_publish_url = url
                    break

    if s3_publish_url is None:
        raise Exception("No s3 URL found in datasets.json")
    return s3_publish_url
