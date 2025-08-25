from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta, timezone
import time
import logging
import boto3
import requests
from requests.auth import HTTPBasicAuth
from pathlib import PurePath
from urllib.parse import urlparse
import json 
from util import get_tropo_objects

bucket_name = ''
container_name = "Temp"

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='tropo_mozart',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['Luca'],
)
def tropo_job_dag():
    
    @task
    def data_search():
        
        #temporarily hardcoded data search 

        bucket_name = "opera-ecmwf"
        urls = get_tropo_objects(bucket_name, date="2024-12-31")

        return urls

    @task_group(group_id="tropo_job_group")
    def process_tropo_object(url):

        @task
        def submit_job(object_url):

            object_url = "s3://opera-ecmwf/" + object_url 
            jobtype = "job-SCIFLO_L4_TROPO:dswxni-triggering-logic"
            queue = "opera-job_worker-sciflo-l4_tropo"
            mozart_url = "https://100.104.40.11/mozart/api/v0.1/job/submit"
            parsed = urlparse(object_url)
            if parsed.scheme != "s3" or not parsed.netloc:
                raise ValueError(f"Invalid S3 URI: {object_url}")
    
            s3_key = parsed.path.lstrip('/')
            logging.info(s3_key)

            product = {"product_metadata" :{
                    "dataset": f"L4_TROPO-{s3_key}",
                    "metadata": {
                        "batch_id": s3_key,
                        "product_paths": {"L4_TROPO": [object_url]},  
                        "ProductReceivedTime": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                        "FileName": PurePath(s3_key).name,
                        "FileLocation": s3_key,
                        "id": s3_key,
                        "Files": [
                            {
                                "FileName": PurePath(s3_key).name,
                                "FileSize": 1, 
                                "FileLocation": object_url,
                                "id": PurePath(s3_key).name,
                                "product_paths": "$.product_paths"
                            }
                        ]
                    }
                },
                "dataset_type":"L4_TROPO",
                "input_dataset_id": s3_key
                }

            params = json.dumps(product)
            logging.info(params)

            payload = {
                "type": jobtype,
                "queue": queue,
                "params": params 
            }

            job = requests.post(
                mozart_url,
                params=payload,  # send as JSON body, not query string
                verify=False,    # NOTE: ignore SSL verification (test only)
                auth=HTTPBasicAuth("", "")
            )
            job.raise_for_status()
            logging.info(job)
            
            response = job.json()
            if response["success"] == True:
                job_id = response["result"]
                logging.info(f"jobID:{job_id}")

                poll_url = "https://100.104.40.11/mozart/api/v0.1/job/status"
                poll_payload = {"id": job_id}

                #Give ample time for job to be visible
                time.sleep(200)

                #Job Polling loop
                while True:
                    status = requests.get(poll_url, verify=False, params=poll_payload, auth=HTTPBasicAuth("", ""))
                    status.raise_for_status()

                    status_json = status.json()

                    if status_json["status"] not in ("job-started", "job-queued"):
                        break
                    logging.info(f"job: {job_id} {status_json['status']}")
                    time.sleep(2)
                
                if status_json["status"] == "job-failed" or status_json["status"] == "job-offline":
                  raise Exception(f"Processing for {object_url} failed, {response.get('message', 'No message')}!")

                else:
                    result_url = "https://100.104.40.11/mozart/api/v0.1/job/info"
                    result = requests.get(result_url, verify=False, params=poll_payload, auth=HTTPBasicAuth(" ", ""))
                    result.raise_for_status()
                    return job_id

            else:
                raise Exception(f"Job submission for {object_url} failed, {response.get('message', 'No message')}!")

        @task 
        def post_processing(job_id):

            info_url = "https://100.104.40.11/mozart/api/v0.1/job/info"
            poll_payload = {"id": job_id}
            result = requests.get(info_url, verify=False, params=poll_payload, auth=HTTPBasicAuth("", ""))
            result_json = result.json()

            key = result_json["result"]["job"]["job_info"]["metrics"]["products_staged"][0]["id"]

            bucket_name = "opera-dev-rs-ryhunter"

            # Create S3 client
            s3_client = boto3.client('s3')
            
            try:
                # Check if the object exists in the bucket
                s3_client.head_object(Bucket=bucket_name, Key=key)
                logging.info(f"Key '{key}' exists in bucket '{bucket_name}'")
                key_exists = True
            except s3_client.exceptions.NoSuchKey:
                logging.warning(f"Key '{key}' does not exist in bucket '{bucket_name}'")
                key_exists = False
            except Exception as e:
                logging.error(f"Error checking key '{key}' in bucket '{bucket_name}': {str(e)}")
                key_exists = False



            logging.info("PostProcessing job")
            time.sleep(10)
            return f"Postprocessed job - Key exists: {key_exists}"

        submit_job_result = submit_job(object_url=url)
        post_processing_result = post_processing(job_id=submit_job_result)

        submit_job_result >> post_processing_result
        

    
    data_filepaths = data_search()
    process_tropo_object.expand(url=data_filepaths)


# Instantiate the DAG
job = tropo_job_dag()
