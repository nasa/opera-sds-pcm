from cgitb import reset
import re
from select import poll
from unittest import result
from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta, timezone
import time
import logging
import boto3
import yaml
import os
import requests
from requests.auth import HTTPBasicAuth
from pathlib import PurePath
from urllib.parse import urlparse
import json 
import docker
from util import get_tropo_objects


bucket_name = ''
container_name = "Temp"

def create_modified_runconfig(template_path, output_path, **kwargs):
    """
    Create a modified run configuration based on a template.
    
    Args:
        template_path: Path to the template run configuration file
        output_path: Path where the modified configuration will be saved
        **kwargs: Key-value pairs for configuration modifications
    """
    # Load the template configuration
    with open(template_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Extract common parameters from kwargs
    input_file = kwargs.get('input_file')
    output_dir = kwargs.get('output_dir')
    scratch_dir = kwargs.get('scratch_dir')
    n_workers = kwargs.get('n_workers', 4)
    product_version = kwargs.get('product_version', '0.2')
    
    # Modify paths based on parameters
    if input_file:
        config['RunConfig']['Groups']['PGE']['InputFilesGroup']['InputFilePaths'] = [input_file]
        config['RunConfig']['Groups']['SAS']['input_file']['input_file_path'] = input_file
    
    # Update output and scratch directories
    config['RunConfig']['Groups']['PGE']['ProductPathGroup']['OutputProductPath'] = output_dir
    config['RunConfig']['Groups']['PGE']['ProductPathGroup']['ScratchPath'] = scratch_dir
    
    config['RunConfig']['Groups']['SAS']['product_path_group']['product_path'] = output_dir
    config['RunConfig']['Groups']['SAS']['product_path_group']['scratch_path'] = scratch_dir
    config['RunConfig']['Groups']['SAS']['product_path_group']['sas_output_path'] = output_dir
    
    # Update worker settings
    config['RunConfig']['Groups']['SAS']['worker_settings']['n_workers'] = n_workers
    
    # Update product version
    config['RunConfig']['Groups']['PGE']['PrimaryExecutable']['ProductVersion'] = str(product_version)
    config['RunConfig']['Groups']['SAS']['product_path_group']['product_version'] = str(product_version)

    # Create the directory if it doesn't exist (including parent directories)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Save the modified configuration
    with open(f"{output_path}runconfig.yaml", 'w') as file:
        yaml.dump(config, file, default_flow_style=False, sort_keys=False, indent=2)
    
    return output_path

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='tropo_dag',
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
        def job_preprocessing(filepath):
            logging.info(f"Preprocessing job {filepath}")
            
            # DAG_DIR = os.path.dirname(__file__)
            # template_file = os.path.join(DAG_DIR, "tropo_sample_runconfig-v3.0.0-er.3.1.yaml")
            # config_path = create_modified_runconfig(
            #     template_path=template_file,
            #     output_path= f"/opt/airflow/config/{filepath.split('/')[-1].split('.')[0]}/",
            #     input_file=  filepath,
            #     output_dir="/opt/airflow/output",
            #     scratch_dir="/opt/airflow/config/scratch",
            #     n_workers=4,
            #     product_version="0.3"
            # )
            return
        

        @task
        def submit_job(object_url):

            object_url = "s3://opera-ecmwf/" + object_url
        
            jobtype = "job-SCIFLO_L4_TROPO:dswxni-triggering-logic"
            queue = "opera-job_worker-sciflo-l4_tropo"
            mozart_url = "https://100.104.40.11/mozart/api/v0.1/job/submit"

            # Construct the payload. "params" must be a JSON string with the
            # actual metadata. The API expects the query string to look like
            # params=%7B%22product_metadata%22%3A%22s3://...%22%7D (i.e. the
            # JSON object URL-encoded)

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
                        "ProductReceivedTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
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
                auth=HTTPBasicAuth("verweyen", "Yogananda11*")
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
                    status = requests.get(poll_url, verify=False, params=poll_payload, auth=HTTPBasicAuth("verweyen", "Yogananda11*"))
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
                    result = requests.get(result_url, verify=False, params=poll_payload, auth=HTTPBasicAuth("verweyen", "Yogananda11*"))
                    result.raise_for_status()
                    return job_id

            else:
                raise Exception(f"Job submission for {object_url} failed, {response.get('message', 'No message')}!")

        @task 
        def post_processing(job_id):

            info_url = "https://100.104.40.11/mozart/api/v0.1/job/info"
            poll_payload = {"id": job_id}
            result = requests.get(info_url, verify=False, params=poll_payload, auth=HTTPBasicAuth("verweyen", "Yogananda11*"))
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

        
        preprocessing_result = job_preprocessing(filepath=url)
        submit_job_result = submit_job(object_url=url)
        post_processing_result = post_processing(job_id=submit_job_result)

        preprocessing_result >> submit_job_result >> post_processing_result
        

    
    data_filepaths = data_search()
    process_tropo_object.expand(url=data_filepaths)


# Instantiate the DAG
job = tropo_job_dag()
