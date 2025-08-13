from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
import time
import logging
import yaml
import os
import docker
from util import get_tropo_objects
import boto3


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
    config['RunConfig']['Groups']['PGE']['ProductPathGroup']['OutputProductPath'] = output_dir
    config['RunConfig']['Groups']['PGE']['ProductPathGroup']['ScratchPath'] = scratch_dir
    config['RunConfig']['Groups']['SAS']['product_path_group']['product_path'] = output_dir
    config['RunConfig']['Groups']['SAS']['product_path_group']['scratch_path'] = scratch_dir
    config['RunConfig']['Groups']['SAS']['product_path_group']['sas_output_path'] = output_dir
    config['RunConfig']['Groups']['SAS']['worker_settings']['n_workers'] = n_workers
    config['RunConfig']['Groups']['PGE']['PrimaryExecutable']['ProductVersion'] = str(product_version)
    config['RunConfig']['Groups']['SAS']['product_path_group']['product_version'] = str(product_version)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(f"{output_path}runconfig.yaml", 'w') as file:
        yaml.dump(config, file, default_flow_style=False, sort_keys=False, indent=2)
    return output_path

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


@dag(
    dag_id='tropo_PGE',
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
        s3 = boto3.client("s3")
        bucket_name = "opera-ecmwf"
        response = get_tropo_objects(bucket_name, date="2024-12-31")
        tropo_directory = "/opt/airflow/config/tropo-objects"
        file_paths = []

        for obj in response:
            object_key = obj['Key']

            local_file_path = f"{tropo_directory}/{object_key.split('/')[-1]}"
          
            s3.download_file(bucket_name, object_key, local_file_path)

            logging.info(f"Downloaded {object_key} to {local_file_path}")

            file_paths.append(local_file_path)

        return file_paths

    @task_group(group_id="tropo_job_group")
    def process_tropo_object(filepath):

        @task
        def job_preprocessing(filepath):
            logging.info(f"Preprocessing job {filepath}")

            
            DAG_DIR = os.path.dirname(__file__)
            template_file = os.path.join(DAG_DIR, "tropo_sample_runconfig-v3.0.0-er.3.1.yaml")
            config_path = create_modified_runconfig(
                template_path=template_file,
                output_path= f"/opt/airflow/config/{filepath.split('/')[-1].split('.')[0]}/",
                input_file=  filepath,
                output_dir="/opt/airflow/output",
                scratch_dir="/opt/airflow/config/scratch",
                n_workers=4,
                product_version="0.3"
            )
            return config_path

        preprocessing_result = job_preprocessing(filepath=filepath)
        

        @task
        def spinup_workers(config_path: str, input_path: str):
            """
            Start a docker container using the python docker SDK instead of the DockerOperator.
            
            Args:
                config_path (str): Directory provided from job_preprossesing that contians the run config
                input_path (str):  Directory provided from data_search containing downloaded troposhperic data
            """

            logging.info("Spinning up container for %s", input_path)

            client = docker.from_env()

            # Build environment variables for the container
            env_vars = {
                "UID": str(os.getuid()),
                "container_name": input_path.split('/')[-1],
                "CONFIG_PATH": config_path,
                "input_data_dir": "/home/airflow/input_data",
                "output_dir": "/opt/airflow/output",
                "scratch_dir": "/opt/airflow/config/scratch",
            }

            # Build the command so the PGE receives the required runconfig file (-f flag)
            runconfig_file = os.path.join(config_path, "runconfig.yaml")
            cmd = ["-f", runconfig_file]

            container_name_local = input_path.split('/')[-1]

            current_container_id = os.environ.get("HOSTNAME")

            try:
                output = client.containers.run(
                    image="opera_pge/tropo:3.0.0-er.3.1-tropo",
                    command=cmd,
                    name=container_name_local,
                    environment=env_vars,
                    user="0",
                    volumes_from=[current_container_id] if current_container_id else None,
                    remove=True,
                    detach=False,
                )
                logging.info("Container finished. Output:\n%s", output.decode("utf-8") if isinstance(output, bytes) else output)
            finally:
                client.close()

            return f"Container {container_name_local} completed"

        spinup_workers_result = spinup_workers(config_path=preprocessing_result, input_path=filepath)

        @task 
        def post_processing():
            logging.info("PostProcessing job")
            time.sleep(10)
            return "Postprocessed job"

        post_processing_result = post_processing()

        preprocessing_result >> spinup_workers_result >> post_processing_result
    
    data_filepaths = data_search()
    process_tropo_object.expand(filepath=data_filepaths)

# Instantiate the DAG
job = tropo_job_dag()
