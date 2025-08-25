from airflow.decorators import dag, task, task_group
from datetime import datetime, timezone, timedelta
import time
import logging
import yaml
import os
import sys
import uuid

dag_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dag_dir)

from util import get_tropo_objects
import boto3
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

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
    product_version = kwargs.get('product_version', '1.0')
    
    # Modify paths based on parameters
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
    runconfig_output = f"{output_path}runconfig.yaml"
    return runconfig_output


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
        bucket_name = "opera-ecmwf"
        response = get_tropo_objects(bucket_name, date="2024-12-31")   
        logging.info(f"{response}")
        return [response[0]]

    @task_group(group_id="tropo_job_group")
    def process_tropo_object(s3_uri):

        @task
        def job_preprocessing(s3_uri):

            #We need to upload the outputted file existing at output path to s3 
            #We output config path URI and Tropo Object URI
            s3 = boto3.resource("s3")
            logging.info(f"Generating runconfig for job {s3_uri}")

            DAG_DIR = os.path.dirname(__file__)
            template_file = os.path.join(DAG_DIR, "tropo_sample_runconfig-v3.0.0-er.3.1.yaml")
            local_config_path = create_modified_runconfig(
                template_path=template_file,
                output_path= f"/opt/airflow/storage/runconfigs/{s3_uri.split('/')[-1].split('.')[0]}",
                input_file=  f"/workdir/input/{s3_uri.split('/')[-1]}",
                output_dir="/workdir/output/",
                scratch_dir="/workdir/output/scratch",
                n_workers=4,
                product_version="1.0"
            )
            bucket_name = "opera-dev-cc-verweyen"
            bucket = s3.Bucket(bucket_name)
            s3_config_uri = f"tropo/runconfigs/ECMWF_TROP_202412310000_202412310000_1runconfig.yaml"
            bucket.upload_file(local_config_path, s3_config_uri)
            #Return config uri, tropo object uri and the filepath to where both will be downloaded to in our tropo PGE
            input_path = f"/workdir/input/{s3_uri.split('/')[-1]}"
            return s3_uri

        job_id = str(uuid.uuid4()).replace('-', '')[:8].lower()  # Remove hyphens and ensure lowercase
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        preprocessing_result = job_preprocessing(s3_uri=s3_uri)
        
        # Environment variables for the main container and init containers
        env_vars = {
            "UID": "1000", 
            "CONFIG_PATH": f"/workdir/config/runconfig.yaml",
            "INPUT_DATA_PATH": "/workdir/input1/data.nc",
            "OUTPUT_PATH": "/workdir/output/",
            "S3_OUTPUT_BUCKET": "opera-dev-cc-verweyen",
            "JOB_ID": job_id,
            "TROPO_OBJECT": "20241231/ECMWF_TROP_202412310000_202412310000_1.nc",
            "RUN_CONFIG": "ECMWF_TROP_202412310000_202412310000_1runconfig.yaml"
        }

        # Convert dict to k8s env list for V1Container.env
        init_env = [k8s.V1EnvVar(name=k, value=v) for k, v in env_vars.items()]

        # Shared volume for data exchange between containers
        shared_volume = k8s.V1Volume(
            name="workdir",
            empty_dir=k8s.V1EmptyDirVolumeSource()
        )

        shared_mount = k8s.V1VolumeMount(
            name="workdir",
            mount_path="/workdir"
        )
        
                
        run_tropo_pge_k8s = KubernetesPodOperator(
            task_id="run_tropo_pge_kubernetes",
            namespace="opera-dev",
            name=f"tropo-pge-{job_id}", 
            image="artifactory-fn.jpl.nasa.gov:16001/gov/nasa/jpl/opera/sds/pge/opera_pge/tropo:3.0.0-rc.1.0-tropo",
            in_cluster=True,
            kubernetes_conn_id=None,
            image_pull_secrets=[k8s.V1LocalObjectReference(name="artifactory-creds")],
            config_file=None,
            init_container_logs=True,
            startup_timeout_seconds=600,
            
            arguments=["-f", "/workdir/config/runconfig.yaml"],

            init_containers= [
                    k8s.V1Container(
                        name="download-tropo-data",
                        image="amazon/aws-cli:2.17.52",
                        command=["/bin/sh", "-c"],
                        args=[
                            "set -e; "
                            "mkdir -p /workdir/input; "
                            "F=$(basename \"$TROPO_OBJECT\"); "
                            "aws s3 cp \"s3://opera-ecmwf/$TROPO_OBJECT\" \"/workdir/input/$F\"; "
                            "echo \"Downloaded $F to /workdir/input/\""
                        ],
                        volume_mounts=[shared_mount],
                        env=init_env
                    ),
                    k8s.V1Container(
                        name="download-runconfig",
                        image="amazon/aws-cli:2.17.52", 
                        command=["/bin/sh", "-c"],
                        args=[
                            "set -e; "
                            "mkdir -p /workdir/config; "
                            "aws s3 cp \"s3://$S3_OUTPUT_BUCKET/tropo/runconfigs/$RUN_CONFIG\" '/workdir/config/runconfig.yaml'; "
                            "echo 'Downloaded runconfig to /workdir/config/runconfig.yaml'"
                        ],
                        volume_mounts=[shared_mount],
                        env=init_env
                    )
                ],

            
            env_vars=env_vars,
            get_logs=True,
            is_delete_operator_pod=False,
            
            # Use the existing Airflow worker service account
            service_account_name="airflow-worker",  # Existing service account with AWS permissions

            container_resources=k8s.V1ResourceRequirements(
                requests={
                    "cpu": "12000m",     # 12 CPU cores (75% of 16)
                    "memory": "48Gi"     # 48GB RAM (75% of 64GB)
                },
                limits={
                    "cpu": "15000m",     # Max 15 CPU cores (leave some headroom)
                    "memory": "60Gi"     # Max 60GB RAM (leave some headroom)
                }
            ),

            volumes=[shared_volume],
            volume_mounts=[shared_mount]
        )
           

        @task 
        def post_processing():
            logging.info("PostProcessing job")
            time.sleep(10)
            return "Postprocessed job"
          
        
        post_processing_result = post_processing()

        # Set up task dependencies
        preprocessing_result >> run_tropo_pge_k8s >> post_processing_result
        
        return run_tropo_pge_k8s  # Return reference to the Kubernetes operator
    
    s3_uris = data_search()
    process_tropo_object.expand(s3_uri=s3_uris)

# Instantiate the DAG
job = tropo_job_dag()
