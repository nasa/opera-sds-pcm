from setuptools import setup, find_packages

# adaptation_path = "folder/"

setup(
    name="opera_pcm",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "smart_open",
        "pandas",
        "h5py"
    ],
    extras_require={
        "docker": [
            "more-itertools",

            "pytest==7.4.0",
            "scripttest",
            "mock",
            "mockito",
            "flake8",
            "pytest-cov",
            "flake8-junit-report",
            "flake8-string-format",
            "xmltodict",
            "yamale==4.0.4",
            "ruamel.yaml",
            "elasticmock",
            "geopandas",
            "smart_open",

            "pytest-asyncio",
            "pytest-mock",
            "elasticsearch[async]",
            
            "mgrs",
            "pyproj",

            "python-dateutil",
            "validators",
            "cachetools==5.3.0",

            "boto3-stubs",
            "boto3-stubs-lite[essential]",  # for ec2, s3, rds, lambda, sqs, dynamo and cloudformation
        ],
        "subscriber": [
            "boto3",
            "boto3-stubs",
            "boto3-stubs-lite[essential]",  # for ec2, s3, rds, lambda, sqs, dynamo and cloudformation
            "python-dateutil",
            "elasticsearch==8.9.0",
            "elasticsearch[async]>=8.9.0",
            "more-itertools==10.1.0",
            "requests==2.31.0",
            "validators",
            "cachetools==5.3.1"
        ],
        "test": [
            "prov-es@https://github.com/hysds/prov_es/archive/refs/tags/v0.2.2.tar.gz",
            "osaka@https://github.com/hysds/osaka/archive/refs/tags/v1.1.0.tar.gz",
            "hysds-commons@https://github.com/hysds/hysds_commons/archive/refs/tags/v1.0.9.tar.gz",
            "hysds@https://github.com/hysds/hysds/archive/refs/tags/v1.1.5.tar.gz",
            "chimera@https://github.com/hysds/chimera/archive/refs/tags/v2.2.1.tar.gz",
            "pyyaml",
            "backoff",
            "yamale",
            "jinja2",
            "boto3",
            "botocore",
            "click==8.1.6",
            # "GDAL==3.6.2",  # install native gdal first. `brew install gdal` on macOS.
            "Shapely",
            "elasticsearch==8.9.0",
            "elasticsearch[async]>=8.9.0",
            "requests==2.31.0",
            "pytest==7.4.0",
            "pytest-mock>=3.11.1",
            "pytest-asyncio==0.21.1",
            "pytest-cov==4.1.0",
            "mgrs",
            "pyproj",
            "validators",
            "cachetools==5.3.1",
            "matplotlib",
            "numpy",
            "more-itertools==8.13.18",
            "ruamel.yaml"  # NOTE: deployed instances use ruamel-yaml-conda
        ],
        "integration": [
            "pytest==7.4.0",
            "boto3",
            "boto3-stubs-lite[essential]",  # for ec2, s3, rds, lambda, sqs, dynamo and cloudformation
            "boto3-stubs[sns]",
            "elasticsearch==8.9.0",
            "elasticsearch-dsl==8.13.18",
            "requests==2.31.0",
            "backoff==2.2.1",
            "python-dotenv==1.0.0",
            "pytest-xdist==3.1.1",
            "pytest-xdist[psutil]",
            "filelock==3.12.2"
        ],
        "benchmark": [
            "boto3-stubs",
            "boto3-stubs-lite[essential]",  # for ec2, s3, rds, lambda, sqs, dynamo and cloudformation
            "botocore",
            "elasticsearch[async]",
            "more-itertools==10.1.0",
            'pytest-asyncio==0.21.1'
        ],
        "audit": [
            "elasticsearch[async]",
            "more-itertools",
            "python-dateutil",
            "python-dotenv"
        ],
        "cmr_audit": [
            "aiohttp[speedups]",
            "backoff",
            "compact-json",
            "more-itertools",
            "python-dateutil",
            "python-dotenv"
        ],
        "cnm_check": [
            "compact-json",
            "elasticsearch[async]",
            "more-itertools",
            "python-dateutil",
            "python-dotenv"
        ]
    }
)
