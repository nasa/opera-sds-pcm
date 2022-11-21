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

            "pytest==7.1.1",
            "scripttest",
            "mock",
            "mockito",
            "flake8",
            "pytest-cov",
            "flake8-junit-report",
            "flake8-string-format",
            "xmltodict",
            "yamale==3.0.6",
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
            "cachetools==5.2.0"
        ],
        "subscriber": [
            "boto3",
            "python-dateutil",
            "elasticsearch==7.13.4",
            "elasticsearch[async]>=7.13.4",
            "more-itertools==8.13.0",
            "requests==2.27.1",
            "validators",
            "cachetools==5.2.0"
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
            "click==8.1.3",
            # "GDAL==3.5.0",  # install native gdal first. `brew install gdal` on macOS.
            "Shapely",
            "elasticsearch==7.13.4",
            "elasticsearch[async]>=7.13.4",
            "requests==2.27.1",
            "pytest>=7.1.1",
            "pytest-mock>=3.8.2",
            "pytest-asyncio==0.18.3",
            "pytest-cov==3.0.0",
            "mgrs",
            "pyproj",
            "validators",
            "cachetools==5.2.0"
        ],
        "integration": [
            "pytest==7.1.1",
            "boto3",
            "boto3-stubs-lite[essential]",  # for ec2, s3, rds, lambda, sqs, dynamo and cloudformation
            "boto3-stubs[sns]",
            "elasticsearch==7.13.4",
            "elasticsearch-dsl==7.3.0",
            "requests==2.27.1",
            "backoff==1.11.1",
            "python-dotenv==0.20.0",
            "pytest-xdist==3.0.2",
            "pytest-xdist[psutil]",
            "filelock==3.6.0"
        ],
        "benchmark": [
            "boto3-stubs",
            "boto3-stubs-lite[essential]",  # for ec2, s3, rds, lambda, sqs, dynamo and cloudformation
            "botocore",
            "elasticsearch[async]",
            "more-itertools==8.13.0",
            'pytest-asyncio==0.20.2'
        ]
    }
)
