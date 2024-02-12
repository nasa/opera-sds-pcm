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
            # The list of dependencies that are additionally installed as part of the opera-pcm docker image.
            #  See ./docker/Dockerfile
            "more-itertools",

            "pytest==7.2.1",
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
            "cachetools==5.2.0",

            "boto3-stubs",
            "boto3-stubs-lite[essential]",  # for ec2, s3, rds, lambda, sqs, dynamo and cloudformation
        ],
        "subscriber": [
            # The list of dependencies required to run the data_subscriber module standalone.
            "boto3",
            "boto3-stubs",
            "boto3-stubs-lite[essential]",  # for ec2, s3, rds, lambda, sqs, dynamo and cloudformation
            "python-dateutil",
            "elasticsearch==7.13.4",
            "elasticsearch[async]>=7.13.4",
            "more-itertools==8.13.0",
            "requests==2.27.1",
            "validators",
            "cachetools==5.2.0",
            "geopandas",
            "pyproj"
        ],
        "test": [
            # The list of dependencies required to run tests locally.
            #  Also doubles as list of dependencies to run all modules of the codebase outside of a cloud environment.
            "prov-es@https://github.com/hysds/prov_es/archive/refs/tags/v0.2.3.tar.gz",
            "osaka@https://github.com/hysds/osaka/archive/refs/tags/v1.2.3.tar.gz",
            "hysds-commons@https://github.com/hysds/hysds_commons/archive/refs/tags/v1.0.16.tar.gz",
            "hysds@https://github.com/hysds/hysds/archive/refs/tags/v1.2.12.tar.gz",
            "chimera@https://github.com/hysds/chimera/archive/refs/tags/v2.2.3.tar.gz",
            "pyyaml",
            "backoff",
            "yamale",
            "jinja2",
            "boto3",
            "botocore",
            "click==8.1.3",
            # "GDAL==3.7.0",  # install native gdal first. `brew install gdal` on macOS.
            "Shapely",
            "elasticsearch==7.13.4",
            "elasticsearch[async]>=7.13.4",
            "requests==2.27.1",
            "pytest==7.2.1",
            "pytest-mock>=3.8.2",
            "pytest-asyncio==0.20.3",
            "pytest-cov==4.0.0",
            "mgrs",
            "pyproj",
            "validators",
            "cachetools==5.2.0",
            "matplotlib",
            "numpy",
            "more-itertools==8.13.0",
            "ruamel.yaml"  # NOTE: deployed instances use ruamel-yaml-conda
        ],
        "integration": [
            # The list of dependencies required for the integration test module
            "pytest==7.2.1",
            "boto3",
            "boto3-stubs-lite[essential]",  # for ec2, s3, rds, lambda, sqs, dynamo and cloudformation
            "boto3-stubs[sns]",
            "elasticsearch==7.13.4",
            "elasticsearch-dsl==7.3.0",
            "requests==2.27.1",
            "backoff==1.11.1",
            "python-dotenv==0.20.0",
            "pytest-xdist==3.1.0",
            "pytest-xdist[psutil]",
            "filelock==3.6.0"
        ],
        "benchmark": [
            # The list of dependencies required for the benchmarking module
            "boto3-stubs",
            "boto3-stubs-lite[essential]",  # for ec2, s3, rds, lambda, sqs, dynamo and cloudformation
            "botocore",
            "elasticsearch[async]",
            "more-itertools==8.13.0",
            'pytest-asyncio==0.20.3'
        ],
        "audit": [
            # The list of dependencies required for the (internal) audit tools.
            "elasticsearch[async]",
            "more-itertools",
            "python-dateutil",
            "python-dotenv"
        ],
        "cmr_audit": [
            # The list of dependencies required for the (CMR) audit tools.
            "aiohttp[speedups]",
            "backoff",
            "compact-json",
            # "GDAL==3.6.2",  # install native gdal first. `brew install gdal` on macOS.
            "more-itertools",
            "python-dateutil",
            "python-dotenv",
            "requests"
        ],
        "cnm_check": [
            # The list of dependencies required for the cnm_check tool.
            "compact-json",
            "elasticsearch[async]",
            "more-itertools",
            "python-dateutil",
            "python-dotenv"
        ]
    }
)
