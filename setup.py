from setuptools import setup, find_packages

# adaptation_path = "folder/"

setup(
    name='opera_pcm',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        'smart_open',
        'pandas',
        'h5py'
    ],
    extras_require={
        'subscriber': [
            'boto3==1.22.3',
            'elasticsearch==7.13.4',
            'elasticsearch[async]>=7.13.4',
            'more-itertools==8.13.0',
            'requests==2.27.1',
        ],
        'test': [
            'prov-es@https://github.com/hysds/prov_es/archive/refs/tags/v0.2.2.tar.gz',
            'osaka@https://github.com/hysds/osaka/archive/refs/tags/v1.1.0.tar.gz',
            'hysds-commons@https://github.com/hysds/hysds_commons/archive/refs/tags/v1.0.9.tar.gz',
            'hysds@https://github.com/hysds/hysds/archive/refs/tags/v1.1.5.tar.gz',
            'chimera@https://github.com/hysds/chimera/archive/refs/tags/v2.2.1.tar.gz',
            'pyyaml',
            'backoff',
            'yamale',
            'jinja2',
			'mgrs',
            'boto3==1.22.3',
            'click==8.1.3',
            # 'GDAL==3.4.2',  # install native gdal first. `brew install gdal` on macOS.
            'Shapely',
            'elasticsearch==7.13.4',
            'elasticsearch[async]>=7.13.4',
            'requests==2.27.1',
            'pytest==7.1.1',
            'pytest-asyncio==0.18.3',
            'pytest-cov==3.0.0'
        ],
        'integration': [
            'pytest==7.1.1',
            'boto3==1.22.3',
            "boto3-stubs-lite[essential]",
            "elasticsearch==7.13.4",
            "elasticsearch-dsl==7.3.0",
            "requests==2.27.1",
            "backoff==1.11.1",
            "python-dotenv==0.20.0",
            "pytest-xdist==2.5.0",
            "pytest-xdist[psutil]",
            "filelock==3.6.0"
        ],
        "benchmark": [
            "boto3-stubs",
            "boto3-stubs-lite[essential]",
            "botocore",
            "elasticsearch[async]",
            "more-itertools==8.13.0",
            "pytest-asyncio==0.18.3"
        ]
    }
)
