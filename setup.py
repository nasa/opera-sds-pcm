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
        'integration': [
            'pytest==7.1.1',
            'boto3',
            "elasticsearch==7.13.4",
            "elasticsearch-dsl==7.3.0",
            "requests==2.27.1",
            "backoff==1.11.1",
            "python-dotenv==0.20.0",
            "pytest-xdist==2.5.0",
            "pytest-xdist[psutil]",
            "filelock==3.6.0"
        ]
    }
)
