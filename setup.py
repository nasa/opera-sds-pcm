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
    ]
)
