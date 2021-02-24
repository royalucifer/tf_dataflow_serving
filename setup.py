from setuptools import find_packages
from setuptools import setup

REQUIRED_PACKAGES = [
    "apache-beam[gcp]", "scipy", "pandas",
    "numpy", "google-cloud", "tensorflow==1.15"
]
PACKAGE_NAME = "news_rec_predict"
PACKAGE_VERSION = "0.01"

setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(),
    # package_data={'model': ['trained/*']},
    requires=[]
)