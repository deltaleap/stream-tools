from setuptools import setup
from setuptools import find_packages


setup(
    name="stream-tools",
    version="0.3",
    author="Mattia Terenzi",
    packages=find_packages(),
    install_requires=["aioredis==1.3.1", "uvloop==0.14.0", "numpy==1.19.4"],
)
