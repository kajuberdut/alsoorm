import os
from setuptools import setup, find_packages

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="alsoorm",
    version="0.0.2",
    author="Patrick Shechet",
    author_email="patrick.shechet@gmail.com",
    description=(
        "Description Like many others, this is also an ORM. This one is based on asyncpg."
    ),
    license="Apache 2.0",
    packages=find_packages(),
    long_description=read("README.md"),
    install_requires=["asyncpg", "tenacity"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
