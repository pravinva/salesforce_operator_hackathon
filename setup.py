"""
Setup configuration for Salesforce Integration package.
"""

from setuptools import setup, find_packages

setup(
    name="salesforce_integration",
    version="1.0.1",
    description="Salesforce integration for Databricks Python Operator Framework",
    author="Pravin Varma",
    author_email="pravin.varma@databricks.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=[
        "requests>=2.28.0",
        "databricks-sdk>=0.20.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
