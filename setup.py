from setuptools import setup, find_packages

setup(
    name='sparkutils',
    version='0.1dev',
    packages=find_packages(),
    install_requires=[
        'pyspark>=2.0'
    ]
)