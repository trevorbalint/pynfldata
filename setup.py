from setuptools import setup

setup(
    name='pynfldata',
    version='0.1.9',
    description="Data extraction, cleaning, and verification from NFL's feeds-rs data service",
    author='Trevor Balint',
    author_email='trevor.balint@gmail.com',
    packages=['pynfldata', 'pynfldata.coaches_data', 'pynfldata.data_tools'],  # same as name
    install_requires=['xmltodict', 'pandas', 'urllib3', 'pyspark', 'pyarrow'],  # external packages as dependencies
)
