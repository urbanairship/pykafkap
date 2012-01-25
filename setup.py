from setuptools import setup


setup(
    name='pykafkap',
    url='https://github.com/urbanairship/pykafkap',
    version='0.1.0',
    license='Apache',
    author='Michael Schurter',
    author_email='schmichael@urbanairship.com',
    description='A simple Kafka producer client for Python.',
    long_description=open('README.rst').read(),
    py_modules=['kafkap'],
    classifiers=['License :: OSI Approved :: Apache Software Licens'],
)
