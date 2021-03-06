from setuptools import setup

requirements = [l.strip() for l in open('requirements.txt').readlines()]

setup(
    name='pykafkap',
    url='https://github.com/urbanairship/pykafkap',
    version='0.2.0',
    license='Apache',
    author='Urban Airship',
    author_email='platform@urbanairship.com',
    description='A simple Kafka producer client for Python.',
    long_description=open('README.rst').read(),
    py_modules=['kafkap'],
    test_suite='test_kafkap',
    tests_require=['mox'],
    install_requires=requirements,
    classifiers=['License :: OSI Approved :: Apache Software Licens'],
)
