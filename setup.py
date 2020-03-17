from setuptools import find_packages, setup

readme = open('README.rst').read() + open('CHANGELOG.rst').read()

setup(
    name='asgi_rabbitmq',
    version='0.3-fix-pika',
    description='RabbitMQ backend for ASGI',
    long_description=readme,
    url='https://github.com/amorim/asgi_rabbitmq',
    license='BSD',
    author='Django Software Foundation',
    author_email='foundation@djangoproject.com',
    maintainer='Artem Malyshev',
    maintainer_email='proofit404@gmail.com',
    packages=find_packages(),
    install_requires=[
        'pika==0.11.0',
        'asgiref>=1.0.0',
        'msgpack-python',
        'futures ; python_version < "3.0"',
    ],
    extras_require={
        'tests': ['rabbitmq-admin>=0.2'],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development',
    ])
