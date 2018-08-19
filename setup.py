
from setuptools import setup, find_packages

setup(
    name='pyCDI',
    version='0.0.1',
    description='',
    url='hhttps://bitbucket.org/JuanluOnieva/',
    author='Juan L. Onieva',
    author_email='juanluonieva@uma.es',
    license='MIT',
    python_requires='>=3',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Programming Language :: Python :: 3.6'
    ],
    packages=find_packages(exclude=["tests.*", "tests"]),
)