from setuptools import setup, find_packages

setup(
    name='wipac_fc',
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    description='WIPAC File Catalog Python Client',
    url='https://github.com/WIPACrepo/wipac_fc',
    author='WIPAC',
    author_email='contact-us@icecube.wisc.edu',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    packages=find_packages(),
    install_requires=['requests'],
    package_data={
        'wipac_fc': []
    },
    entry_points={
        'console_scripts': [
        ]
    },
    python_requires='>=2.7'
)
