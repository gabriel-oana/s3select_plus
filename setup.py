import setuptools

setuptools.setup(
    name='s3select_plus',
    version='1.0.2',
    author="Gabriel Oana",
    author_email="gabriel.oana91@gmail.com",
    description="S3 select parallel package",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url='https://github.com/gabriel-oana/s3select_plus',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'boto3>=1.24.75',
        'tqdm>=4.64.1'
    ]
)