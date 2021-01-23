import setuptools

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name='danql-danofsteel32',
    version='0.0.2',
    author='Dan Davis',
    author_email='dan@chamberlainbuildersllc.om',
    description='ORM-lite library for SQLite',
    long_description=long_description,
    url='https://github.com/danofsteel32/danql',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
    ],
    python_requires='>=3.5',
)
