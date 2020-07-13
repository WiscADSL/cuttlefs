import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cuttlefs",
    version="0.0.1",
    author="Anthony Rebello",
    author_email="arebello@wisc.edu",
    description="Emulate file-system failure handling characteristics.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/WiscADSL/cuttlefs",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: POSIX :: Linux",
        "Topic :: System :: Filesystems",
    ],
    # TODO license
    # TODO requirements / dependencies
    entry_points={
        'console_scripts': ['cuttlefs=cuttlefs.cli:main'],
    }
)
