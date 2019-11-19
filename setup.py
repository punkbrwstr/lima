import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="lima-punkbrwstr",
    version="0.0.2",
    author="Peter Graf",
    author_email="magnumpi@gmail.com",
    description="Column-oriented time series data store in Redis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/punkbrwstr/lima",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
