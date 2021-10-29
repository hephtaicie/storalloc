"""setuptool based setup script

Adapted from https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
import pathlib
from setuptools import setup, find_packages

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / "README.md").read_text(encoding="utf-8")

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name="storalloc",
    version="0.0.1",
    description="Dynamic Storage Allocator and Simulator",
    long_description=long_description,  # Optional
    long_description_content_type="text/markdown",
    url="https://gitlab.inria.fr/Kerdata/kerdata-projects/storalloc",
    author="kerdata@INRIA",  # Optional
    # author_email="",
    classifiers=[  # Optional
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3 :: Only",
    ],
    keywords="simulation, scheduler, storage",  # Optional
    package_dir={"": "src"},  # Optional
    packages=find_packages(where="src"),  # Required
    python_requires=">=3.6, <4",
    install_requires=[
        "reportlab>=3.5.57",
        "six>=1.15.0",
        "scipy>=1.5.4",
        "numpy>=1.19.4",
        "pandas>=1.0.5",
        "matplotlib>=3.3.4",
        "kmodpy>=0.1.13",
        "Pillow>=8.3.0",
        "PyYAML>=5.4.1",
        "pyzmq>=22.1.0",
        "click>=8.0",
    ],
    extras_require={  # Optional
        "dev": ["black>=21.9b0"],
        "test": ["tox", "pylint", "pytest", "pytest-cov"],
    },
    entry_points={
        "console_scripts": [
            "storalloc=storalloc.cli:cli",
        ],
    },
    # package_data={
    #     "sample": ["package_data.dat"],
    # },
    # data_files=[("my_data", ["data/data_file"])],
)
