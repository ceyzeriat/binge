from sys import argv, exit
import os, re


_m = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "binge", "_version.py")).read()
version = re.findall(r"__version__ *= *\"(.*?)\"", _m)[0]


if "upl" in argv[1:]:
    os.system("python setup.py sdist")
    os.system("twine upload -r pypi ./dist/binge-{}.tar.gz".format(version))
    exit()


try:
    from setuptools import setup
    setup
except ImportError:
    from distutils.core import setup
    setup

try:
    desc = open("README.rst").read() + "\n\n" + "Changelog\n" + "---------\n\n" + open("HISTORY.rst").read()
except:
    desc = ""

setup(
    name="binge",
    version=version,
    author="Guillaume Schworer",
    author_email="guillaume.schworer@gmail.com",
    packages=["binge"],
    url="https://github.com/ceyzeriat/binge/",
    license="GNU General Public License v3 or later (GPLv3+)",
    description="Lazy multiprocess your callables in three extra characters",
    long_description=desc,
    package_data={"": ["LICENSE", "AUTHORS.rst", "HISTORY.rst", "README.rst"]},
    include_package_data=True,
    install_requires=[],
    download_url='https://github.com/ceyzeriat/binge/tree/master/dist',
    keywords=['multi', 'processing', 'multiprocessing', 'lazy', 'wrapper', 'function', 'threading', 'multithreading', 'multi-processing', 'multi-threading'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        'Intended Audience :: Education',
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: OS Independent",
        "Programming Language :: Python"
    ],
)
