#!/usr/bin/env python
# coding: utf-8

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

# -----------------------------------------------------------------------------
# Minimal Python version sanity check (from IPython/Jupyterhub)
# -----------------------------------------------------------------------------

from __future__ import print_function

import os
import sys

from setuptools import setup
from glob import glob

pjoin = os.path.join
here = os.path.abspath(os.path.dirname(__file__))

# Get the current package version.
version_ns = {}
with open(pjoin(here, "version.py")) as f:
    exec(f.read(), {}, version_ns)

with open(pjoin(here, "README.md"), encoding="utf-8") as f:
    long_desc = f.read()

setup_args = dict(
    name="remote-slurm-spawner",
    scripts=glob(pjoin("scripts", "*")),
    packages=["remote_slurm_spawner"],
    version=version_ns["__version__"],
    description="""Remote-slurm-spawner: A spawner for Jupyterhub to spawn notebooks using slurm on remote machine.""",
    long_description=long_desc,
    long_description_content_type="text/markdown",
    author="D. Ciangottini, credits to Michael Milligan, Andrea Zonca, Mike Gilbert",
    author_email="dciangot@cern.ch",
    url="http://jupyter.org",
    license="BSD",
    platforms="Linux, Mac OS X",
    python_requires="~=3.3",
    keywords=["Interactive", "Interpreter", "Shell", "Web", "Jupyter"],
)

# setuptools requirements
if "setuptools" in sys.modules:
    setup_args["install_requires"] = install_requires = []
    with open("requirements.txt") as f:
        for line in f.readlines():
            req = line.strip()
            if not req or req.startswith(("-e", "#")):
                continue
            install_requires.append(req)


def main():
    setup(**setup_args)


if __name__ == "__main__":
    main()
