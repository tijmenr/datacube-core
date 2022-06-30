##
## This file is part of the Open Data Cube, see https://opendatacube.org for more information
##
## Copyright (c) 2015-2020 ODC Contributors
## SPDX-License-Identifier: Apache-2.0
##
ARG V_PG=14
FROM osgeo/gdal:ubuntu-small-latest

# Update and install Ubuntu packages

USER root

# Notes:
#       Install numpy and rasterio from pip - shouldn't be necessary, further investigation required
#       python3-cfgrib installs a prebuilt cfunits - cfunits does not install in Python3.10 as of version 3.0.1.post0
#       pyproj works for me in venv with no-binary - but is failing in Docker for some reason
RUN apt-get update -y \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --allow-change-held-packages --fix-missing --no-install-recommends \
        git \
        libpq-dev libudunits2-dev libproj-dev \
        python3-dev python3-distutils python3-pip \
        python3-numpy python3-rasterio \
        build-essential \
        postgresql \
        redis-server \
        postgresql-client-${V_PG} \
        postgresql-${V_PG} \
        sudo make graphviz \
        tini \
    && apt-get clean

# Build constrained python environment

# Set the locale, this is required for some of the Python packages
ENV LC_ALL C.UTF-8

COPY docker/constraints.in /conf/requirements.txt
COPY docker/constraints.txt docker/nobinary.txt /conf/

RUN python3 -m pip install --upgrade pip

# Pre-release version of Cython required for some packages to build and install correctly
RUN python3 -m pip install cython==0.29.27
# Pyproj requires a no-binary install
RUN python3 -m pip install --no-use-pep517 --no-binary ':all:' pyproj==3.3.0

RUN python3 -m pip list

# Rest can install safely from constraints.txt
RUN python3 -m pip install \
       -r /conf/requirements.txt \
       -c /conf/constraints.txt

# Copy datacube-core source code into container and install from source (with addons for tests).
COPY . /code

RUN python3 -m pip install '/code/[all]' \
    && python3 -m pip install /code/examples/io_plugin \
    && python3 -m pip install /code/tests/drivers/fail_drivers

RUN python3 -m pip list

# Copy bootstrap script into image.
COPY docker/assets/with_bootstrap /usr/local/bin/

# prep db
RUN  install --owner postgres --group postgres -D -d /var/run/postgresql /srv/postgresql \
  && sudo -u postgres "$(find /usr/lib/postgresql/ -type f -name initdb)" -D "/srv/postgresql" --auth-host=md5 --encoding=UTF8

# users and groups.
RUN groupadd --gid 1000 odc \
  && useradd --gid 1000 \
  --uid 1000 \
  --create-home \
  --shell /bin/bash -N odc \
  && adduser odc users \
  && adduser odc sudo \
  && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
  && install -d -o odc -g odc /env \
  && install -d -o odc -g odc /code \
  && true

USER root
VOLUME /code
WORKDIR /code

ENTRYPOINT ["/bin/tini", "-s", "--", "/usr/local/bin/with_bootstrap"]