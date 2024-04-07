FROM python:3.8-slim as build

ARG PYTHON_DIST_PATH=/usr/lib/python3/dist-packages/
ARG PYTHON_SITE_PATH=/home/python/.local/lib/python3.8/site-packages

ENV PIP_NO_CACHE_DIR=off
# We need to use python3-gdcm as the gdcm pip package is built against gdcm v3.0
# but Debian at the moment only has v2.8 libraries. It's in upstream for the next release.
RUN apt-get update && \
  apt-get install --no-install-recommends -y \
  gcc \
  libopenjp2-7-dev \
  python3-gdcm && \
  rm -rf /var/lib/apt/lists/* && \
  useradd -m python

COPY --from=snyk/snyk:python-3.7 /usr/local/bin/snyk /usr/local/bin/snyk
ENV PATH="/home/python/.local/bin:${PATH}"
USER python

RUN pip install --user poetry && \
    poetry config virtualenvs.create false && \
    cp ${PYTHON_DIST_PATH}gdcmswig.py ${PYTHON_SITE_PATH} && \
    cp ${PYTHON_DIST_PATH}gdcm.py ${PYTHON_SITE_PATH} && \
    cp ${PYTHON_DIST_PATH}_gdcmswig.cpython-37m-x86_64-linux-gnu.so ${PYTHON_SITE_PATH}

# This is terrible but necessary
USER root
RUN chown python:python ${PYTHON_SITE_PATH}/*gdcm*

USER python
COPY --chown=python:python . /home/python/app/
WORKDIR  /home/python/app

# Exposes ourselves to the venv
RUN poetry install && \
    black data_uploader --check && \
    rm -rf /home/python/.cache/pypoetry

FROM build as dev

USER root
RUN apt-get update && rm -rf /var/lib/apt/lists/*

USER python
