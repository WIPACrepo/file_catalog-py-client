FROM centos:7

RUN yum install -y epel-release

RUN yum -y install \
    git \
    python-pip \
 && pip install --upgrade setuptools

COPY . /wipac_fc/

RUN cd /wipac_fc \
 && rm -rf dist/* \
 && python setup.py sdist \
 && pip install dist/wipac_fc-*
