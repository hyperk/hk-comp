FROM registry.git.hyperk.org/hyperk/hk-containers/dependencies2:latest

RUN curl -LO https://github.com/DIRACGrid/DIRACOS2/releases/latest/download/DIRACOS-Linux-$(uname -m).sh &&\
    bash DIRACOS-Linux-$(uname -m).sh &&\
    rm DIRACOS-Linux-$(uname -m).sh &&\
    source /usr/local/hk/diracos/diracosrc

WORKDIR /usr/local/hk/hk-comp

COPY src src
COPY README.md .
COPY setup.cfg .
COPY setup.py .

RUN source /usr/local/hk/diracos/diracosrc &&\
    pip install .