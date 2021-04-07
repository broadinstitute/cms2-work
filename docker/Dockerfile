#############################################################
# Dockerfile to build a sample tool container for BAMStats
#############################################################

# Set the base image to Ubuntu
FROM ubuntu:20.04

# File Author / Maintainer
MAINTAINER Ilya Shlyakhter <ilya_shl@alum.mit.edu>

# Setup packages
USER root
RUN apt-get -m update && apt-get install -y wget unzip curl zip build-essential python3

# get the tool and install it in /usr/local/bin
# RUN wget -q http://downloads.sourceforge.net/project/bamstats/BAMStats-1.25.zip
# RUN unzip BAMStats-1.25.zip && \
#     rm BAMStats-1.25.zip && \
#     mv BAMStats-1.25 /opt/
# COPY bin/bamstats /usr/local/bin/
# RUN chmod a+x /usr/local/bin/bamstats

RUN wget -q https://github.com/broadinstitute/cosi2/archive/v2.3.2rc8.zip
RUN unzip v2.3.2rc8.zip && rm v2.3.2rc8.zip && cd cosi2-2.3.2rc8 && ./configure && make install
RUN cd cosi2-2.3.2rc8 && VERBOSE=1 make check && cd .. && rm -rf cosi2-2.3.2rc8
RUN strip /usr/local/bin/coalescent && rm /usr/local/bin/sample_stats_extra \
    && rm /usr/local/bin/get_recomap && rm /usr/local/bin/recomap_hapmap2 && rm /usr/local/bin/recosimulate \
    && rm /usr/local/lib/libcosi*
RUN apt-get remove -y wget unzip zip curl build-essential && apt-get autoremove -y
#RUN apt-get remove -y wget unzip zip curl build-essential python3 && apt-get autoremove -y

# RUN curl -S https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh > miniconda.sh && \
#     chmod u+x miniconda.sh && \
#     bash miniconda.sh -b -p /usr/local/miniconda && \
#     conda config --set always_yes yes --set changeps1 no --set remote_max_retries 6 && \
#     conda config --add channels defaults && \
#     conda config --add channels bioconda && \
#     conda config --add channels conda-forge

# switch back to the ubuntu user so this tool (and the files written) are not owned by root
RUN groupadd -r -g 1000 ubuntu && useradd -r -g ubuntu -u 1000 -m ubuntu
USER ubuntu

VOLUME ["/user-data"]
ENV \
    COSI2_DOCKER_DATA_PATH="/user-data" \
    COSI_NEWSIM=1

# by default /bin/bash is executed
CMD ["/bin/bash"]
