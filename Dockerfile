######################################################################
# Dockerfile to build an image for computing CMS oomponent statistics
######################################################################

# Adapted from https://raw.githubusercontent.com/broadinstitute/viral-baseimage/master/Dockerfile

# Set the base image to Ubuntu
FROM ubuntu:20.04

# File Author / Maintainer
MAINTAINER Ilya Shlyakhter <ilya_shl@alum.mit.edu>

# Setup packages
USER root
RUN apt-get -m update && DEBIAN_FRONTEND=noninteractive apt-get install -y locales wget unzip curl zip python3

# Set default locale to en_US.UTF-8
#ENV LANG="en_US.UTF-8" LANGUAGE="en_US:en" LC_ALL="en_US.UTF-8"

# RUN wget -q https://github.com/broadinstitute/cosi2/archive/v2.3.2rc8.zip
# RUN unzip v2.3.2rc8.zip && rm v2.3.2rc8.zip && cd cosi2-2.3.2rc8 && ./configure && make install
# RUN cd cosi2-2.3.2rc8 && VERBOSE=1 make check && cd .. && rm -rf cosi2-2.3.2rc8
# RUN strip /usr/local/bin/coalescent && rm /usr/local/bin/sample_stats_extra \
#     && rm /usr/local/bin/get_recomap && rm /usr/local/bin/recomap_hapmap2 && rm /usr/local/bin/recosimulate \
#     && rm /usr/local/lib/libcosi*
# RUN apt-get remove -y wget unzip zip curl build-essential && apt-get autoremove -y
#RUN apt-get remove -y wget unzip zip curl build-essential python3 && apt-get autoremove -y

# RUN curl -S https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh > miniconda.sh && \
#     chmod u+x miniconda.sh && \
#     bash miniconda.sh -b -p /usr/local/miniconda && \
#     conda config --set always_yes yes --set changeps1 no --set remote_max_retries 6 && \
#     conda config --add channels defaults && \
#     conda config --add channels bioconda && \
#     conda config --add channels conda-forge


# install miniconda3 with our default channels and no other packages
ENV MINICONDA_PATH="/opt/miniconda"
COPY install-miniconda.sh /opt/docker/
RUN /opt/docker/install-miniconda.sh

ENV PATH="$MINICONDA_PATH/bin:$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

RUN mkdir -p /tmp/miniconda/miniconda/conda-bld
COPY conda-bld /tmp/miniconda/miniconda/conda-bld

RUN conda install -c file:///tmp/miniconda/miniconda/conda-bld numpy scipy matplotlib pandas selscan=1.3.0a06

RUN rm -rf /tmp/miniconda

#COPY model .
#RUN make

# switch back to the ubuntu user so this tool (and the files written) are not owned by root
RUN groupadd -r -g 1000 ubuntu && useradd -r -g ubuntu -u 1000 -m ubuntu
USER ubuntu

#COPY cms .
#COPY sim_generation .

VOLUME ["/user-data"]
ENV \
    DOCKER_DATA_PATH="/user-data"

# by default /bin/bash is executed
# set up entrypoint
CMD ["/bin/bash"]

