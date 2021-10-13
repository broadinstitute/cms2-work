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

# install miniconda3 with our default channels and no other packages
ENV MINICONDA_PATH="/opt/miniconda"
COPY install-miniconda.sh /opt/docker/
RUN /opt/docker/install-miniconda.sh

ENV PATH="$MINICONDA_PATH/bin:$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

#RUN mkdir -p /tmp/miniconda/miniconda/conda-bld
#COPY conda-bld /tmp/miniconda/miniconda/conda-bld

#RUN conda install -c file:///tmp/miniconda/miniconda/conda-bld tar numpy scipy matplotlib pandas pytables h5py psutil tabix selscan=1.3.0a09 && conda clean -a -y
RUN conda install tar numpy scipy matplotlib pandas pytables h5py psutil tabix dominate bedtools && conda clean -a -y

#RUN rm -rf /tmp/miniconda

COPY Dockerfile /opt/docker/Dockerfile.save.txt
COPY misc_utils.py /opt/pyutils/

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
