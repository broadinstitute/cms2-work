######################################################################
# Dockerfile to build an image for wget
######################################################################

# Adapted from https://raw.githubusercontent.com/broadinstitute/viral-baseimage/master/Dockerfile

# Set the base image to Ubuntu
FROM ubuntu:20.04

# File Author / Maintainer
MAINTAINER Ilya Shlyakhter <ilya_shl@alum.mit.edu>

# Setup packages
USER root
RUN apt-get -m update && DEBIAN_FRONTEND=noninteractive apt-get install -y locales coreutils wget unzip

# Set default locale to en_US.UTF-8
#ENV LANG="en_US.UTF-8" LANGUAGE="en_US:en" LC_ALL="en_US.UTF-8"

COPY Dockerfile /opt/docker/Dockerfile.save.txt

# switch back to the ubuntu user so this tool (and the files written) are not owned by root
RUN groupadd -r -g 1000 ubuntu && useradd -r -g ubuntu -u 1000 -m ubuntu
USER ubuntu

CMD ["/bin/bash"]

