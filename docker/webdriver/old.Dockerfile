######################################################################
# Dockerfile to build an image for computing CMS oomponent statistics
######################################################################

# Adapted from https://raw.githubusercontent.com/broadinstitute/viral-baseimage/master/Dockerfile

# Set the base image to Ubuntu
#FROM ubuntu:20.04
FROM debian

# File Author / Maintainer
MAINTAINER Ilya Shlyakhter <ilya_shl@alum.mit.edu>

# Setup packages
USER root
RUN apt-key adv --fetch-keys "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xea6e302dc78cc4b087cfc3570ebea9b02842f111" \
&& echo 'deb http://ppa.launchpad.net/chromium-team/beta/ubuntu bionic main ' >> /etc/apt/sources.list.d/chromium-team-beta.list \
&& apt update
RUN export DEBIAN_FRONTEND=noninteractive \
&& export DEBCONF_NONINTERACTIVE_SEEN=true \
&& apt-get -y install chromium-browser
RUN apt-get -y install python3-selenium

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

