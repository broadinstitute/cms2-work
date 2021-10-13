#
#   Docker file for the image "chromium brower without snap"
#
FROM selenium/standalone-chrome:latest
MAINTAINER ilya_shl@alum.mit.edu
 
#chromium browser
#original PPA repository, use if our local fails
#RUN echo "tzdata tzdata/Areas select Etc" | debconf-set-selections && echo "tzdata tzdata/Zones/Etc select UTC" | debconf-set-selections
USER root
RUN apt-get -m -y update && apt-get -y upgrade
RUN export DEBIAN_FRONTEND=noninteractive && export DEBCONF_NONINTERACTIVE_SEEN=true
RUN apt-get -y install gnupg2 apt-utils wget python3-pip chromium-chromedriver
RUN pip3 install chromedriver-binary-auto
RUN export PATH=$PATH:/home/seluser/.local/bin
#RUN wget -O /root/chromium-team-beta.pub "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xea6e302dc78cc4b087cfc3570ebea9b02842f111" && apt-key add /root/chromium-team-beta.pub
#RUN apt-key adv --fetch-keys "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xea6e302dc78cc4b087cfc3570ebea9b02842f111" && echo 'deb http://ppa.launchpad.net/chromium-team/beta/ubuntu bionic main ' >> /etc/apt/sources.list.d/chromium-team-beta.list && apt update
RUN export DEBIAN_FRONTEND=noninteractive && export DEBCONF_NONINTERACTIVE_SEEN=true && apt-get -y install chromium-browser chromium-chromedriver
RUN apt-get -y install python3-selenium python3-pip
RUN pip3 install chromedriver-binary-auto

# switch back to the ubuntu user so this tool (and the files written) are not owned by root
RUN groupadd -r -g 1000 ubuntu && useradd -r -g ubuntu -u 1000 -m ubuntu
USER ubuntu

# by default /bin/bash is executed
# set up entrypoint
CMD ["/bin/bash"]

