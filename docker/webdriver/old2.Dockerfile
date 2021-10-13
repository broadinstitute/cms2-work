#
#   Docker file for the image "chromium brower without snap"
#
FROM ubuntu:20.04
MAINTAINER myuser@example.com
 
#chromium browser
#original PPA repository, use if our local fails
RUN echo "tzdata tzdata/Areas select Etc" | debconf-set-selections && echo "tzdata tzdata/Zones/Etc select UTC" | debconf-set-selections
RUN export DEBIAN_FRONTEND=noninteractive && export DEBCONF_NONINTERACTIVE_SEEN=true
RUN apt-get -y update && apt-get -y upgrade
RUN apt-get -y install gnupg2 apt-utils wget
#RUN wget -O /root/chromium-team-beta.pub "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xea6e302dc78cc4b087cfc3570ebea9b02842f111" && apt-key add /root/chromium-team-beta.pub
RUN apt-key adv --fetch-keys "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xea6e302dc78cc4b087cfc3570ebea9b02842f111" && echo 'deb http://ppa.launchpad.net/chromium-team/beta/ubuntu bionic main ' >> /etc/apt/sources.list.d/chromium-team-beta.list && apt update
RUN export DEBIAN_FRONTEND=noninteractive && export DEBCONF_NONINTERACTIVE_SEEN=true && apt-get -y install chromium-browser chromium-chromedriver
RUN apt-get -y install python3-selenium python3-pip
RUN pip3 install chromedriver-binary-auto
