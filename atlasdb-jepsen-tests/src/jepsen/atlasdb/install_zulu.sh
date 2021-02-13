#!/bin/bash

set -x

sudo apt-key adv --key-server "hkp://keyserver.ubuntu.com:80" --recv-keys "0xB1998361219BD9C9"
wget "https://cdn.azul.com/zulu/bin/zulu-repo_1.0.0-2_all.deb"

sudo apt-get -y --force-yes install apt-transport-https
sudo dpkg -i ./zulu-repo_1.0.0-2_all.deb
sudo apt-get -y --force-yes install -f

# See Signal-Desktop#2483
sudo apt-get -y --force-yes remove :libgnutls-deb0-28
sudo apt-get update
sudo apt-get -y --force-yes install zulu8-jre-headless
