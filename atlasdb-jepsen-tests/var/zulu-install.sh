apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 0xB1998361219BD9C9
apt-get update
apt-get install -y apt-transport-https
wget https://cdn.azul.com/zulu/bin/zulu-repo_1.0.0-2_all.deb
dpkg -i zulu-repo_1.0.0-2_all.deb
apt-get update
apt-get install -y zulu8-jre-headless
export JAVA_HOME=/usr/lib/jvm/zulu8
rm zulu-repo_1.0.0-2_all.deb