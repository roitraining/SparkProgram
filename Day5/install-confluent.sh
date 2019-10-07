#! /bin/sh
wget -qO – http://packages.confluent.io/deb/3.3/archive.key | sudo apt-key add –
add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/5.3 stable main"
apt-get update
apt-get install -y confluent-community-2.12


