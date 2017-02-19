#!/bin/bash

sudo service clamav-freshclam stop
sudo service couchdb stop
sudo service memcached stop
sudo service mongodb stop
sudo service mysql stop
sudo service postgresql stop
sudo service rabbitmq-server stop
sudo service redis-server stop
sudo service zookeeper stop

# We don't care if any services didn't exist/failed to stop
exit 0
