#!/bin/bash

# Install python dependency.
sudo apt-get -y install python3-dev libpq-dev
pip3 install psycopg2

# Install HypoPg.
sudo apt-get -y install postgresql-server-dev-14
wget -O hypopg.tar.gz https://github.com/HypoPG/hypopg/archive/refs/tags/1.3.1.tar.gz
tar -xf hypopg.tar.gz
cd hypopg-1.3.1/
sudo make install
cd ..
yes | rm -r hypopg*