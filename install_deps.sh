#!/bin/bash

# Install Postgres.
sudo sh -c 'echo "deb [arch=amd64] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get -y install postgresql-14

# Install python dependency.
sudo apt install python3-dev libpq-dev
sudo apt install python3-pip python3-doit
pip3 install doit psycopg2

# Install Java 17.
sudo add-apt-repository ppa:linuxuprising/java
sudo apt update
sudo apt install oracle-java17-installer

# Install HypoPg.
sudo apt install postgresql-server-dev-14
wget -O hypopg.tar.gz https://github.com/HypoPG/hypopg/archive/refs/tags/1.3.1.tar.gz
tar -xf hypopg.tar.gz
cd hypopg-1.3.1/
sudo make install
cd ..
yes | rm -r hypopg*