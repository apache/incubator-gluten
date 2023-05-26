#!/bin/bash

set -ex

# Build Gluten
EXTRA_MAVEN_OPTIONS="-Pspark-3.2 \
                     -Pbackends-velox \
                     -Prss \
                     -DskipTests \
                     -Dscalastyle.skip=true \
                     -Dcheckstyle.skip=true"

cd /opt/gluten
bash dev/builddeps-veloxbe.sh --build_type=Debug --enable_ep_cache=ON
mvn clean install $EXTRA_MAVEN_OPTIONS

apt-get -y -q --no-install-recommends install firefox tmux openjdk-8-source

# Install IDEs
apt-get update
apt-get -y -q --no-install-recommends install libgbm-dev libxkbcommon-dev
mkdir -p /opt/ide
cd /opt/ide
wget https://download.jetbrains.com/idea/ideaIC-2022.3.2.tar.gz
tar -xvzf ideaIC-2022.3.2.tar.gz
wget https://download.jetbrains.com/cpp/CLion-2022.3.2.tar.gz
tar -xvzf CLion-2022.3.2.tar.gz
