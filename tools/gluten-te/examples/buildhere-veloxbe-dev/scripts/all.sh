#!/bin/bash

set -ex

# Build Gluten
EXTRA_MAVEN_OPTIONS="-Pspark-3.2 \
                     -Pbackends-velox \
                     -DskipTests \
                     -Dscalastyle.skip=true \
                     -Dcheckstyle.skip=true"

cd /opt/gluten
bash dev/builddeps-veloxbe.sh
mvn clean install $EXTRA_MAVEN_OPTIONS

# Setup SSH server
apt-get update
apt-get -y -q --no-install-recommends install openssh-server

echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config.d/override.conf
echo 'X11Forwarding yes' >> /etc/ssh/sshd_config.d/override.conf
echo 'X11UseLocalhost no' >> /etc/ssh/sshd_config.d/override.conf

service ssh restart
echo -e "123\n123" | passwd

# Install IDEs
apt-get update
apt-get -y -q --no-install-recommends install libgbm-dev libxkbcommon-dev
mkdir -p /opt/ide
cd /opt/ide
wget https://download.jetbrains.com/idea/ideaIC-2022.3.2.tar.gz
tar -xvzf ideaIC-2022.3.2.tar.gz
wget https://download.jetbrains.com/cpp/CLion-2022.3.2.tar.gz
tar -xvzf CLion-2022.3.2.tar.gz

# Hold the prompt
bash
