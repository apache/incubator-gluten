#!/bin/bash
# Execute this script in host machine
set -eu

wget https://developer.download.nvidia.com/compute/cuda/12.8.1/local_installers/cuda-repo-amzn2023-12-8-local-12.8.1_570.124.06-1.x86_64.rpm
sudo rpm -i cuda-repo-amzn2023-12-8-local-12.8.1_570.124.06-1.x86_64.rpm
sudo dnf clean all
sudo dnf -y install cuda-toolkit-12-8
sudo dnf -y module install nvidia-driver:open-dkms

sudo yum install -y docker

curl -s -L https://nvidia.github.io/libnvidia-container/stable/rpm/nvidia-container-toolkit.repo | \
  sudo tee /etc/yum.repos.d/nvidia-container-toolkit.repo
sudo dnf install -y nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl restart docker
# May need reboot here after install cuda driver
# Run the gpu example
sudo docker run --rm --runtime=nvidia --gpus all ubuntu nvidia-smi
# Then run this command to
sudo docker run --name gpu_gluten_container --gpus all -itd apache/gluten:centos-9-jdk8-cudf
