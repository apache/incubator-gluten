#!/bin/bash

echo "Running gluten-buildenv docker container with SSH X11 forwarding enabled. SSH password: 123"
/usr/sbin/sshd -D
