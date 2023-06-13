#!/bin/bash

ENV="$*"
echo "export $ENV" >> /etc/profile.d/99-gluten.sh
