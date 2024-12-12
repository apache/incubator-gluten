# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# To set up the virtual environment required to run this script,
# refer to the `Format and mount disks` subsection under `System Setup` in initialize.ipynb.
import sys
import subprocess
import questionary
import json

def yes_or_no(question):
    while True:
        user_input = input(question + '(yes/no/quit): ')
        if user_input.lower() == 'yes':
            return True
        elif user_input.lower() == 'no':
            return False
        elif user_input.lower() == 'quit':
            sys.exit(1)
        else:
            continue

def filter_empty_str(l):
    return [x for x in l if x]

def run_and_log(cmd):
    # Print command in yellow
    print('\033[93m' + '>>> Running command: ' + repr(cmd) + '\033[0m')
    result = subprocess.run(cmd, check=True, shell=True, capture_output=True, text=True)
    # Print stdout in green
    print('\033[92m' + '==========stdout==========' + '\033[0m')
    print(result.stdout)
    # Print stderr in red
    print('\033[91m' + '==========stderr==========' + '\033[0m')
    print(result.stderr)

def init_disks():
    all_disks = filter_empty_str(subprocess.run("lsblk -I 7,8,259 -npd --output NAME".split(' '), capture_output=True, text=True).stdout.split('\n'))
    if not all_disks:
        print("No disks found on system. Exit.")
        sys.exit(0)

    answer = False
    disks = []
    while not answer:
        disks = questionary.checkbox('Select disks to initialize:', choices=all_disks).ask()
        answer = yes_or_no('Confirm selected:\n' + '\n'.join(disks) + '\n')

    if not disks:
        print('No disks are selected.')
        return

    for d in disks:
        print('Initializing {} ...'.format(d))
        run_and_log('wipefs -a {}'.format(d))
        run_and_log('echo "g\nw\n" | fdisk {}'.format(d))
        run_and_log('echo "n\n\n\n\nw\n" | fdisk {}'.format(d))
        run_and_log('mkfs.ext4 {}p1'.format(d))

def mount_partitions():
    subprocess.run('lsblk -pf --json > lsblk.json', shell=True)
    partitions = []
    with open('lsblk.json', 'r') as f:
        data = json.load(f)
        for d in data['blockdevices']:
            if 'children' in d:
                for c in d['children']:
                    if c['fstype'] == 'ext4':
                        partitions.append(c['name'])
    answer = False
    while not answer:
        partitions = questionary.checkbox('Select partitions to create mount points:', choices=partitions).ask()
        answer = yes_or_no('Confirm selected:\n' + '\n'.join(partitions) + '\n')

    for i, p in enumerate(partitions):
        d = 'data{}'.format(i)
        run_and_log('e2label {} ""'.format(p))
        run_and_log('e2label {} {}'.format(p, d))
        run_and_log('mkdir -p /{}'.format(d))
        run_and_log('mount -L {} /{}'.format(d, d))

def choose():
    choice = questionary.select('Select operation:', choices=['Format disks', 'Mount partitions']).ask()
    print(choice)
    if choice == 'Format disks':
        init_disks()
    elif choice == 'Mount partitions':
        mount_partitions()

if __name__ == '__main__':
    choose()
