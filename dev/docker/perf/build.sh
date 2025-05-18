pushd ~
mkdir jars
mkdir shared-libs
mkdir tpch_sf100_parquet_zstd

disk_names=( $(sudo lsblk -d -o NAME | tail -n +2) )
data_id=1
for disk in "${disk_names[@]}"; do
    echo "Checking disk: $disk"
    # If the disk is an Amazon EC2 NVMe Instance Storage volume, then install btrfs onto that disk
    if sudo fdisk -l "/dev/$disk" | grep -q "Amazon EC2 NVMe Instance Storage"; then
        echo "Disk $disk is an Amazon EC2 NVMe Instance Storage"
        sudo mkfs.ext4 /dev/$disk
        mkdir -p /mnt/data${data_id}
        sudo mount -t ext4 /dev/$disk /mnt/data${data_id}
        sudo echo "/dev/$disk /mnt/data${data_id} auto noatime 0 0" | sudo tee -a /etc/fstab
        sudo lsblk -f
        data_id=$((data_id + 1))
    else
        echo "Disk $disk is not an Amazon EC2 NVMe Instance Storage volume"
    fi
done




wget https://dl.influxdata.com/telegraf/releases/telegraf-1.34.1_linux_amd64.tar.gz
tar zxvf ./telegraf-1.34.1_linux_amd64.tar.gz > /dev/null
popd

docker buildx build --load --platform "linux/amd64" -t gluten-images -f ./Dockfile.centos9-run .

