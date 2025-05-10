pushd ~
mkdir jars
mkdir shared-libs
mkdir tpch_sf100_parquet_zstd
wget https://dl.influxdata.com/telegraf/releases/telegraf-1.34.1_linux_amd64.tar.gz
tar zxvf ./telegraf-1.34.1_linux_amd64.tar.gz > /dev/null
popd

docker buildx build --load --platform "linux/amd64" -t gluten-images -f ./Dockfile.centos9-run .

