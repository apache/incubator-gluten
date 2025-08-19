docker run -itd --network host --name gluten_run \
	-v ~/jars:/opt/gluten/jars \
	-v $PWD/../../../tools/gluten-it/common/src/main/resources/tpch-queries:/opt/spark/tpch-queries \
	-v $PWD/../../../tools/gluten-it/common/src/main/resources/tpcds-queries:/opt/spark/tpcds-queries \
	-v ~/telegraf-1.34.1/usr/bin/telegraf:/usr/bin/telegraf \
	-v ~/.aws:/home/spark/.aws \
	-v ~/tpch_sf100_parquet_zstd:/opt/spark/database/tpch_sf100_parquet_zstd \
	gluten-images:latest
