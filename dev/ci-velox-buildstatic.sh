yum install sudo patch java-1.8.0-openjdk-devel -y
cd $GITHUB_WORKSPACE/ep/build-velox/src
./get_velox.sh
source /opt/rh/devtoolset-9/enable
cd $GITHUB_WORKSPACE/
source ./dev/vcpkg/env.sh
sed -i '/^headers/d' ep/build-velox/build/velox_ep/CMakeLists.txt
export NUM_THREADS=4
./dev/builddeps-veloxbe.sh --build_tests=OFF --build_benchmarks=OFF --enable_s3=ON --enable_gcs=OFF --enable_hdfs=ON --enable_abfs=ON
