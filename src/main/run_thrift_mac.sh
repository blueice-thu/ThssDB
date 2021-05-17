rm -rf ./java/cn/edu/thssdb/rpc/thrift
mkdir ./java/cn/edu/thssdb/rpc/thrift
chmod +x ./thrift/thrift_0.12.0_0.13.0_mac.exe
./thrift/thrift_0.12.0_0.13.0_mac.exe --gen java thrift/rpc.thrift
cp ./gen-java/cn/edu/thssdb/rpc/thrift/*.* ./java/cn/edu/thssdb/rpc/thrift