rmdir /s/q .\java\cn\edu\thssdb\rpc\thrift
mkdir .\java\cn\edu\thssdb\rpc\thrift
start .\thrift\thrift-0.13.0-win-x86_64.exe --gen java thrift\rpc.thrift
copy .\gen-java\cn\edu\thssdb\rpc\thrift\*.* .\java\cn\edu\thssdb\rpc\thrift