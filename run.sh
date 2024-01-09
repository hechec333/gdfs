#!/bin/bash

function utime(){
    echo -n "[`date "+%Y-%m-%d %H:%M:%S"`]"
}
echo ">>> start gdfs clusters configuartion"
utime

echo ">>> start running Master-m1"
go run cmd/main.go -r m -u 1 &

echo ">>> start running Master-m2"
go run cmd/main.go -r m -u 2 &

echo ">>> start running Master-m3"
go run cmd/main.go -r m -u 3 &

utime
sleep 1s

echo ">>> start running ChunkServer-ck1"
go run cmd/main.go -r c -u 1 &

echo ">>> start running ChunkServer-ck2"
go run cmd/main.go -r c -u 2 &

echo ">>> start running ChunkServer-ck3"
go run cmd/main.go -r c -u 3 &


echo ">>> finish running gdfs clusters"


sleep 3s