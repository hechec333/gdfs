#!/bin/bash
function utime(){
    echo -n "[`date "+%Y-%m-%d %H:%M:%S"`]"
}
rmnode=$1
if [[ $rmnode < 1 ]];then
rmnode=1
fi
echo ">>> testing master cluster fault tolerance"
echo ">>> current setting will randomly remove ${rmnode} node to test fault-torlance"

# fresh build
if [[ -f "ctl" ]];then
  echo ">>> build fresh toolkit ctl"
  rm -f ctl 
fi

utime
echo " build debug tool"
go build toolkit/debug/ctl.go 

if [[ $? -ne 0 ]];then
  echo ">>> build toolkit ctl fail"
  exit
fi


if [[ -f "run.sh" ]];then
  echo ">>> build fresh run script"
  rm -f run.sh
fi


utime
echo " build startup script"
cd build && go run build.go script && cd ..

if [[ $? -ne 0 ]];then
  echo "build run.sh fail"
  exit
fi 

utime
echo "start the testing cluster"
chmod +x run.sh
/bin/bash run.sh >/dev/null 2>&1 &

sleep 3s
utime
echo " query current leader"
./ctl -check m

utime
echo " query current chunkserver stat"
./ctl -check c

mnodes=(`./ctl -check m | awk 'n==1{print} $0~/**************************/{n=1}'  | sed -n "1p"`)
master=`./ctl -check m| awk 'n==1{print} $0~/**************************/{n=1}'  | grep Master | awk '{for(i=1;i<=NF;i++) if($i=="Master") print i} `

echo "current leader in ${mnodes[$master]}"