#!/bin/bash
pw="/mnt/h/CODEfield/GO/src/project/gdfs"

rm -f ${pw}/debug/*.log
rm -f ${pw}/.raft
rm -f ${pw}/.snap
ports=(3679 3680 3681 4679 4680 4681)
for port in ${ports[@]};do
  echo "kill $port"
  kill -9 `netstat -lnput | grep ${port} | awk '{print $NF}' | awk -F / '{print $1}'` > /dev/null
done
