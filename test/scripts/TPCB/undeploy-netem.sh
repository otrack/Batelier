#!/bin/sh

source configuration.sh

let e=${#groups[@]}-1
for i in `seq 0 $e`
  do 

  g=${groups[$i]}

  for j in ${g}
    do

      # reset 
      ${SSHCMD} ${j} sudo tc qdisc del dev eth0 root
      ${SSHCMD} ${j} sudo tc qdisc add dev eth0 handle 1: root htb default 30
      
  done
  
 done
