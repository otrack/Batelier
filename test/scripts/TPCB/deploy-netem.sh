#!/bin/bash

source configuration.sh


# usage deploy_internet ${node} ${delay} ${rate}
function deploy_internet(){

    echo "deploying internet topology on node $1"

    # reset 
    ${SSHCMD} ${1} sudo tc qdisc del dev eth0 root
    ${SSHCMD} ${1} sudo tc qdisc add dev eth0 handle 1: root htb default 30
    
    # regular traffic
    ${SSHCMD} ${1} sudo tc class add dev eth0 parent 1:1 classid 1:30 htb rate 1000mbit
    ${SSHCMD} ${1} sudo tc qdisc add dev eth0 parent 1:30 sfq
    # do not throttle ssh
    ${SSHCMD} ${1} sudo tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dport 22 0xffff flowid 1:30
    
    # group $j to all other groups traffic
    ${SSHCMD} ${1} sudo tc class add dev eth0 parent 1:1 classid 1:11 htb rate ${3}kbit ceil ${3}kbit
    ${SSHCMD} ${1} sudo tc qdisc add dev eth0 parent 1:11 netem delay ${2}ms 
    
    let f=${#groups[@]}-1
    for k in `seq 0 $f| sed s/$i//`;
    do
 	h=${groups[$k]}
	for l in ${h};
	do  
 	    remote=`host ${l} | cut -d" " -f 4`
	    # don't trottle ssh

            # Internet topology
	    ${SSHCMD} ${1} sudo tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst ${remote}/32 flowid 1:11
	done
    done

}


# usage ${delay} ${rate}

let e=${#groups[@]}-1
for i in `seq 0 $e`
  do 

  g=${groups[$i]}

    for j in ${g}
    do

	deploy_internet ${j} ${1} ${2} &

    done
  
 done

wait

# Triangle topology    
#     for k in `seq 1 ${ngroups}| sed s/$i//`;
#       do
#       # group $j to group $k traffic
#       ${SSHCMD} ${j} sudo tc class add dev eth0 parent 1:1 classid 1:1$k htb rate ${2}kbit ceil ${2}kbit
#       ${SSHCMD} ${j} sudo tc qdisc add dev eth0 parent 1:1$k netem delay ${1}ms;
#       h=group$k;
#       for l in ${!h};
# 	do 
# 	${SSHCMD} ${j} sudo tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst $l/32 flowid 1:1$k;
#       done
#     done
