 #!/bin/bash

SSHCMD="oarsh" # ssh or oarsh for oar equiped cluster
scriptdir="/home/psutra/tpcb"

# 1 - Repartition of the nodes
# We partitionate the nodes in ${ngroups} groups.
# All the resulting groups are stored in $groups, e.g., ${groups[0]} contains the first group.
# The parameters below are also used by ./deploy-netem.sh and ./undeploy-netem.sh

nodes=("cluster1u1" "cluster1u2" "cluster1u3" "cluster1u4" "cluster1u5" "cluster1u6" "cluster1u7" "cluster1u8" "cluster1u9" "cluster1u10" "cluster1u11" "cluster1u12" "cluster1u13" "cluster1u14" "cluster1u15" "cluster1u16" "cluster1u17" "cluster1u18" "cluster1u19" "cluster1u20" "cluster1u21" "cluster1u22" "cluster1u23" "cluster1u24")
ngroups="8"
nnodes=${#nodes[@]}

let nodesPerGroup=$nnodes/$ngroups
if [[ $nodesPerGroup*$ngroups -ne $nnodes ]]
then
    echo "invalid number of groups"
    exit -1
fi

for g in `seq 1 ${ngroups}`
do
    let begin=($g-1)*${nodesPerGroup}
    let end=$g*${nodesPerGroup}-1
    for i in `seq ${begin} ${end}`
    do
	if [[ $i -eq ${begin} ]]
	then
	    group=(${nodes[$i]})
	else
	    group=("`echo $group` ${nodes[$i]}")
	fi
    done
    groups[$g-1]=$group
done

delay="1" # inter-group delay
rate="1000000"  # in kilo bits per seconds

# 2 - TPCB parameters 

dbdir="/tmp/TpcbDB";
configurationFile="${scriptdir}/fractal.xml"
algType="1" # 0 -> gamcast, 1 -> ngamcast, 2 -> ftngamcast
nbranches="3200" # number of branches in the TPC-B benchmark
ntransactions="1000" # number of transactions per client

