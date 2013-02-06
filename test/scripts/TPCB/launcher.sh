#!/bin/bash

source /home/psutra/tpcb/configuration.sh

function stopExp(){

    let e=${#nodes[@]}-1
    for i in `seq 0 $e`
    do
	echo "stopping on ${nodes[$i]}"
	nohup ${SSHCMD} ${nodes[$i]} "killall java" 2&>1 > /dev/null &
    done

}

function collectStats(){

    #
    # usage collectStats nnodes replicationFactor
    # 

    throughput=0;
    commitTime=0;
    commitTimeStdDeviation=0;
    abortRate=0;
    globalRatio=0;

    let e=${#nodes[@]}-1
    for i in `seq 0 $e`
    do

	node=${nodes[$i]}

	tmp=`grep throughput\: ${scriptdir}/${node} | gawk -F':' '{print $2}'`;
	throughput=`echo "${tmp} + ${throughput}" | bc`;

	tmp=`grep "commitTime\:" ${scriptdir}/${node} | gawk -F':' '{print $2}'`;
	commitTime=`echo "${tmp} + ${commitTime}" | bc`;

	tmp=`grep commitTimeStdDeviation ${scriptdir}/${node} | gawk -F':' '{print $2}'`;
	commitTimeStdDeviation=`echo "${tmp} + ${commitTimeStdDeviation}" | bc`;

	tmp=`grep  abortRate ${scriptdir}/${node} | gawk -F':' '{print $2}'`;
	abortRate=`echo "${tmp} + ${abortRate}" | bc`;

	tmp=`grep  percentageGlobalTransactions ${scriptdir}/${node} | gawk -F':' '{print $2}'`;
	globalRatio=`echo "${tmp} + ${globalRatio}" | bc`;
    done

    throughput=`echo "scale=2; ${throughput}" | bc `;
    commitTime=`echo "scale=2; ${commitTime} / $1" | bc`;
    commitTimeStdDeviation=`echo "scale=2; ${commitTimeStdDeviation} / $1" | bc`;
    if [[ ${globalRatio} == *E* ]]
    then
	abortRate=`echo "scale=2; ( 100 * ${abortRate} ) / $1" | awk -F"E" '{print $1 * ( 10 ^ $2)}' | bc`;
    else
	abortRate=`echo "scale=2; ( 100 * ${abortRate} ) / $1" | bc`;
    fi
    globalRatio=`echo "scale=2; (100 * ${globalRatio} ) / $1" | bc`;

    echo -e  "$[c*nnodes]\t${throughput}\t${commitTime}\t${commitTimeStdDeviation}\t${abortRate}\t${globalRatio}"

}

trap "stopExp; wait; exit 255" SIGINT SIGTERM


# 2 - Experimentation

# DB setup 
client_increment="5"
client_glb="5"
client_lub="5"
client=`seq ${client_glb} ${client_increment} ${client_lub}`

global_increment="1"
global_glb="25"
global_lub="25"
global=`seq ${global_glb} ${global_increment} ${global_lub}`

rfactor_increment="1"
rfactor_glb="8"
rfactor_lub="8"
replicationFactor=`seq ${rfactor_glb} ${rfactor_increment} ${rfactor_lub}`

# Network setup
latency_increment="10"
latency_glb="30"
latency_lub="30"
latency=`seq ${latency_glb} ${latency_increment} ${latency_lub}`
rate=1000000

for l in ${latency}
do

    echo "deploying network with latency ${l} rate ${rate}";

    if [[ "${l}" == "0" ]]
    then
	${scriptdir}/undeploy-netem.sh
    else
	${scriptdir}/deploy-netem.sh ${l} ${rate}
    fi

    mkdir -p ${scriptdir}/results/group${ngroups}
    mkdir -p ${scriptdir}/results/group${ngroups}/latency${l}

    for r in ${replicationFactor}
    do

	if [[ "${r}" == "8" ]]
	then
	    global="0"
	fi

	for g in ${global}
	do   

	    percentageGlobal=`echo "scale=1${global} / 100" | bc`

	    for c in ${client}
	    do

		echo "using replication factor ${r} global ${g} client ${c}"

		let e=${#nodes[@]}-1
		for i in `seq 0 $e`
		do
		    echo "launching on ${nodes[$i]}"
		    nohup ${SSHCMD} ${nodes[$i]} "${scriptdir}/tpcb.sh ${configurationFile} ${nbranches} 0.${g} ${ngroups} ${r} ${c} ${ntransactions} ${algType} ${l}" 2>&1 > ${scriptdir}/${nodes[$i]} &
		done

		wait

		collectStats ${nnodes} ${replicationFactor} >>  ${scriptdir}/results/group${ngroups}/latency${l}/result-replicationFactor${r}-percentageGlobal${g}.txt 

		sleep 20

	    done

	done

    done

done