package net.sourceforge.fractal.replication.simplestatemachine;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.SynchroProcess;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream.RECOVERY;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.replication.GPaxosReplicationStream;
import net.sourceforge.fractal.replication.RBCastReplicationStream;
import net.sourceforge.fractal.replication.ReplicationStream;
import net.sourceforge.fractal.utils.PerformanceProbe.FloatValueRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;
/**   
* @author P. Sutra
* 
*/

public class SimpleStateMachine {

	private static void usage(){
		
		System.out.println(
				"Usage: "+
				" fractalConfigFile" +
				" #nbSimulatedClientsPerSite" +
				" #commandsPerClient" +
				" #nbRegister " +
				" ReplicationStream " +
				" [cstructClassName useFastBallot recovery checkpointSize ballotTO]");
		System.out.println("\n ReplicationStream = 0|1|2 where 0 = abcast, 1 = gpaxos and 2 = rbcast");
		
		System.exit(-1);
		
	}

	public static void main(String[] args){
		
		if(args.length!=5 && args.length != 10){
			usage();
		}else{

			FractalManager fl = FractalManager.init(args[0],null);
			int nbSimulatedClients = Integer.valueOf(args[1]);
			int cmds = Integer.valueOf(args[2]);
			int nbRegisters = Integer.valueOf(args[3]);
						
			// 1 - Partitionnate nodes and check usage
			Membership mb = FractalManager.getInstance().membership;
			Group Clients = mb.getOrCreateTCPGroup("SimpleStateMachineClients",8000);
			mb.getOrCreateTCPGroup("Proposers",8001);
			mb.getOrCreateTCPGroup("Acceptors",8002);
			mb.getOrCreateTCPGroup("Learners", 8003);
			
			// The first 3 nodes implement the state machine, the rest are clients.
			if( mb.allNodes().size() != 1 && mb.allNodes().size()<4 ){
				usage();
			}
			
			if(mb.allNodes().size()==1){
				mb.addNodeTo(mb.myId(),"Acceptors");
				mb.addNodeTo(mb.myId(),"Proposers");
				mb.addNodeTo(mb.myId(),"SimpleStateMachineClients");
			}else{
				for(int p=0; p<3; p++){
					mb.addNodeTo(p,"Acceptors");
					mb.addNodeTo(p,"Proposers");
				}			
				for(int p=3; p<mb.allNodes().size();p++){
					mb.addNodeTo(p,"SimpleStateMachineClients");
					mb.addNodeTo(p,"Learners");
				}
			}
			
			fl.start();
			
			// 2 - create client ids
			Set<Integer>  ids = new HashSet<Integer>();
			if(Clients.contains(mb.myId())){
				for(int i=1; i<=nbSimulatedClients; i++){
					ids.add((FractalManager.getInstance().membership.myId()+1)*1000+i);
				}
			}
			
			// 3- Create and start the replication stream
			int replicationStreamType = Integer.valueOf(args[4]);
			ReplicationStream STMstream=null;
			
			switch(replicationStreamType){
				
//			case 0: 		
//					fl.getOrCreateRBCastStream("SimpleStateMachine", "SimpleStateMachineClients");
//					fl.getOrCreatePaxosStream("SimpleStateMachine", "Proposers", "Acceptors", "Learners");
//					STMstream = fl.getOrCreateABCastStream("SimpleStateMachine", "SimpleStateMachine","SimpleStateMachine");
//					break;
//				
			case 1:
				String cstructClassName = args[5];
				boolean useFastBallot = Boolean.valueOf(args[6]);
				RECOVERY recovery = RECOVERY.valueOf(args[7]);
				int checkpointSize = Integer.valueOf(args[8]);
				int ballotTimeOut = Integer.valueOf(args[9]);
				STMstream = new GPaxosReplicationStream(ids,
														 fl.getOrCreateGPaxosStream("SimpleStateMachine", "Proposers", "Acceptors", "Learners",
																 					cstructClassName, useFastBallot, recovery, ballotTimeOut, checkpointSize),
													     nbRegisters);
				break;
				
			case 2:
				STMstream = new RBCastReplicationStream(ids,fl.getOrCreateBroadcastStream("SimpleStateMachine", "SimpleStateMachineClients"),nbRegisters);
				break;
					
			default: usage();
			}

			SynchroProcess.synchronise();
			STMstream.start();
			
			
			// 4 - Create and start the clients
			if(Clients.contains(mb.myId())){
			
				ArrayList<SimpleStateMachineReplicationClient> clients = new ArrayList<SimpleStateMachineReplicationClient>();								

				for(int i : ids){
					clients.add(new SimpleStateMachineReplicationClient(i,STMstream,cmds,nbRegisters));
				}

				for(int i=0; i<nbSimulatedClients; i++){
					clients.get(i).start();
				}

				for(int i=0; i<nbSimulatedClients; i++){
					clients.get(i).join();
				}

				FloatValueRecorder latency= new FloatValueRecorder("StateMachine#latency");
				latency.setFormat("%a");
				FloatValueRecorder latencyStdDeviation = new FloatValueRecorder("StateMachine#latencyStdDeviation");
				latencyStdDeviation.setFormat("%a");

				for(int i=0; i<nbSimulatedClients; i++){
					clients.get(i).latencyRecorder.setFormat("%a");
					latency.add(Double.valueOf(clients.get(i).latencyRecorder.toString()));
					clients.get(i).latencyRecorder.setFormat("%d");
					latencyStdDeviation.add(Double.valueOf(clients.get(i).latencyRecorder.toString()));
					clients.get(i).latencyRecorder.release();
				}

				ValueRecorder throughput= new ValueRecorder("StateMachine#throughput");
				throughput.setFormat("%t");
				throughput.add((long)(1000000/Double.valueOf(latency.toString()))*nbSimulatedClients);

			}
				
			// 4 - Wait that clients terminate
			SynchroProcess.synchronise();
			STMstream.stop();
			fl.stop();
			System.exit(0);

		}
	}

}
