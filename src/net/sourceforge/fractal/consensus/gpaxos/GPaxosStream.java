package net.sourceforge.fractal.consensus.gpaxos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.consensus.gpaxos.cstruct.CStructFactory;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.replication.CheckpointCommand;
import net.sourceforge.fractal.replication.Command;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;


/**
 * @author Pierre Sutra
 * 
 * An implementation of Generalized Paxos (see "Generalized Consensus and Paxos" by Leslie Lamport).
 * 
 */

public class GPaxosStream extends Stream implements Runnable{

	public static enum RECOVERY {
		DEFAULT,
		COORDINATED,
		FGGC
	};
		
	private String name;
	private int swid;
	@SuppressWarnings("unused")
	private Group pgroup, agroup, lgroup;
	private boolean isTerminated;

	private CStructFactory cfactory;
	private LinkedHashMap<Command,Integer> proposed;
	private BlockingQueue<Message> toPropose;
	private int timeout;
	
	private GPAcceptor acceptor;
	private GPLeader leader;
	private GPLearner learner;
	private Thread streamThread;
	
	@SuppressWarnings("unchecked")
	GPaxosStream(String n, int id, Group pg, Group ag, Group lg, String cstructName, boolean useFastBallot, RECOVERY recovery, int to, int checkpointSize){
		name = n;
		pgroup = pg; // removeMe
		agroup = ag;
		lgroup = lg;
		swid = id;
		isTerminated = true;				
		cfactory = CStructFactory.getInstance(cstructName);		
		proposed =  new LinkedHashMap<Command, Integer>(1000,0.75f,true){
			private static final long serialVersionUID = 1L;
			private static final int MAX_ENTRIES = 1000;

			@SuppressWarnings("unchecked")
			protected boolean removeEldestEntry(Map.Entry eldest) {
				return size() > MAX_ENTRIES;
			}
		};
		toPropose = CollectionUtils.newBlockingQueue();
			
		if(agroup.contains(swid)){
			acceptor = new GPAcceptor(id,ag,lg,this,useFastBallot,recovery,checkpointSize);
			leader = new GPLeader(id,ag,lg,this,useFastBallot,recovery, checkpointSize);
			streamThread = new Thread(this,"GPaxosProposerThread@"+swid);
			agroup.registerQueue("ProposeMessage", toPropose);
		}
		
		if(lgroup.contains(swid)){
			learner = new GPLearner(id,ag,lg,this,useFastBallot,recovery);
		}
		
		timeout=to;
		
		if( ConstantPool.PAXOS_DL>0 ) 
			System.out.println( this
									 + " created with "
									 + "\n acceptor group="+ag.name()
									 + "\n proposer group="+pg.name()
									 + "\n learner group="+lg.name()
									 + "\n cstructName="+cstructName
								     + "\n useFastBallot="+useFastBallot
								     + "\n recovery="+recovery);
		
		if(checkpointSize<3)
			throw new RuntimeException("Invalid usage; chkSize >= 3");
		
		// FIXME agroup \inter lgroup = \emptySet.
		
	}

	@Override
	public void start(){
		if(!isTerminated) throw new RuntimeException("Invalid usage");
		isTerminated = false;
		if(agroup.contains(swid)){
			acceptor.start();
			leader.start();
			streamThread.start();
		}
		if(lgroup.contains(swid))learner.start();
	}

	@Override
	public void stop(){
		if(isTerminated) throw new RuntimeException("Invalid usage");
		isTerminated = true;
		if(agroup.contains(swid)){
			acceptor.stop();
			leader.stop();
			streamThread.interrupt();
			agroup.unregisterQueue("ProposeMessage", toPropose);
		}
		if(lgroup.contains(swid)) learner.stop();
	}

	@Override
	public void deliver(Serializable  s) {
	
		Iterable<Command> u = (Iterable<Command>) s;
		for(Command c: u){
			proposed.remove(c);
			if( ConstantPool.PAXOS_DL>3 ) System.out.println( this+" I deliver "+c);
			if(c instanceof CheckpointCommand) continue;
			if(!learners.isEmpty()){
				for(Learner l : learners.get("*"))  l.learn(this,c);
			}
		}
		
	}
		
	public void propose(Command c){
		if( isTerminated )
			throw new RuntimeException(this+" Stream "+name+" is not started, or is now over");
		if( ConstantPool.PAXOS_DL>3 ) System.out.println( this+" I propose "+c);
		agroup.broadcast(new ProposeMessage(swid, c));
	}
	
	public Collection<Command> proposed() {
		return proposed.keySet();
	}
	
	public int timeout(){
		return timeout;
	}
	
	CStructFactory cstructFactory() {
		return cfactory;
	}
	
	public GPAcceptor getAcceptor(){
		return acceptor;
	}
	
	public void run() {

		try{
				
			List<Message> lm = new ArrayList<Message>();
			LinkedHashSet<Command> newlyProposed  = new LinkedHashSet<Command>();

			while(!isTerminated){

				lm.clear();
				Message msg= toPropose.poll(timeout, TimeUnit.MILLISECONDS);
				if(msg!=null) lm.add(msg);

				toPropose.drainTo(lm);

				newlyProposed.clear();
				for(Message m : lm){
						newlyProposed.add(((ProposeMessage)m).command);
						proposed.put(((ProposeMessage)m).command, null);
				}

				if( !newlyProposed.isEmpty() ){
					if( agroup.contains(swid) ){
						leader.phase2AClassic(newlyProposed);
						acceptor.phase2BFast(newlyProposed);
					}	
				}
				
			}
			
		}catch(InterruptedException e){
			if(!isTerminated)
				System.err.println(this+"no reacheable state");
		}
		
	}
	
	
	public String toString(){
		return "GPaxosStream@"+swid+":"+System.currentTimeMillis();
	}
	
}