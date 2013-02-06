package net.sourceforge.fractal.ftwanamcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.broadcast.BroadcastStream;
import net.sourceforge.fractal.consensus.paxos.PaxosStream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.multicast.MulticastMessage;
import net.sourceforge.fractal.utils.CollectionUtils;
/**   
* @author P. Sutra
* 
*/ 

public class FTWanAMCastStream extends Stream implements Runnable, Learner{

	private int mySWid;
	private Group myGroup;
	private TreeSet<String> allGroups;

	private boolean terminate;
	private PaxosStream paxos;
	private MulticastStream rmcast;
	private BroadcastStream rbcast;

	private int K, round, roundToEnd, propK;

	private int nbLocalConsensusPerRound, startRoundFrequency;

	private TreeMap<Integer,HashMap<String, ArrayList<FTWanAMCastIntraGroupMessage>>> globalMsgs; 

	// A variable to simulate gpaxos
	private TreeMap<Integer,HashMap<String, Integer>> gPaxosAck;

	private List<FTWanAMCastIntraGroupMessage> consensusDelivered;
	private List<FTWanAMCastIntraGroupMessage> aDelivered;

	private BlockingQueue<FTWanAMCastInterGroupMessage> interGroupChannel;
	private BlockingQueue<FTWanAMCastIntraGroupMessage> intraGroupChannel;
	private BlockingQueue<Serializable> toDeliver; 
	private List<FTWanAMCastIntraGroupMessage> globaltoSend;

	private Thread intraGroupThread, interGroupThread;

	private Semaphore waitCondition;

	// local variables now global
	private ArrayList<FTWanAMCastIntraGroupMessage> decision;
	private HashSet<FTWanAMCastIntraGroupMessage> toRemove2;
	
	//quorumSize for ACK messages
	private int quorumSize;

	public FTWanAMCastStream(Integer id, String myGroup, Collection<String> groups,
			String streamName, String rbcastStreamName, String rmcastStreamName,
			String consensusStreamName, int nbLocalConsensusPerRound,
			int startRoundFrequency){

		this.mySWid = id;
		this.myGroup = FractalManager.getInstance().membership.group(myGroup);
		this.paxos = FractalManager.getInstance().paxos.stream(consensusStreamName);
		this.rmcast = FractalManager.getInstance().multicast.stream(rmcastStreamName);
		this.rbcast = FractalManager.getInstance().broadcast.stream(rbcastStreamName);

		this.allGroups = new TreeSet<String>();
		for(String s : groups){
			this.allGroups.add(s);
		}

		this.terminate = false;

		this.consensusDelivered = new ArrayList<FTWanAMCastIntraGroupMessage>();
		this.aDelivered = new ArrayList<FTWanAMCastIntraGroupMessage>();

		this.globalMsgs =  new TreeMap<Integer, HashMap<String,ArrayList<FTWanAMCastIntraGroupMessage>>>();

		this.gPaxosAck = new TreeMap<Integer, HashMap<String,Integer>>();

		this.toDeliver = CollectionUtils.newBlockingQueue();
		this.interGroupChannel = CollectionUtils.newBlockingQueue();		
		this.intraGroupChannel = CollectionUtils.newBlockingQueue();		
		this.globaltoSend = new ArrayList<FTWanAMCastIntraGroupMessage>();

		this.rbcast.registerLearner("FTWanAMCastIntraGroupMessage",this);
		this.rmcast.registerLearner("FTWanAMCastInterGroupMessage",this);
		this.rmcast.registerLearner("FTWanAMCastInterGroupAck",this);

		this.interGroupThread = new Thread(this,"FTWanAMCast:ProposerThread@"+this.mySWid);
		this.intraGroupThread = new Thread(this,"FTWanAMCast:IntraGroupMessagesAggregatorThread@"+this.mySWid);

		this.waitCondition = new Semaphore(1);

		// Pseudo-code variables

		this.K = 1;
		this.round = 1;
		this.roundToEnd = 1;
		this.propK = 1;

		this.nbLocalConsensusPerRound = nbLocalConsensusPerRound;
		this.startRoundFrequency = startRoundFrequency;

		// Local variables now global
		this.toRemove2 = new HashSet<FTWanAMCastIntraGroupMessage>();
		
		quorumSize  = (int) Math.ceil((2.0 * ((double) allGroups.size()) + 1.0) / 3.0);		
	}

	public void deliver(Serializable s) {
		Message m = (Message) s;
		
		if( learners.get(m.getMessageType())!=null
			&&
			learners.get(m.getMessageType()).size()>0){
			for(Learner l : learners.get(m.getMessageType())){
				if(ConstantPool.FTWanAMCast_DL > 4)
					System.out.println(this+" I deliver "+m);
				l.learn(this,(m));
			}
		}else{
			if(ConstantPool.FTWanAMCast_DL > 0)
				System.out.println(this+" got a "+ m.getMessageType() +" to nobody");
		}
	}
	
	public void start(){
		rmcast.start();
		if(interGroupThread.getState()==Thread.State.NEW){
			interGroupThread.start();
			intraGroupThread.start();
		}
	}

	public void stop(){
		rmcast.stop();
		terminate=true;
	}

	@SuppressWarnings("unchecked")
	public void run(){

		if( Thread.currentThread().getName().equals("FTWanAMCast:ProposerThread@"+this.mySWid)){

			ArrayList<FTWanAMCastIntraGroupMessage> proposal;
			ArrayList<FTWanAMCastIntraGroupMessage> bundle;

			while(!terminate){

				proposal = new ArrayList<FTWanAMCastIntraGroupMessage>();

				while(!checkCondition(proposal)){
					waitCondition.acquireUninterruptibly();
				}

				if(myGroup.isLeading(mySWid)){
					if(ConstantPool.FTWanAMCast_DL > 4)
						System.out.println("FTWanAMCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) I propose in instance "+propK);
					paxos.propose(proposal,propK);
				}

				propK++;

				try {
					decision = (ArrayList<FTWanAMCastIntraGroupMessage>)paxos.decide(K);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				if(ConstantPool.FTWanAMCast_DL > 4)
					System.out.println("FTWanAMCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) Decision in instance "+ K);

				synchronized(this){
					for(FTWanAMCastIntraGroupMessage m : decision){
						if(! consensusDelivered.contains(m) && ! aDelivered.contains(m)){
							if(m.dest.size()==1 && m.dest.contains(myGroup.name())){
								deliverMessage(m);
							}else{
								globaltoSend.add(m);
								consensusDelivered.add(m);
							}
						}
					}
				}
				synchronized(this){

					if( K == (round * startRoundFrequency) ){ 

						bundle = new ArrayList<FTWanAMCastIntraGroupMessage>();
						bundle.addAll(globaltoSend);
						globaltoSend.clear();

						if(myGroup.isLeading(mySWid)){
							FTWanAMCastInterGroupMessage p = new FTWanAMCastInterGroupMessage(bundle, new HashSet<String>(allGroups), myGroup.name(), mySWid, round);
							rmcast.multicast(p);
							if(ConstantPool.FTWanAMCast_DL > 4)
								System.out.println("FTWanAMCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) I sent "+p);
						}

						round++;
					}

					if( K == roundToEnd * startRoundFrequency + nbLocalConsensusPerRound )
							;
					K++;
				}

			}

		} else { // InterGroupMessagesAggregator

			HashSet<FTWanAMCastInterGroupMessage> q = new HashSet<FTWanAMCastInterGroupMessage>();

			try {

				while(!terminate){

					q.clear();
					q.add((FTWanAMCastInterGroupMessage)interGroupChannel.take());
					interGroupChannel.drainTo(q);

					for(FTWanAMCastInterGroupMessage p : q){

						if(ConstantPool.FTWanAMCast_DL > 4)
							System.out.println("FTWanAMCast ( "+ mySWid +" , "+System.currentTimeMillis()+"  ) I received "+ p);			

						synchronized(this){
							if(!globalMsgs.containsKey(p.round))
								globalMsgs.put(p.round,new HashMap<String, ArrayList<FTWanAMCastIntraGroupMessage>>());
							globalMsgs.get(p.round).put(p.gSource,(ArrayList<FTWanAMCastIntraGroupMessage>)p.serializable);
							if(p.round==roundToEnd && K > roundToEnd * startRoundFrequency + nbLocalConsensusPerRound)
								checkEndOfRoundToEnd();
						}
					}

				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void learn(Stream s, Serializable value) {

		if(value instanceof FTWanAMCastIntraGroupMessage ){
			FTWanAMCastIntraGroupMessage m = (FTWanAMCastIntraGroupMessage)value;

			try {

				intraGroupChannel.put(m);

				if(ConstantPool.FTWanAMCast_DL > 4)
					System.out.println("FTWanAMCast ( "+ mySWid +" , "+System.currentTimeMillis()+ " ) I send "+m+" to consensus");

			} catch (InterruptedException e) {
				e.printStackTrace();
			}			
	
		}else{
			reliableRMCastDeliver((MulticastMessage)value);
		}

	}

	public void reliableRMCastDeliver(MulticastMessage msg) {

		if(msg instanceof FTWanAMCastInterGroupAck){			

			FTWanAMCastInterGroupAck m = (FTWanAMCastInterGroupAck) msg;

			synchronized (this) {

				if( ! gPaxosAck.containsKey(m.round) ){
					gPaxosAck.put(m.round, new HashMap<String,Integer>());
					for(String aGroup : allGroups){
						gPaxosAck.get(m.round).put(aGroup, 0);
					}
				}

				gPaxosAck.get(m.round).put(
						m.gAcked,
						gPaxosAck.get(m.round).get(m.gAcked)+1);

				if(ConstantPool.FTWanAMCast_DL > 4)
					System.out.println("FTWanAMCast ( "+ mySWid +" , "+System.currentTimeMillis()+ " ) " +
							"Ack from "+m.gSource+" for "+m);

				if(K > roundToEnd * startRoundFrequency + nbLocalConsensusPerRound)
					checkEndOfRoundToEnd();
			}

		}else{

			FTWanAMCastInterGroupMessage m = (FTWanAMCastInterGroupMessage) msg;

			// send ack to simulate fast gpaxos rounds:
			if(myGroup.isLeading(mySWid)){
				FTWanAMCastInterGroupAck ack = new FTWanAMCastInterGroupAck(
						new HashSet<String>(allGroups),
						myGroup.name(),
						mySWid,
						m.round,
						new String(m.gSource));
				rmcast.multicast(ack);
			}

			try {
				interGroupChannel.put((FTWanAMCastInterGroupMessage)msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

	//
	// INTERFACES
	//

	public void atomicBroadcast(Serializable s){
		FTWanAMCastIntraGroupMessage m = new FTWanAMCastIntraGroupMessage(s,new HashSet<String>(allGroups),mySWid); 
		if(ConstantPool.FTWanAMCast_DL > 4)
			System.out.println("FTWanAMCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) I AB-Cast"+ m+" to all");
		rbcast.broadcast(m);
	}

	// Non-Genuime Atomic Multicast
	public void atomicMulticastNG(Serializable s, HashSet<String> dest){
		FTWanAMCastIntraGroupMessage m = new FTWanAMCastIntraGroupMessage(s,dest,mySWid);
		if(ConstantPool.FTWanAMCast_DL > 4)
			System.out.println("FTWanAMCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) I ngAMCast"+ m+" to "+dest);
		rbcast.broadcast(m);
	}

	public Serializable atomicDeliver(){
		Serializable result=null;
		try {
			result = toDeliver.take();
		} catch (InterruptedException e) {
			return result;
		}
		return result;
	}

	//
	// INTERNALS
	//

	private boolean checkCondition(ArrayList<FTWanAMCastIntraGroupMessage> proposal){

		proposal.clear();

		synchronized(this){
			if ( K >  roundToEnd * startRoundFrequency + nbLocalConsensusPerRound )
				return false;
		}

		intraGroupChannel.drainTo(proposal);

		return true;

	}

	private void checkEndOfRoundToEnd(){

		if(ConstantPool.FTWanAMCast_DL > 4){
			System.out.println("FTWanAMCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) Trying to finish round "+roundToEnd);
		}

		if(!gPaxosAck.containsKey(roundToEnd) || !globalMsgs.containsKey(roundToEnd) )
			return;

		for(String aGroup : allGroups){
			if(gPaxosAck.get(roundToEnd).containsKey(aGroup)) {
				if(gPaxosAck.get(roundToEnd).get(aGroup) < quorumSize )
					return;
			}
		}

		if(globalMsgs.get(roundToEnd).size() == allGroups.size()){

			if(ConstantPool.FTWanAMCast_DL > 4){
				System.out.println("FTWanAMCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) End of ground "+roundToEnd);
			}

			for(String g: allGroups){
				for(FTWanAMCastIntraGroupMessage m : globalMsgs.get(roundToEnd).get(g)){
					if(m.dest.contains(myGroup.name())){
						deliverMessage(m);
					}
				}
			}

			globalMsgs.remove(roundToEnd);

			roundToEnd ++;

			// gPaxos simulation
			if(!gPaxosAck.containsKey(roundToEnd) ){
				gPaxosAck.put(roundToEnd, new HashMap<String,Integer>());
				for(String aGroup : allGroups){
					gPaxosAck.get(roundToEnd).put(aGroup, 0);
				}
			}

			waitCondition.release(1);

		}

	}

	private void deliverMessage(FTWanAMCastIntraGroupMessage m){

		toDeliver.add(m.serializable);
		deliver(m);

		if(ConstantPool.FTWanAMCast_DL > 0)
			System.out.println("FTWanAMCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) I deliver" + m);

		toRemove2.clear();

		for(FTWanAMCastIntraGroupMessage n : aDelivered){
			if(n.source == m.source && n.compareTo(m) < 0)
				toRemove2.add(n);
			consensusDelivered.remove(m);
		}
		aDelivered.add(m);

		for(FTWanAMCastIntraGroupMessage n : toRemove2){
			aDelivered.remove(n);
			n=null;
		}

	}
}
