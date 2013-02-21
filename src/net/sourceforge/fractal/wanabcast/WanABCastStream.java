/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project. 
This library is free software; you can redistribute it and/or modify it under     
the terms of the GNU Lesser General Public License as published by the Free Software 	
Foundation; either version 2.1 of the License, or (at your option) any later version.      
This library is distributed in the hope that it will be useful, but WITHOUT      
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      
PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      
You should have received a copy of the GNU Lesser General Public License along      
with this library; if nlatencyot, write to the      
Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
 */


package net.sourceforge.fractal.wanabcast;


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
 * 
 * FIXME This algorithm requires a perfect leader.
 * 
 * @author Pierre Sutra
 *
 **/
public class WanABCastStream extends Stream implements Runnable, Learner{ 

	private int mySWid;
	private Group myGroup;
	private TreeSet<String> allGroups;
	private HashSet <String> rmcastDests;

	private boolean terminate;
	private PaxosStream paxos;
	private MulticastStream rmcast;
	private BroadcastStream rbcast;

	private int K, round, roundToEnd, propK;

	private int nbLocalConsensusPerRound, startRoundFrequency;

	private TreeMap<Integer,HashMap<String, ArrayList<WanABCastIntraGroupMessage>>> globalMsgs; 
	private List<WanABCastIntraGroupMessage> consensusDelivered;
	private List<WanABCastIntraGroupMessage> aDelivered;

	private BlockingQueue<WanABCastInterGroupMessage> interGroupChannel;
	private BlockingQueue<WanABCastIntraGroupMessage> intraGroupChannel;
	private BlockingQueue<Serializable> toDeliver; 
	private List<WanABCastIntraGroupMessage> globaltoSend;

	private Thread intraGroupThread, interGroupThread;

	private Semaphore waitCondition;

	// local variables now global
	private ArrayList<WanABCastIntraGroupMessage> decision;
	private HashSet<WanABCastIntraGroupMessage> toRemove2;

	public WanABCastStream(int id, String myGroup, Collection<String> allGroups, String name, 
			String rbcastStreamName, String rmcastStreamName, String consensusStreamName, 
			int nbLocalConsensusPerRound,int startRoundFrequency){

		if(startRoundFrequency<=0 || nbLocalConsensusPerRound<=0)
			throw new IllegalArgumentException("Invalid parameters, startRoundFrequency and nbLocalConsensusPerRound should be higher than 0; nbLocalConsensusPerRound= "
					+nbLocalConsensusPerRound+", startRoundFrequency="+startRoundFrequency );
		
		this.mySWid = id;
		this.myGroup = FractalManager.getInstance().membership.group(myGroup);
		this.paxos = FractalManager.getInstance().paxos.stream(consensusStreamName);
		this.rmcast = FractalManager.getInstance().multicast.stream(rmcastStreamName);
		this.rbcast = FractalManager.getInstance().broadcast.stream(rbcastStreamName);

		this.allGroups = new TreeSet<String>();
		this.rmcastDests = new HashSet<String>();
		for(String s : allGroups){
			if(!s.equals(this.myGroup.name()))this.rmcastDests.add(s);
			this.allGroups.add(s);
		}
		this.terminate = false;

		this.consensusDelivered = new ArrayList<WanABCastIntraGroupMessage>();
		this.aDelivered = new ArrayList<WanABCastIntraGroupMessage>();

		this.globalMsgs =  new TreeMap<Integer, HashMap<String,ArrayList<WanABCastIntraGroupMessage>>>();

		this.toDeliver = CollectionUtils.newBlockingQueue();
		this.interGroupChannel = CollectionUtils.newBlockingQueue();		
		this.intraGroupChannel = CollectionUtils.newBlockingQueue();		
		this.globaltoSend = new ArrayList<WanABCastIntraGroupMessage>();

		this.rbcast.registerLearner("WanABCastIntraGroupMessage",this);
		this.rmcast.registerLearner("WanABCastInterGroupMessage",this);

		this.interGroupThread = new Thread(this,"WanABCast:ProposerThread@"+this.mySWid);
		this.intraGroupThread = new Thread(this,"WanABCast:IntraGroupMessagesAggregatorThread@"+this.mySWid);

		this.waitCondition = new Semaphore(1);

		// Pseudo-code variables

		this.K = 1;
		this.round = 1;
		this.roundToEnd = 1;
		this.propK = 1;

		this.nbLocalConsensusPerRound = nbLocalConsensusPerRound;
		this.startRoundFrequency = startRoundFrequency;

		if(ConstantPool.WANABCAST_DL>0) 
			System.out.println(this+"  Starting with the startRoundFrequency:"+startRoundFrequency+"; nbLocalConsensusPerRound:"+nbLocalConsensusPerRound);
			
		// Local variables now global
		this.toRemove2 = new HashSet<WanABCastIntraGroupMessage>();
	}

	public void deliver(Serializable s) {
		Message m = (Message) s;
		
		if( learners.get(m.getMessageType())!=null
			&&
			learners.get(m.getMessageType()).size()>0){
			for(Learner l : learners.get(m.getMessageType())){
				if(ConstantPool.WANABCAST_DL > 4)
					System.out.println(this+" I deliver "+m);
				l.learn(this,(m));
			}
		}else{
			if(ConstantPool.WANABCAST_DL > 0)
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

		if( Thread.currentThread().getName().equals("WanABCast:ProposerThread@"+this.mySWid)){

			ArrayList<WanABCastIntraGroupMessage> proposal;
			ArrayList<WanABCastIntraGroupMessage> bundle;

			while(!terminate){

				proposal = new ArrayList<WanABCastIntraGroupMessage>();

				while(!checkCondition(proposal)){
					waitCondition.acquireUninterruptibly();
				}
				
				if(myGroup.isLeading(mySWid)){
					if(ConstantPool.WANABCAST_DL > 4)
						System.out.println("WanABCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) I propose in instance "+propK);
					paxos.propose(proposal,propK);
				}

				propK++;

				try {
					decision = (ArrayList<WanABCastIntraGroupMessage>)paxos.decide(K);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				if(ConstantPool.WANABCAST_DL > 4)
					System.out.println("WanABCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) Decision in instance "+ K);

				synchronized(this){
					for(WanABCastIntraGroupMessage m : decision){
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

				if( K == (round * startRoundFrequency) ){ 

					bundle = new ArrayList<WanABCastIntraGroupMessage>();
					bundle.addAll(globaltoSend);
					globaltoSend.clear();

					synchronized(this){
						if(!globalMsgs.containsKey(round)){
							globalMsgs.put(round,new HashMap<String, ArrayList<WanABCastIntraGroupMessage>>());
						}
						globalMsgs.get(round).put(myGroup.name(),bundle);
					}

					if(myGroup.isLeading(mySWid)&&rmcastDests.size()>0){
						WanABCastInterGroupMessage p = new WanABCastInterGroupMessage(bundle, rmcastDests, myGroup.name(), mySWid, round);
						if(ConstantPool.WANABCAST_DL > 4)
							System.out.println("WanABCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) I send "+p+" (instance "+K+") to distant groups "+p.getDest());
						rmcast.multicast(p);
					}

					round++;
				}

				synchronized(this){
					if( K == roundToEnd * startRoundFrequency + nbLocalConsensusPerRound )
						checkEndOfRoundToEnd();
					K++;
				}

			}

		} else { // InterGroupMessagesAggregator

			HashSet<WanABCastInterGroupMessage> q = new HashSet<WanABCastInterGroupMessage>();

			try {

				while(!terminate){

					q.clear();
					q.add((WanABCastInterGroupMessage)interGroupChannel.take());
					interGroupChannel.drainTo(q);

					for(WanABCastInterGroupMessage p : q){

						if(ConstantPool.WANABCAST_DL > 4)
							System.out.println("WanABCast ( "+ mySWid +" , "+System.currentTimeMillis()+"  ) I received "+ p);			

						synchronized(this){
							if(!globalMsgs.containsKey(p.round))
								globalMsgs.put(p.round,new HashMap<String, ArrayList<WanABCastIntraGroupMessage>>());
							globalMsgs.get(p.round).put(p.getGSource(),(ArrayList<WanABCastIntraGroupMessage>)p.serializable);
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
		
		if(value instanceof WanABCastIntraGroupMessage){
		
			WanABCastIntraGroupMessage m = (WanABCastIntraGroupMessage)value;

			try {

				intraGroupChannel.put(m);

				if(ConstantPool.WANABCAST_DL > 3)
					System.out.println("WanABCast ( "+ mySWid +" , "+System.currentTimeMillis()+ " ) I send "+m+" to consensus");

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}else{
			
			MulticastMessage msg = (MulticastMessage) value;
			if(ConstantPool.WANABCAST_DL > 3)
				System.out.println("WanABCast ( "+ mySWid +" I receive an inter-group message "+msg);
			
			try {
				interGroupChannel.put((WanABCastInterGroupMessage)msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}

	}

	//
	// INTERFACES
	//

	public void atomicBroadcast(Serializable s){
		WanABCastIntraGroupMessage m = new WanABCastIntraGroupMessage(s,allGroups,mySWid); 
		if(ConstantPool.WANABCAST_DL > 3)
			System.out.println("WanABCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) I AB-Cast"+ m+" to all");
		rbcast.broadcast(m);
	}

	// Non-Genuime Atomic Multicast
	public void atomicMulticastNG(Serializable s, HashSet<String> dest){
		WanABCastIntraGroupMessage m = new WanABCastIntraGroupMessage(s,dest,mySWid);
		if(ConstantPool.WANABCAST_DL > 3)
			System.out.println("WanABCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) I ngAMCast"+ m+" to "+dest);
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

	private boolean checkCondition(ArrayList<WanABCastIntraGroupMessage> proposal){

		proposal.clear();

		synchronized(this){
			if ( K >  roundToEnd * startRoundFrequency + nbLocalConsensusPerRound )
				return false;
		}

		intraGroupChannel.drainTo(proposal);

		return true;

	}

	private void checkEndOfRoundToEnd(){

		assert globalMsgs.containsKey(roundToEnd) : globalMsgs + " => "+ roundToEnd;
		
		if(globalMsgs.get(roundToEnd).size() == allGroups.size()){

			if(ConstantPool.WANABCAST_DL > 4){
				System.out.println("WanABCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) End of round "+roundToEnd);
			}

			for(String g: allGroups){
				for(WanABCastIntraGroupMessage m : globalMsgs.get(roundToEnd).get(g)){
					if(m.dest.contains(myGroup.name())){
						deliverMessage(m);
					}
				}
			}

			globalMsgs.remove(roundToEnd);

			roundToEnd ++;

			waitCondition.release(1);

		}

	}

	private void deliverMessage(WanABCastIntraGroupMessage m){

		toDeliver.add(m.serializable);
		deliver(m);
		
		if(ConstantPool.WANABCAST_DL > 4)
			System.out.println("WanABCast ( "+ mySWid +" , "+System.currentTimeMillis()+" ) I deliver" + m);

		toRemove2.clear();

		for(WanABCastIntraGroupMessage n : aDelivered){
			if(n.source == m.source && n.compareTo(m) < 0)
				toRemove2.add(n);
			consensusDelivered.remove(m);
		}
		aDelivered.add(m);

		for(WanABCastIntraGroupMessage n : toRemove2){
			aDelivered.remove(n);
			n=null;
		}

	}

}