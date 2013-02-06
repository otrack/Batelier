package net.sourceforge.fractal.abcast;

/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.
This library is free software; you can redistribute it and/or modify it under     
the terms of the GNU Lesser General Public License as published by the Free Software 	
Foundation; either version 2.1 of the License, or (at your option) any later version.      
This library is distributed in the hope that it will be useful, but WITHOUT      
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      
PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      
You should have received a copy of the GNU Lesser General Public License along      
with this library; if not, write to the      
Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.broadcast.BroadcastStream;
import net.sourceforge.fractal.consensus.paxos.PaxosStream;
import net.sourceforge.fractal.replication.Command;
import net.sourceforge.fractal.utils.CollectionUtils;


/**
 * 
 * TODO bound memory
 * 
 * @author Lasaro Camargos
 * @author Pierre Sutra
 *
 */

public final class ABCastStream extends Stream implements Learner, Runnable{

	private Integer mySWid; 
	private String streamName;

	private PaxosStream consensusStream;
	private BroadcastStream rbcastStream;

	private int nextConsensusInstance = 1;
	private static final int WINDOW = 0; // WINDOW+1 is the number of instances of Consensus we run in parallel

	private int nextUndecidedConsensusInstance = 1;

	private Thread proposer;
	private Thread learner;

	private boolean isTerminated;

	private BlockingQueue<ABCastMessage>toSend;
	private Set<ABCastMessage> delivered;
	private Set<ABCastMessage> toPropose; 

	public ABCastStream(String name,
			Integer swid,
			String consensusStreamName,
			String rbcastStreamName) {

		mySWid = swid;
		streamName = name;

		consensusStream = FractalManager.getInstance().paxos.stream(consensusStreamName);
		rbcastStream = FractalManager.getInstance().broadcast.stream(rbcastStreamName);
		rbcastStream.registerLearner("ABCastMessage", this);
		
		toSend = CollectionUtils.newBlockingQueue();
		delivered = new HashSet<ABCastMessage>();
		toPropose = new HashSet<ABCastMessage>();

		isTerminated = false;
		proposer = new Thread(this,"ABCastStream:Proposer@"+mySWid);
		learner = new Thread(this,"ABCastStream:Learner@"+mySWid);

	}

	@Override
	public void start(){
		rbcastStream.start();
		if(proposer.getState()==Thread.State.NEW)
			proposer.start();
		if(learner.getState()==Thread.State.NEW)
			learner.start();
	}

	@SuppressWarnings("deprecation")
	@Override
	public void stop(){
		rbcastStream.unregisterLearner("ABCastMessage", this);
		isTerminated = true; 
		proposer.interrupt();
		learner.interrupt();
	}

	public void run() {

		if(Thread.currentThread().getName().equals("ABCastStream:Proposer@"+mySWid)){

			ArrayList<ABCastMessage> received = new ArrayList<ABCastMessage>(); 
			ArrayList<ABCastMessage> bundle;
			ABCastMessage m;

			try {

				while(!isTerminated){

					received.clear();
					m=toSend.poll(1,  TimeUnit.MILLISECONDS);
					if(m!=null){	
						received.add(m);
						toSend.drainTo(received);
						for(ABCastMessage n : received){
							toPropose.add(n);
						}
					}
					
					synchronized(delivered){
						toPropose.removeAll(delivered);
					}

					if(!toPropose.isEmpty()){
						bundle = new ArrayList<ABCastMessage>();
						bundle.addAll(toPropose);
						if(nextConsensusInstance<nextUndecidedConsensusInstance)
							nextConsensusInstance=nextUndecidedConsensusInstance;
						consensusStream.propose(bundle , nextConsensusInstance);
						if(ConstantPool.ABCAST_DL > 4)
							System.out.println(this+" I propose to Consensus'instance "+nextConsensusInstance);
						if( nextConsensusInstance < nextUndecidedConsensusInstance+WINDOW )
							nextConsensusInstance++;
					}
					
				}

			} catch (Exception e) {
				if(!isTerminated)
					e.printStackTrace();
			}

		}else{ // Learner

			List<ABCastMessage> decision;
			List<ABCastMessage> toDeliver = new ArrayList<ABCastMessage>();

			try {

				while(!isTerminated){

					toDeliver.clear();

					decision = (List<ABCastMessage>) consensusStream.decide(nextUndecidedConsensusInstance);

					if(ConstantPool.ABCAST_DL > 4)
						System.out.println(this+" decision in Consensus'instance "+nextUndecidedConsensusInstance);

					nextUndecidedConsensusInstance++;

					synchronized(delivered){
						for(ABCastMessage m : decision){
							if(!delivered.contains(m)){
								delivered.add(m);
								toDeliver.add(m);
							}
						}
					}

					for(ABCastMessage m : toDeliver)
						deliver(m);

				}

			} catch (InterruptedException e) {
				if(!isTerminated)
					e.printStackTrace();
			}

		}

	}

	public void atomicBroadcast(Serializable s){
		rbcastStream.broadcast(new ABCastMessage(s, mySWid));
	}

	public void propose(Command c){
		atomicBroadcast(c);
	}

	public void learn(Stream s, Serializable value) {
		if(ConstantPool.ABCAST_DL > 4)
			System.out.println(this+" receiving a message by rbcast "+value);
		toSend.add((ABCastMessage) value);
	}

	@Override
	public void deliver(Serializable value) {	
		ABCastMessage m = (ABCastMessage)value;
		Serializable s=m.serializable;
		if( learners.containsKey(s.getClass().getName()) && !learners.get(s.getClass().getName()).isEmpty() ){
			for(Learner l : learners.get(s.getClass().getName())){
				l.learn(this,s);
			}
		}else{
			System.out.println(this+" got a "+ s.getClass().getName() +" to nobody");
		}
	}

	@Override
	public String toString(){
		return "ABCastStream:"+mySWid+":"+streamName;
	}

}
