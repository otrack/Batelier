/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
 */


package net.sourceforge.fractal.wanamcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.consensus.LongLivedConsensus;
import net.sourceforge.fractal.consensus.primary.PrimaryBasedLongLivedConsensus;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.ExecutorPool;
import net.sourceforge.fractal.utils.PerformanceProbe.FloatValueRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

/**   
* @author P. Sutra
* 
*/

public class WanAMCastStream extends Stream{

	private ValueRecorder aDeliveredSize, stagesSize;
	private FloatValueRecorder ratioLocalMsgs;
	private TimeRecorder consensusLatency, coreLoopLatency, sideLoopLatency;
	private Map<WanAMCastMessage,Long> convoyEffectTracker;
	private FloatValueRecorder convoyEffect;
	private ValueRecorder checksum;
	private FloatValueRecorder latency;
	
	private int deliveredCnt = 0, localdeliveredCnt = 0;
	@SuppressWarnings("unused")
	private String myName;
	int mySWid;
	private Group myGroup;
	private boolean terminate;
	
	private MulticastStream multicastStream;
	private LongLivedConsensus<List<WanAMCastMessage>> consensus;
	private TimestampingTask tstask;
	private MessageReceiverTask mrtask;
	
	private int K;
	private BlockingQueue<WanAMCastMessage> intraGroupChannel;
	private Collection<WanAMCastMessage> aDelivered; // Does not contain msg with dest={myGroup.name()}
	private Map<WanAMCastMessage, HashMap<String, Integer>> stage1;

	// PENDING:
	private Map<WanAMCastMessage, Integer> stages; // keep tracks of the current stage
	private TreeMap<Timestamp, WanAMCastMessage> ts2msg; 
	private HashMap<WanAMCastMessage, Timestamp> msg2ts; 
	
	// local Variables going to global so as to enhance the code
	private List<String> toMyGroup;
	
	// local variable indicating if the primitive provides acyclicity or not.
	private boolean acycl;

	public WanAMCastStream(int id, Group g, String streamName, MulticastStream multicast, boolean acycl){
		this.mySWid = id;
		this.myGroup = g;
		this.myName = streamName;
		this.terminate = false;
		this.multicastStream = multicast;
		this.consensus = new PrimaryBasedLongLivedConsensus(myGroup);

		this.acycl = acycl;
		
		this.intraGroupChannel =  CollectionUtils.newBlockingQueue();
		
		this.aDelivered = CollectionUtils.newBoundedSet(50000);
//		aDelivered = CollectionUtils.newBoundedSet(
//				new Comparator<WanAMCastMessage>() {
//					private final ClassCastException ex = new ClassCastException();
//					public int compare(WanAMCastMessage m, WanAMCastMessage n){
//						if(m.source!=n.source) throw ex;
//						return m.compareTo(n);
//					}
//				});
				
		this.stages = new HashMap<WanAMCastMessage, Integer>();
		this.stage1 = new HashMap<WanAMCastMessage, HashMap<String,Integer>>();

		this.ts2msg = new TreeMap<Timestamp,WanAMCastMessage>();
		this.msg2ts = new HashMap<WanAMCastMessage,Timestamp>();

		this.K=1;

		// helpers
		toMyGroup = new ArrayList<String>();
		toMyGroup.add(g.name());
		
		// probes
		if(ConstantPool.WANAMCAST_DL>0){
			aDeliveredSize = new ValueRecorder(this+"#aDeliveredSize");
			aDeliveredSize.setFormat("%M");
			stagesSize = new ValueRecorder(this+"#stagesSize");
			stagesSize.setFormat("%M");
			consensusLatency = new TimeRecorder(this+"#consensusLatency(ms)");
			consensusLatency.setFormat("%a");
			coreLoopLatency =  new TimeRecorder(this+"#(max)coreLoopLatency(ms)");
			coreLoopLatency.setFormat("%M");
			sideLoopLatency =  new TimeRecorder(this+"#(max)sideLoopLatency(ms)");
			sideLoopLatency.setFormat("%M");
			checksum = new ValueRecorder(this+"#checksum");
			checksum.setFormat("%t");
			convoyEffectTracker = new HashMap<WanAMCastMessage, Long>();
			convoyEffect = new FloatValueRecorder(this+"#convoyEffect(ms)");
			convoyEffect.setFormat("%a");
			latency = new FloatValueRecorder(this+"#latency(ms)");
			latency.setFormat("%a");
			ratioLocalMsgs = new FloatValueRecorder(this+"ratioLocalMsgs");
			ratioLocalMsgs.setFormat("%a");
		}
		
	}

	
	/** 
	 * Atomic multicast a message to the set of destination groups.
	 * This primitive ensures both atomicity and causality.
	 * 
	 * @param m the message to send.
	 */
	public void atomicMulticast(WanAMCastMessage m){
		ArrayList<WanAMCastMessage> msgBox = new ArrayList<WanAMCastMessage>();
		msgBox.add(m);		
		if(ConstantPool.WANAMCAST_DL > 3)
			System.out.println(this+" I Wan Atomic Multicast "+m);
		multicastStream.multicast(
				new WanAMCastInterGroupMessage(
						msgBox,
						m.dest,
						myGroup.name(),
						mySWid)
				);
	}

	public void start(){
		tstask = new TimestampingTask();
		ExecutorPool.getInstance().submit(tstask);
		mrtask = new MessageReceiverTask();
		multicastStream.registerLearner("WanAMCastInterGroupMessage",mrtask);
		multicastStream.start();
		consensus.start();
	}

	public void stop(){
		multicastStream.unregisterLearner("WanAMCastInterGroupMessage",mrtask);
		multicastStream.stop();
		consensus.stop();
		// FIXME stop tstask
	}

	@Override
	public boolean registerLearner(String msgType, Learner learner){
		if(ConstantPool.WANAMCAST_DL > 0){
			System.out.println(this+" register learner for "+msgType);
		}
		return super.registerLearner(msgType, learner);
	}
	
	@Override
	public void deliver(Serializable s) {
		
		WanAMCastMessage m = (WanAMCastMessage) s;
		
		if( learners.get(m.getMessageType())!=null
			&&
			learners.get(m.getMessageType()).size()>0){
			for(Learner l : learners.get(m.getMessageType())){
				if(ConstantPool.WANAMCAST_DL > 0){
					deliveredCnt++;
					if(m.dest.size()==1) localdeliveredCnt++;
					checksum.add(m.getUniqueId().hashCode()%deliveredCnt);
					ratioLocalMsgs.add((double)localdeliveredCnt/(double)deliveredCnt);
				}
				if(ConstantPool.WANAMCAST_DL > 4)
					System.out.println(this+" I AM-deliver "+m);
				l.learn(this,(m));
			}
		
		}else{
			if(ConstantPool.WANAMCAST_DL > 0)
				System.out.println(this+" got a "+ m.getMessageType() +" to nobody");
		}
	
		aDelivered.add(m);
		intraGroupChannel.remove(m);
		stages.remove(m);
		stage1.remove(m); 
		if(msg2ts.containsKey(m)){
			ts2msg.remove(msg2ts.get(m));
			msg2ts.remove(m);
		}
			
		// Performance tracking
		if(ConstantPool.WANAMCAST_DL>0){ 
			aDeliveredSize.add(aDelivered.size());
			if(convoyEffectTracker.containsKey(m)){
				convoyEffect.add(System.currentTimeMillis()-convoyEffectTracker.get(m));
				convoyEffectTracker.remove(m);
			}else{
				convoyEffect.add(0); // to get an average				
			}
			latency.add(System.currentTimeMillis()-m.start);
		}
		
	}

	private String getString(){
		return "WanAMCast@"
				+ mySWid
				+ (ConstantPool.WANAMCAST_DL>10 ?
						("("+System.currentTimeMillis()+")") : "" );
	}
	
	public String toString(){
		return getString();
	}

	public boolean isClean(){
		return stages.isEmpty() && msg2ts.isEmpty() && ts2msg.isEmpty() && intraGroupChannel.isEmpty();
	}	
	
	//
	// Internals
	//
	
	private class TimestampingTask implements Runnable{

		@Override
		public void run() {
			
			List<WanAMCastMessage> received, proposed,decided;
			received = new ArrayList<WanAMCastMessage>();

			Integer maxClock;
			List<String>dest;
			boolean needToDeliver;
			HashMap<String, ArrayList<WanAMCastMessage>> toSend 
				= new HashMap<String, ArrayList<WanAMCastMessage>>();
			
			try {
				
				while(!terminate){

					// FIXME not fault-tolerant
					
					// 1 - Grab freshly received messages
					received.clear();
					if(myGroup.iLead()) received.add(intraGroupChannel.take());
					intraGroupChannel.drainTo(received);

					if(ConstantPool.WANAMCAST_DL > 6)
						System.out.println(this+ " I start round "+ K);
					
					// 2 - Compute message to propose to consensus
					proposed = new ArrayList<WanAMCastMessage>();

					coreLoopLatency.start();
					synchronized(aDelivered){
					
						for(WanAMCastMessage m : received){

							if( aDelivered.contains(m) ) {
								if(ConstantPool.WANAMCAST_DL > 3)
									System.out.println(this+" I kick "+m);
								continue;
							}
							
							assert stages.containsKey(m);
							assert !myGroup.iLead() || ( stages.get(m)==0 || stages.get(m) == 2);
							
							proposed.add(m);
							
						}

					}
					coreLoopLatency.stop();

					
					if(ConstantPool.WANAMCAST_DL > 6)
						System.out.println(this+ " Calling consensus in round "+ K);
					if(ConstantPool.WANAMCAST_DL > 0)
						consensusLatency.start();

					// 2 - Call the long-lived consensus object
					decided = consensus.propose(proposed);
					if(ConstantPool.WANAMCAST_DL > 9)
						System.out.println(this+ " I decided in round "+ K+" messages "+decided);
					if(ConstantPool.WANAMCAST_DL > 0)
						consensusLatency.stop();

					// 4 - Timestamp messages, stages advancements and timestamps propagation
					maxClock=K;
					needToDeliver = false;
					coreLoopLatency.start();
					toSend.clear();

					synchronized(aDelivered){

						for(WanAMCastMessage m : decided){

							if(ConstantPool.WANAMCAST_DL > 6)
								System.out.println(this+" Next message "+m);

							if( aDelivered.contains(m)) {
								if(ConstantPool.WANAMCAST_DL > 3)
									System.out.println(this+" I kick "+m);
								continue;
							}

							if(m.dest.size()==1 || ! acycl){ 
								assert m.dest.contains(myGroup.name()) && !msg2ts.containsKey(m);
								deliver(m);
								continue;
							}

							if( !stages.containsKey(m)){
								enterStageZero(m);
							}
							
							if(stages.get(m)==0){	
								enterStageOne(m);
								updateStageOne(m,myGroup.name(),K);
								updateLargestTimestamp(m);
								if(testEndStageOne(m)) 
									enterStageTwo(m);
								for(String g: m.dest){
									if(!g.equals(myGroup.name())){
										if( !toSend.keySet().contains(g)){
											toSend.put(g,new ArrayList<WanAMCastMessage>());
										}
										WanAMCastMessage m1 = (WanAMCastMessage)m.clone();
										m1.clock = K;
										m1.gSource = myGroup.name();
										toSend.get(g).add(m1);
									}
								}
								continue;
							}
							
							enterStageThree(m);
							needToDeliver = true;
							if(m.clock > maxClock) maxClock = m.clock;
							
						} // end for

						if(needToDeliver) 
							testDeliver();

					} // synchronized

					// FIXME
					if(myGroup.isLeading(mySWid)){
						for(String g : toSend.keySet()){
							if(toSend.get(g).size()>0){
								dest = new ArrayList<String>();
								dest.add(g);
								if(ConstantPool.WANAMCAST_DL > 3)
									System.out.println(this+" I RM-cast "+toSend.get(g)+" to "+ dest);
								multicastStream.multicast(new WanAMCastInterGroupMessage(toSend.get(g),dest,myGroup.name(),mySWid));
							}
						}
					}
					
					if(ConstantPool.WANAMCAST_DL > 6)
						System.out.println(this+" I set the clock to "+ (maxClock+1));

					K = maxClock+1 ;

					coreLoopLatency.stop();	
					
				}	
				
			}catch(Exception e){
				e.printStackTrace();
			}
			
		}
		
		public String toString(){
			return getString();
		}

	}
	
	private class MessageReceiverTask implements Learner{

		@Override
		public void learn(Stream s, Serializable value) {

			WanAMCastInterGroupMessage n = (WanAMCastInterGroupMessage)value;
			ArrayList<WanAMCastMessage> msgs = (ArrayList<WanAMCastMessage>)n.serializable;
			
			if(ConstantPool.WANAMCAST_DL > 3){
				System.out.println(this+" I RM-deliver "+ msgs );	
			}

			sideLoopLatency.start();

			synchronized(aDelivered){

 				for(WanAMCastMessage m : msgs){

					if( aDelivered.contains(m) ) {
						if(ConstantPool.WANAMCAST_DL > 3)
							System.out.println(this+" I kick "+m);
						continue;
					}

					if( !stages.containsKey(m)){
						enterStageZero(m);
					}

					if( m.clock!=-1 ){
						updateStageOne(m, m.gSource, m.clock);
						if(testEndStageOne(m)){
							enterStageTwo(m);
						}
					}

				} // end for

			}
			
			sideLoopLatency.stop();

		}

		public String toString(){
			return getString();
		}
		
	}

	//
	// Stages and delivery management
	//
	
	private void enterStageZero(WanAMCastMessage m){
		if(ConstantPool.WANAMCAST_DL > 0)
			stagesSize.add(stages.size());
		stages.put(m, 0);
		try {
			intraGroupChannel.put(m);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}
	
	/**
	 * @param m enters stage one.
	 */
	private void enterStageOne(WanAMCastMessage m){
		assert stages.get(m)==0;
		stages.put(m, 1);
	}
	
	private void updateStageOne(WanAMCastMessage m, String group, int ts){
		if(!stage1.containsKey(m)){
			stage1.put(m, new HashMap<String,Integer>());
		}
		stage1.get(m).put(group,ts);
	}
	
	/**
	 * @return true iff we have all timestamps for m and m is in stage 1.
	 * @param m
	 */
	private boolean testEndStageOne(WanAMCastMessage m){
		if(stages.get(m)!=1)
			return false;
		if(stage1.get(m).keySet().size()!=m.dest.size())
			return false;
		return true;
	}
	
	/**
	 * @param m enters stage two.
	 */
	private void enterStageTwo(WanAMCastMessage m){
		stages.put(m, 2);
		int maxGroupClock = 0;
		for(String g : stage1.get(m).keySet()){
			if(stage1.get(m).get(g) > maxGroupClock){
				maxGroupClock = stage1.get(m).get(g);
			}
		}
		m.clock = maxGroupClock;
		intraGroupChannel.offer(m);
	}
	
	private void enterStageThree(WanAMCastMessage m){
		if(ConstantPool.WANAMCAST_DL > 0)
			stagesSize.add(stages.size());
		stages.put(m, 3);
		updateLargestTimestamp(m);
	}
		
	private void testDeliver(){
		WanAMCastMessage m;
		
		if(ConstantPool.WANAMCAST_DL>0){
			stagesSize.add(stages.keySet().size());
			stagesSize.add(stage1.keySet().size());
			stagesSize.add(ts2msg.keySet().size());
			stagesSize.add(msg2ts.keySet().size());
		}

		if(ConstantPool.WANAMCAST_DL > 3)
			System.out.println(this+" Smallest ts ="+ts2msg.keySet().iterator().next()+" in round "+K);
					
		List<WanAMCastMessage> previous = new ArrayList<WanAMCastMessage>(ts2msg.size());
		List<WanAMCastMessage> toDeliver = new ArrayList<WanAMCastMessage>(ts2msg.size());
			
		for(Timestamp ts : ts2msg.keySet()){

			m = ts2msg.get(ts);

			assert stages.containsKey(m) : m;
			
			if( stages.get(m)!=3 ){
				previous.add(m);
				continue;
			}

			boolean deliverIt=true;
			for(WanAMCastMessage m1:previous){
				if(!m.commute(m1)){
					deliverIt=false;
					break;
				}
			}

			if(deliverIt){
				toDeliver.add(m);
			}else{
				previous.add(m);
				if( ConstantPool.WANAMCAST_DL>0 && stages.get(m)==3 ){
					if(!convoyEffectTracker.containsKey(m))
						convoyEffectTracker.put(m,System.currentTimeMillis());
				}						
			}
							
		}
		
		for(WanAMCastMessage msg : toDeliver){
			deliver(msg);
		}
	}

	//
	// Largest timestamp management
	//
	
	/**
	 * Update the largest recorded timestamp for m.
	 * @param m
	 */
	private void updateLargestTimestamp(WanAMCastMessage m){
		
		if(ConstantPool.WANAMCAST_DL > 6){
			System.out.println(this+" Updating ts of "+m);
		}

		if(!msg2ts.containsKey(m)){
			Timestamp ts = new Timestamp(m.uidToObject(),m.clock);
			msg2ts.put(m,ts);
			ts2msg.put(ts, m);
		}else{
			Timestamp oldTs = msg2ts.get(m);
			if(oldTs.compareToTs(m.uidToObject(),m.clock) < 0){
				ts2msg.remove(oldTs);
				Timestamp newTs = new Timestamp(m.uidToObject(),m.clock);
				ts2msg.put(newTs,m);
				msg2ts.put(m,newTs);
			}
		}
	}
	
	//
	// Debug
	//
	
	public String detailedInformation(){
		return "STAGE1 = " + stage1
				+ "\nSTAGES = " + stages
				+ "\nts2msg = "+ ts2msg
				+ "\nIntraGrouChannel = "+intraGroupChannel
				+ "\nConvoyEffectTracker = "+convoyEffectTracker;
	}

}	
