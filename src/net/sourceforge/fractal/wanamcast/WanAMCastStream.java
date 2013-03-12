/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
 */


package net.sourceforge.fractal.wanamcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.consensus.paxos.PaxosStream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.multicast.MulticastMessage;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.PerformanceProbe.FloatValueRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

/**   
* @author P. Sutra
* 
*/

public class WanAMCastStream extends Stream implements Runnable, Learner{

	private ValueRecorder aDeliveredSize;
	private ValueRecorder stagesSize;
	private TimeRecorder consensusLatency, coreLoopLatency, sideLoopLatency;
	private Map<WanAMCastMessage,Long> convoyEffectTracker;
	private FloatValueRecorder convoyEffect;
	private ValueRecorder checksum;
	private FloatValueRecorder latency;
	
	private int deliveredCnt = 0;
	@SuppressWarnings("unused")
	private String myName;
	int mySWid;
	private Group myGroup;
	private boolean terminate;
	
	private MulticastStream multicastStream;
	private PaxosStream paxosStream;
	private int K;

	private BlockingQueue<WanAMCastMessage> intraGroupChannel;
	private Collection<WanAMCastMessage> aDelivered; // Does not contain msg with dest={myGroup.name()}
	
	Thread mainThread;
	
	private Map<WanAMCastMessage, HashMap<String, Integer>> stage1;

	// PENDING:
	private Map<WanAMCastMessage, Integer> stages; // keep tracks of the current stage

	private TreeMap<Timestamp, WanAMCastMessage> ts2msg; 
	private HashMap<WanAMCastMessage, Timestamp> msg2ts; 
	
	// local Variables going to global so as to enhance the code
	private HashSet<String> toMyGroup;

	public WanAMCastStream(int id, Group g, String streamName, MulticastStream multicast, PaxosStream paxos){
		this.mySWid = id;
		this.toMyGroup = new HashSet<String>();
		toMyGroup.add(g.name());
		this.myGroup = g;
		this.myName = streamName;
		this.terminate = false;
		this.multicastStream = multicast;
		this.paxosStream = paxos;

		this.intraGroupChannel =  CollectionUtils.newBlockingQueue();
		
		this.aDelivered = CollectionUtils.newBoundedSet(5000);
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

		multicastStream.registerLearner("WanAMCastInterGroupMessage",this);

		mainThread = new Thread(this,"WanAMCast:main@size="+myGroup.size()+"@"+this.mySWid+"mainThread");
		
		// probes
		if(ConstantPool.WANAMCAST_DL>0){
			aDeliveredSize = new ValueRecorder(this+"#aDeliveredSize");
			aDeliveredSize.setFormat("%M");
			stagesSize = new ValueRecorder(this+"#stagesSize");
			stagesSize.setFormat("%M");
			consensusLatency = new TimeRecorder(this+"#consensusLatency(ms)");
			consensusLatency.setFormat("%a");
			coreLoopLatency =  new TimeRecorder(this+"#coreLoopLatency(ms)");
			coreLoopLatency.setFormat("%a");
			sideLoopLatency =  new TimeRecorder(this+"#sideLoopLatency(ms)");
			sideLoopLatency.setFormat("%a");
			checksum = new ValueRecorder(this+"#checksum");
			checksum.setFormat("%t");
			convoyEffectTracker = new HashMap<WanAMCastMessage, Long>();
			convoyEffect = new FloatValueRecorder(this+"#convoyEffect(ms)");
			convoyEffect.setFormat("%a");
			latency = new FloatValueRecorder(this+"#latency(ms)");
			latency.setFormat("%a");
		}
		
	}

	@SuppressWarnings("unchecked")
	public void run(){

		WanAMCastMessage msg;
		ArrayList<WanAMCastMessage> p;
		ArrayList<WanAMCastMessage> d;
		Integer maxClock;
		HashSet<String>dest = new HashSet<String>();
		boolean needToDeliver;
		
		HashMap<String, ArrayList<WanAMCastMessage>> toSend = new HashMap<String, ArrayList<WanAMCastMessage>>();
		
		while(!terminate){
			try {
				
				if(ConstantPool.WANAMCAST_DL > 6)
					System.out.println(this+ " I start round "+ K);
	
				if(myGroup.size()==1){ // Optimization 
				
					d = new ArrayList<WanAMCastMessage>();
					d.add(intraGroupChannel.take());
					intraGroupChannel.drainTo(d);
				
				}else{
										
					// FIXME
					
					if( ! paxosStream.isDecided(K) && myGroup.isLeading(mySWid) ){

						// 1 - Batch messages
						p = new ArrayList<WanAMCastMessage>();
						p.add(intraGroupChannel.take());
						intraGroupChannel.drainTo(p);
						
						// 2 - Propose them to consensus
						if(ConstantPool.WANAMCAST_DL>0 ) consensusLatency.start();
						paxosStream.propose(p,K);

						// 3 - Decide consensus
						d = (ArrayList<WanAMCastMessage>)paxosStream.decide(K);
						if(ConstantPool.WANAMCAST_DL>0 ) consensusLatency.stop();

					}else{

						if(ConstantPool.WANAMCAST_DL>0 ) consensusLatency.start();
						d = (ArrayList<WanAMCastMessage>)paxosStream.decide(K);
						if(ConstantPool.WANAMCAST_DL>0 ) consensusLatency.stop();
						intraGroupChannel.removeAll(d);

					}

				}

				maxClock=K;

				needToDeliver = false;

				coreLoopLatency.start();

				for(WanAMCastMessage m : d){

					synchronized(this){

						if(ConstantPool.WANAMCAST_DL > 6)
							System.out.println(this+" Next message "+m);

						if( aDelivered.contains(m)) {
							if(ConstantPool.WANAMCAST_DL > 3)
								System.out.println(this+" I kick "+m);
							continue;
						}

						if(m.dest.size()==1){ 

							assert m.dest.contains(myGroup.name());
							assert !msg2ts.containsKey(m);
							deliver(m);

							// stages.put(m, 3);
							// m.clock = K;
							// updateTimestamp(m);
							// needToDeliver=true;		

						}else{

							if(!stages.containsKey(m))  
								stages.put(m, 0);

							if(stages.get(m)==0){	
								stages.put(m, 1);

								if(!stage1.containsKey(m)){
									stage1.put(m, new HashMap<String,Integer>());
								}
								m.clock=K;

								// FIXME
								if(myGroup.isLeading(mySWid)){
									for(String g: m.dest){
										if(!g.equals(myGroup.name())){
											if( !toSend.keySet().contains(g)){
												toSend.put(g,new ArrayList<WanAMCastMessage>());
											}
											m.gSource = myGroup.name();
											toSend.get(g).add((WanAMCastMessage)m);
										}
									}
								}

							}else{ // stage 1 or 2 locally (if it is 1, I am in late)
								stages.put(m, 3);
								needToDeliver = true;
								if(m.clock > maxClock) maxClock = m.clock;
							}

							stage1.get(m).put(myGroup.name(), m.clock);	
							updateTimestamp(m);
							needToDeliver |= testEndGathering(m);

						}

					} // end synchronized

				} // end for

				if(needToDeliver) 
					testDeliver();

				if(ConstantPool.WANAMCAST_DL > 6)
					System.out.println(this+" I set the clock to "+ (maxClock+1));

				K = maxClock+1 ;

				// We send to others
				if(myGroup.isLeading(mySWid)){
					for(String g : toSend.keySet()){
						if(toSend.get(g).size()>0){
							dest.clear();
							dest.add(g);
							if(ConstantPool.WANAMCAST_DL > 3)
								System.out.println(this+" I RM-cast "+toSend.get(g)+" to "+ dest);
							multicastStream.multicast(new WanAMCastInterGroupMessage(toSend.get(g),dest,myGroup.name(),mySWid));
						}
					}
					toSend.clear();
				}

				coreLoopLatency.stop();	
				
			}catch(Exception e){
				e.printStackTrace();
			}
			
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
		multicastStream.start();
		if(mainThread.getState()==Thread.State.NEW){
			mainThread.start();
		}
	}

	public void stop(){
		multicastStream.stop();
		terminate=true;
	}

	public String toString(){
		return "WanAMCast:"+mySWid;
	}
	
	@SuppressWarnings("unchecked")
	public void learn(Stream s, Serializable value) {
		
		MulticastMessage n = (MulticastMessage)value;
		ArrayList<WanAMCastMessage> msgs = (ArrayList<WanAMCastMessage>)n.serializable;
		
		if(ConstantPool.WANAMCAST_DL > 3){
			System.out.println(this+" I RM-deliver "+ msgs );	
		}

		sideLoopLatency.start();
		
		for(WanAMCastMessage m : msgs){

			assert(m.dest.contains(myGroup.name())) : myGroup.name() + " vs "+ m.dest;
			assert(!m.gSource.equals(myGroup.name()) || myGroup.contains(m.source)) : m+" "+m.source;

			synchronized(this){

				if( aDelivered.contains(m) ) {
					if(ConstantPool.WANAMCAST_DL > 3)
						System.out.println(this+" I kick "+m);
					continue;
				}

				if( !stages.containsKey(m)){
					stages.put(m,0);
					try {
						intraGroupChannel.put(m);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}						
				}

				if( m.clock!=-1 ){
					if(!stage1.containsKey(m)) 
						stage1.put(m, new HashMap<String,Integer>());
					stage1.get(m).put(m.gSource, m.clock);
					testEndGathering(m);
				}

			}

		} // end for
		
		sideLoopLatency.stop();

	}

	public boolean isClean(){
		return stages.isEmpty() && msg2ts.isEmpty() && ts2msg.isEmpty() && intraGroupChannel.isEmpty();
	}

	@Override
	public boolean registerLearner(String msgType, Learner learner){
		if(ConstantPool.WANAMCAST_DL > 1){
			System.out.println(this+" register learner fo "+msgType);
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
					checksum.add(m.getUniqueId().hashCode()%deliveredCnt);
				}
				if(ConstantPool.WANAMCAST_DL > 4)
					System.out.println(this+" I deliver "+m);
				l.learn(this,(m));
			}
		
		}else{
			if(ConstantPool.WANAMCAST_DL > 0)
				System.out.println(this+" got a "+ m.getMessageType() +" to nobody");
		}
	
		synchronized(this){
			aDelivered.add(m);
			intraGroupChannel.remove(m);
			stages.remove(m);
			stage1.remove(m); 
			if(msg2ts.containsKey(m)){
				ts2msg.remove(msg2ts.get(m));
				msg2ts.remove(m);
			}
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
	
	private void testDeliver(){
		WanAMCastMessage m;
		
		if(ConstantPool.WANAMCAST_DL>0){
			stagesSize.add(stages.keySet().size());
			stagesSize.add(stage1.keySet().size());
			stagesSize.add(ts2msg.keySet().size());
			stagesSize.add(msg2ts.keySet().size());
		}

		if(ConstantPool.WANAMCAST_DL > 3)
			System.out.println(this+" Smallest ts ="+ts2msg.keySet().iterator().next());
					
		List<WanAMCastMessage> previous = new ArrayList<WanAMCastMessage>(ts2msg.size());
		List<WanAMCastMessage> toDeliver = new ArrayList<WanAMCastMessage>(ts2msg.size());
		
		synchronized(this){
			
			for(Timestamp ts : ts2msg.keySet()){
				
		 		m = ts2msg.get(ts);
				assert stages.containsKey(m) : m + " "+ ts + " "+ts2msg;
									
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
			
		}
		
		for(WanAMCastMessage msg : toDeliver){
			deliver(msg);
		}
	}

	private void updateTimestamp(WanAMCastMessage m){
		if(ConstantPool.WANAMCAST_DL > 6){
			System.out.println(this+" Updating ts of "+m);
		}
		assert msg2ts.size()==ts2msg.size();
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

	private boolean testEndGathering(WanAMCastMessage msg){
		
		Integer maxGroupClock = 0;
		
		if(stage1.get(msg).keySet().size()!=msg.dest.size())
			return false;

		assert stage1.get(msg).containsKey(myGroup.name()) : myGroup.name() + " with " +stage1.get(msg);

		for(String g : stage1.get(msg).keySet()){
			if(stage1.get(msg).get(g) > maxGroupClock){
				maxGroupClock = stage1.get(msg).get(g);
			}
		}

		if( stages.get(msg)==1 ) {
			msg.clock = maxGroupClock;
			if(ConstantPool.WANAMCAST_DL > 3)
				System.out.println(this+" "+msg+": stage 1 -> stage 2");
			stages.put(msg, 2);
			try {
				intraGroupChannel.put(msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}else{
			if( stages.get(msg)==2 ) {
				if(ConstantPool.WANAMCAST_DL > 3)
					System.out.println(this+" "+msg+" : stage 1/2 -> stage 3 (Highest clock)");
				stages.put(msg,3);
			}
		}
			
		return true;

	}

	public String detailedInformation(){
		return "STAGE1 = " + stage1
				+ "\nSTAGES = " + stages
				+ "\nts2msg = "+ ts2msg
				+ "\nIntraGrouChannel = "+intraGroupChannel
				+ "\nConvoyEffectTracker = "+convoyEffectTracker;
	}

}	
