package net.sourceforge.fractal.broadcast;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.CollectionUtils;

public final class BroadcastStream extends Stream implements Runnable {

	@SuppressWarnings("unused")
	private String streamName;
	private int mySWid;
	private Group myGroup;

	private BlockingQueue<Message> receiveChannel;
	private Thread receptionThread;
	private boolean terminate;
	
	public BroadcastStream(String streamName, Group myGroup, int mySWid){
		this.streamName = streamName;
		this.mySWid = mySWid;
		this.myGroup = myGroup;
		this.terminate = false;
		
		// setting up queues
		receiveChannel = CollectionUtils.newIdBlockingQueue(this+":queue");

		// Setting-up the threads
		receptionThread = new Thread(this,this+":reception");
	}

	public void run() {
			BroadcastMessage m;
		
			while(!terminate){
				try{
					m = (BroadcastMessage) receiveChannel.take();
					deliver(m);
				} catch (InterruptedException e){
					if(!terminate)
						e.printStackTrace();
				}
			}
	}

	public void broadcast(BroadcastMessage m){
		assert learners.containsKey(m.getMessageType()) && ! learners.get(m.getMessageType()).isEmpty() : m.getMessageType()+" is not registered";
		if(ConstantPool.BROADCAST_DL > 0)
			System.out.println(this+" I Reliable Broadcast "+m);
		myGroup.broadcast(m);
	}

	public void broadcastToOthers(BroadcastMessage m){
		if(ConstantPool.BROADCAST_DL > 0)
			System.out.println(this+" I Reliable Broadcast (to others) "+m);
		myGroup.broadcastToOthers(m);
	}
	
	public void unicast(BroadcastMessage m, int swid){
		if(ConstantPool.BROADCAST_DL > 0)
			System.out.println(this+" I unicast "+m+" to "+swid);
		myGroup.unicast(swid, m);
	}
	
	@Override
	public void start(){
		if(receptionThread.getState()==Thread.State.NEW)
			receptionThread.start();
	}
	
	@Override
	public void stop(){
		for(String registeredType : learners.keySet()){
			for(Learner l : learners.get(registeredType))
				unregisterLearner(registeredType, l);
		}
		terminate=true;
		receptionThread.interrupt();
	}

	@Override
	public boolean registerLearner(String msgType, Learner learner){
		if(super.registerLearner(msgType, learner)){
			return myGroup.registerQueue(msgType, receiveChannel);
		}
		return false;
	}
	
	@Override
	public boolean unregisterLearner(String msgType, Learner learner){
		if(super.unregisterLearner(msgType, learner)){
			return myGroup.unregisterQueue(msgType, receiveChannel);
		}
		return false;
	}
	
	@Override
	public void deliver(Serializable s) {
		Message m = (Message)s;
		if( learners.containsKey(m.getMessageType()) && !learners.get(m.getMessageType()).isEmpty() ){
			for(Learner l : learners.get(m.getMessageType())){
				if(ConstantPool.BROADCAST_DL > 0)
					System.out.println(this+" deliver "+ m+" to "+l);
				l.learn(this,(m));
			}
		}else{
			if(ConstantPool.BROADCAST_DL > 0)
				System.out.println(this+" got a "+ m.getMessageType() +" to nobody");
		}
	}
	
	@Override
	public String toString(){
		return "Broadcast:"+mySWid+":"+streamName;
	}

}
