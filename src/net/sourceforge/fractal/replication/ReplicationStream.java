package net.sourceforge.fractal.replication;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.MutedStream;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

public abstract class ReplicationStream extends MutedStream implements Learner,Runnable {
	/**   
	* @author P. Sutra
	* 
	*/
	private static ValueRecorder checksum;
	static{
		checksum = new ValueRecorder("ReplicationStream#checksum");
		checksum.setFormat("%t");
	}
	
	protected Map<Integer,Semaphore> results;
	protected Set<Integer> clients;
	protected int myId;
	protected Stream stream;
	protected Thread emissionThread;
	protected BlockingQueue<CommutativeCommand> q;
	protected boolean terminated;
	private int deliveredCmds;
	private int lastDelivered;
	
	protected boolean isClient;
	
	private Map<Long,Long> checksumValues;
	
	public ReplicationStream(Set<Integer> c, Stream s, long nbObjects){
		results = CollectionUtils.newMap();
		clients = c;
		for(int i : clients){
			results.put(i, new Semaphore(0));
		}
		
		myId = FractalManager.getInstance().membership.myId();
		stream=s;
		emissionThread = new Thread(this, "ReplicationStream:emissionThread");
		q = CollectionUtils.newBlockingQueue();
		terminated=false;
		
		deliveredCmds=0;
		lastDelivered=0;
		
		checksumValues=new TreeMap<Long, Long>();
		for(long key=1; key<=nbObjects;key++){
			checksumValues.put(key,(long)0);
		}
		
	}
	
	@Override
	public final void start() {
		if(!clients.isEmpty()) register();
		stream.start();
		terminated=false;
		if(emissionThread.getState()==Thread.State.NEW)
			emissionThread.start();
	}

	@Override
	public final void stop() {
		if(!clients.isEmpty()) unregister();
		stream.stop();
		terminated=true;
		emissionThread.interrupt();
		for(long key : checksumValues.keySet()){
			checksum.add(checksumValues.get(key));	
		}
	}
	
	public final int execute(CommutativeCommand c,int clientId) {
		q.add(c);
		results.get(clientId).acquireUninterruptibly();
		return 0;
	}
	

	@SuppressWarnings("unchecked")
	public final void run() {
		ArrayList<CommutativeCommand> list;
		while(!terminated){
			list = new ArrayList<CommutativeCommand>();
			try {
				Thread.sleep(1);
				 list.add(q.take());
				 q.drainTo(list);
				 for(Command c : list){
					 sendCommand(c);
				 }
				// sendCommand(new ArrayCommutativeCommand(myId,list,lastDelivered));
			} catch (InterruptedException e) {
				if(!terminated){
					System.out.println("Interrupted!");
					terminated=true;
					continue;
				}
			}
		}
	}
	
	public final void learn(Stream s, Serializable value) {
		Command c = (Command) value;
//		ArrayCommutativeCommand array = (ArrayCommutativeCommand) receiveCommand(value);
//		for(Command c : array){
			ReadWriteRegisterCommand d = (ReadWriteRegisterCommand) c;
			deliveredCmds++;
			if(ConstantPool.PAXOS_DL > 1){
				assert checksumValues.containsKey(d.key) : d.key;
				if(d.isRead ){
					checksumValues.put(d.key, checksumValues.get(d.key)+ d.sequenceNumber());
				}else{
					checksumValues.put(d.key, checksumValues.get(d.key)%d.sequenceNumber());
				}
			}
			if(results.containsKey(c.source()))
				results.get(c.source).release();
//		}
//		lastDelivered = array.seq;
	}
	
	protected abstract void sendCommand(Command c);
	protected abstract Command receiveCommand(Serializable value);
	protected abstract void register();
	protected abstract void unregister();
}
