package net.sourceforge.fractal.membership;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import com.sleepycat.db.MessageHandler;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.ExecutorPool;
import net.sourceforge.fractal.utils.IdBlockingQueue;
import net.sourceforge.fractal.utils.ObjectUtils.InnerObjectFactory;

/**   
* @author P. Sutra
* @author L. Camargos
* 
*/ 

public abstract class Group extends Stream implements Comparable<Group>{

	protected BlockingQueue<ByteBuffer[]> queue;

	protected Membership membership;
	protected String name;
	protected int port;
	protected boolean joined;
	
	protected Map<Integer,InetSocketAddress> swid2ip;	//Group wide to ip map
	protected Map<String,Set<BlockingQueue<Message>>> type2QueueSet;
	protected Map<String, Set<Learner>> learners;

	public Group(Membership m, String n, int p) {
		membership = m;
		name = n;
		port = p;
		joined = false;
		
		swid2ip = new TreeMap<Integer, InetSocketAddress>();
		
		type2QueueSet = CollectionUtils.newConcurrentMap();
		learners = CollectionUtils.newConcurrentMap();
		
		queue = CollectionUtils.newBlockingQueue();
		ExecutorPool.getInstance().submitMultiple(
				new InnerObjectFactory<MessageHandlerTask>(MessageHandlerTask.class,Group.class,this));
	}

	// Interfaces to implement
	public abstract void unicast(int dstSWID, Message msg);
	public abstract void broadcast(Message msg);
	public abstract void broadcastTo(ByteBuffer bb, Set<Integer> swids);
	public abstract void broadcastTo(Message msg, Set<Integer> swids);
	public abstract void broadcastToOthers(Message msg);
	public abstract void broadcast(ByteBuffer bb);
	public abstract boolean joinGroup();
	public abstract boolean leaveGroup();
	public abstract void closeConnections();

	public int leader() {
		return swid2ip.keySet().iterator().next(); // lowest id
	}
	
	public final boolean isLeading(int swid) throws IllegalArgumentException{
		return swid==swid2ip.keySet().iterator().next(); // lowest id
	}

	@SuppressWarnings("unchecked")
	public boolean registerQueue(String type, BlockingQueue<Message> queue) {
		Set<BlockingQueue<Message>> queueSet = type2QueueSet.get(type);
		if(queueSet ==  null){
			queueSet = CollectionUtils.newSet();
			type2QueueSet.put(type,queueSet);
		}
		queueSet.add(queue);
		
		if(ConstantPool.MEMBERSHIP_DL > 7) 
			if(! (queue instanceof IdBlockingQueue))
				System.out.println(this+" added receiver to type " + type);
			else System.out.println(this+" added receiver " 
					+ ((IdBlockingQueue) queue).getId() 
					+ " to type " + type);
		return true;
	}
	
	public boolean unregisterQueue(String type, BlockingQueue<Message> queue) {
		Set<BlockingQueue<Message>> queueSet = type2QueueSet.get(type);
		if(queueSet == null)
			return false;
		return queueSet.remove(queue);
	}
	
	public boolean registerLearner(String msgType, Learner learner){
		if(learners.get(msgType)==null)
			learners.put(msgType,new HashSet<Learner>());
		return learners.get(msgType).add(learner);
	}
	
	public boolean unregisterLearner(String msgType, Learner learner){
		return learners.get(msgType).remove(learner);
	}
	
	protected final void deliver(ByteBuffer[] toRecv, TCPGroupP2PConnection connection)
	throws InterruptedException{
		queue.put(toRecv);
	}
	
	@SuppressWarnings("unchecked")
	protected void deliver(Message m) 
	throws InterruptedException{
		
		Set<BlockingQueue<Message>> rcvs = type2QueueSet.get(m.getMessageType());
		boolean isDelivered = false;
		
		if(ConstantPool.MEMBERSHIP_DL > 6)
			System.out.println(this +"  got "+m+" of type " + m.getMessageType() + " from "+ m.source);
		
		if(rcvs != null && ! rcvs.isEmpty()){			
			for(BlockingQueue<Message> rcv : rcvs){
				rcv.put(m);
				isDelivered = true;
			}
		}
		
		if( learners.containsKey(m.getMessageType()) ){
			for(Learner l : learners.get(m.getMessageType()) ){
				l.learn(this,m);
				isDelivered = true;
			}
		}
		
		if(!isDelivered)
			System.out.println(this+" got  a "+m.getMessageType()+"  fornobody");
		
	}

	//
	// Nodes management
	//
	
	public synchronized boolean putNode(Integer swid, String ip) {
			
    	if( swid2ip.containsKey(swid))  return false;
		
		if(ConstantPool.MEMBERSHIP_DL>1)
			System.out.println(this+" put node "+swid+"("+ip+")");
		
		membership.addNode(swid, ip);
		
		InetSocketAddress localAddress = new InetSocketAddress(ip, port);
		swid2ip.put(swid, localAddress);
		if(! joined && membership.myId() == swid){
			joinGroup();
			joined = true;
		}
		return true;
	}
	
	public synchronized boolean removeNode(int swid){
		
		if(ConstantPool.MEMBERSHIP_DL>1)
			System.out.println(this+" remove node "+swid);
		
		if(!swid2ip.containsKey(swid)) return false;
		
		if( joined && membership.myId() == swid) return false;
		
		swid2ip.remove(swid);
		membership.removeNode(swid);

		return true;
		
	}
	
	public synchronized boolean putNodes(Collection<Integer> nodes){
		boolean ret = false;
		for(int swid : nodes){
			if(! membership.allNodes().contains(swid)) throw new IllegalArgumentException("node "+swid+" does not exist");
			ret |= putNode(swid, membership.adressOf(swid));
		}
		return ret;
	}
	
	public boolean contains(int swid){
		return swid2ip.containsKey(swid);
	}

	/**
	 * 
	 * @param i
	 * @return the i^th  node in the (sorted) group.
	 */
	public int get(int i) {
		return swid2ip.keySet().toArray(new Integer[0])[i];
	}

	public Set<Integer> members(){
		return swid2ip.keySet();
	}

	public int size() {
		return swid2ip.size();
	}

	
	//
	// Others
	//
	
	public String name() {
		return name;
	}
	
	public int compareTo(Group g){
		return this.name.compareTo(g.name);
	}
	
	// FIXME
	public void deliver(Serializable s){
		throw new RuntimeException("Invalid usage");
	}
	
	@Override
	public int hashCode(){
		return name.hashCode();
	}
	

	//
	// Inner classes
	//

	public class MessageHandlerTask implements Runnable{

		public MessageHandlerTask() {}

		@Override
		public void run() {
			List<ByteBuffer[]> list = new ArrayList<ByteBuffer[]>();
			while(true){
				try{
					list.add(queue.take());
					queue.drainTo(list);
					for(ByteBuffer[] bbs : list)
						for(ByteBuffer bb : bbs)
							deliver(Message.unpack(bb));
					list.clear();
				} catch (Exception e) {
					if(ConstantPool.MEMBERSHIP_DL>1)
						e.printStackTrace();
				}
			}
		}
		
	}
		
}
