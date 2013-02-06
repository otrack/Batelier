package net.sourceforge.fractal.membership;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.MessageInputStream;
import net.sourceforge.fractal.MessageOutputStream;
import net.sourceforge.fractal.utils.ExecutorPool;
import net.sourceforge.fractal.utils.ThreadPool;


/**
 * This class implements unreliable TCP connections inside a group.
 *
 * The group is static, i.e., the membership never changes.
 * 
 * To send a message to a node, the sender does not have to belong to the group.
 *
 * We create a new connection to a node p when:
 * (i)  we send a message to this node and the connection to p does not exist.
 * (ii) the TCPGroupServer receives a new connection from p.
 * 
 * When we join the group we create a server to listen new incoming connections.
 *  
 * 
 * @author P. Sutra
 * 
 */

// FIXME merged joined and isTerminated
// FIXME add selectors
public class TCPGroup extends Group {

	protected TCPGroupServer server;
	protected ConcurrentHashMap<Integer, List<TCPGroupP2PConnection>> swid2connections;
	protected boolean isTerminated;

	public TCPGroup(Membership m, String n, int p) {
		super(m,n, p);
		swid2connections = new ConcurrentHashMap<Integer, List<TCPGroupP2PConnection>>();
		isTerminated = true;
	}

	//
	//	Interfaces		
	//

	public static void validateMagic(int magic) throws IllegalArgumentException
	{
		if (magic != ConstantPool.PROTOCOL_MAGIC)
			throw new IllegalArgumentException("invalid protocol header");
	}

	@Override
	public synchronized boolean joinGroup() {
		if(!swid2ip.containsKey(membership.myId())) return false;
		if(server==null){
			server = new TCPGroupServer(this);
			server.start();
			if (ConstantPool.MEMBERSHIP_DL > 1 ) 
				System.out.println(this+" group joined");
		}
		return true;
	}

	@Override
	public synchronized boolean leaveGroup() {
		if(server!=null){
			server.stop();
		}
		return true;
	}


	@Override
	public void broadcast(ByteBuffer bb){
		broadcastTo(bb, swid2ip.keySet());
	}

	@Override
	public void broadcast(Message m){
		broadcast(Message.pack(m,membership.myId()));
	}

	@Override
	public void broadcastTo(Message m, Set<Integer> swids) {
		broadcastTo(Message.pack(m,membership.myId()),swids);
	}

	public void broadcastToOthers(Message msg){
		Set<Integer> swids = new HashSet<Integer>(swid2ip.keySet()); // FIXME use filtering set
		swids.remove(membership.myId());
		broadcastTo(Message.pack(msg,membership.myId()), swids);
	}
	
	@Override
	public void broadcastTo(ByteBuffer bb, Set<Integer> swids) {
		boolean once = true;
		for(int swid : swids){
			if(once){
				performUnicast(swid,bb);
				once = false;
			}else{
				performUnicast(swid,bb.duplicate());	
			}
		}
	}


	@Override
	public void unicast(int swid, Message msg) {
		
		if( !membership.allNodes().contains(swid) ){
			if (ConstantPool.MEMBERSHIP_DL > 1 )
				System.out.println( this + " node "+ swid+" has left this group.");
			return;
		}
		
		ByteBuffer bb;
		bb = Message.pack(msg,membership.myId());
		performUnicast(swid,bb);
	}

	@Override
	public void stop(){
		if(!isTerminated){
			isTerminated=true;
			leaveGroup();
			closeConnections();
		}
	}

	@Override
	public void start(){
		if(isTerminated){
			joinGroup();
			isTerminated=false;
		}
	}

	//
	// Connection management 
	// 

	protected void unregisterConnection(int swid, TCPGroupP2PConnection connection) {	

		if( !swid2connections.containsKey(swid)
				||
				!swid2connections.get(swid).contains(connection)) return;

		if (ConstantPool.MEMBERSHIP_DL > 1 ) 
			System.out.println(this+" unregister connection to " + swid);

		connection.stop();
		swid2connections.get(swid).remove(connection);	
	}

	protected  void registerConnection(int swid, TCPGroupP2PConnection connection){

		if (ConstantPool.MEMBERSHIP_DL > 1 ) 
			System.out.println(this+" register connection to " + swid);

		swid2connections.putIfAbsent(swid, new CopyOnWriteArrayList<TCPGroupP2PConnection>());
		swid2connections.get(swid).add(connection);
		membership.addNode(swid, connection.getRemoteHost());

	}

	protected TCPGroupP2PConnection getOrCreateConnection(int swid) throws TCPConnectionDownException {

		if (ConstantPool.MEMBERSHIP_DL > 3 ) 
			System.out.println(this+" get or create connection to " + swid);

		if ( swid2connections.containsKey(swid) &&  !swid2connections.get(swid).isEmpty() ){
			return swid2connections.get(swid).iterator().next();
		}
		
		synchronized(swid2ip.get(swid)){ // to prevent concurrent connection creation.
			if ( swid2connections.containsKey(swid) &&  !swid2connections.get(swid).isEmpty() ){
				return swid2connections.get(swid).iterator().next();
			}
			TCPGroupP2PConnection connection;
			InetSocketAddress remoteAddress = swid2ip.get(swid);
			connection = new TCPGroupP2PConnection(swid,remoteAddress, this);
			registerConnection(swid, connection);
			connection.start();
			return connection;
		}

	}

	@Override
	public void closeConnections() {
		if (server != null)
			server.stop();

		if (ConstantPool.MEMBERSHIP_DL > 1)
			System.out.println(this+" closing connections");

		List<TCPGroupP2PConnection> toStop = new ArrayList<TCPGroupP2PConnection>();
		for (int swid : swid2connections.keySet()) {
			for (TCPGroupP2PConnection connection : swid2connections.get(swid)) {
				toStop.add(connection);
			}
		}
		for(TCPGroupP2PConnection connection : toStop)
			connection.stop();

	}

	//
	// Message management 
	// 



	protected void performUnicast(int swid, ByteBuffer bb) {

		if (ConstantPool.MEMBERSHIP_DL > 7)
			System.out.println(this + " unicasting to "+swid+" by "+Thread.currentThread().getName());

		if( !membership.allNodes().contains(swid) ){
			System.out.println( this + " node "+ swid+" does not exist.");
			return;
		}

		if(swid==membership.myId()){
			
			ByteBuffer[] bbs = {bb};
			try{
				deliver(  bbs,  null);
			}catch(InterruptedException e){};
			
		}else{
		
			TCPGroupP2PConnection connection = null;

			try{

				connection = getOrCreateConnection(swid);			
				connection.unicast(bb);

			}catch (TCPConnectionDownException e) {

				if (ConstantPool.MEMBERSHIP_DL > 1)
					System.out.println(this + " connection to "+swid+" is down; resaon: "+e.getMessage());

				if(connection!=null)
					unregisterConnection(swid, connection);
			}
			
		}

	}

	//
	// Misc
	//

	public String toString() {
		return "TCPGroup:"+name+ ((ConstantPool.MEMBERSHIP_DL > 3) ? "@"+membership.myId() : "");
	}


}
