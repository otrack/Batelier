package net.sourceforge.fractal.membership;

import java.nio.ByteBuffer;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Message;

public class TCPDynamicGroup extends TCPGroup {

	public TCPDynamicGroup(Membership m, String n, int p) {
		super(m,n, p);
	}
	
	@Override
	protected synchronized void registerConnection(int swid,TCPGroupP2PConnection connection) {
		super.registerConnection(swid, connection);
		putNode(swid, connection.getRemoteHost());
	}
	
	@Override
	protected synchronized void unregisterConnection(int swid, TCPGroupP2PConnection connection) {
		super.unregisterConnection(swid, connection);
		if( swid2connections.get(swid).isEmpty()){
			if(swid!=membership.myId()) removeNode(swid);
		}
	}
	
	@Override 
	public void unicast(int swid, Message m){
		
		if(membership.adressOf(swid)==null){
			if (ConstantPool.MEMBERSHIP_DL > 1 )
				System.out.println("Node "+swid+" does not exist.");
				return;
		}
		
		if(!swid2ip.containsKey(swid)){
				String ip = membership.adressOf(swid);
				putNode(swid, ip);
		}
		
		ByteBuffer bb;
		bb = Message.pack(m,membership.myId());
		performUnicast(swid,bb);
		
	}
	
	//
	// Misc
	//
	
	public String toString() {
		return "TCPDynamicGroup:"+name + ((ConstantPool.MEMBERSHIP_DL > 2) ? "@"+membership.myId() : "");
	}



}
