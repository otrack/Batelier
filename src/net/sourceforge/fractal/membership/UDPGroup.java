package net.sourceforge.fractal.membership;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.utils.CollectionUtils;

/**   
* @author L. Camargos
* 
*/

public class UDPGroup extends Group  implements Runnable{

	// For IP multicast
	protected String ip = null;
	
	private static Map<String,SocketListener> unicastListeners = CollectionUtils.newMap();

	private DatagramPacket mSndPack = null,  dSndPack = null;

	private MulticastSocket mRcvSock, sndSock;
	
	private SocketListener multiListener;
	
	private SocketListener uniListener;

	private InetAddress mcastAddr = null;

	private InetSocketAddress msockAddr = null, dsockAddr = null;

	private String ifaceAddrToListen;

	private BlockingQueue<byte[]> multicastQueue = null;

	private ByteArrayOutputStream multiBos = new ByteArrayOutputStream(ConstantPool.MEMBERSHIP_PAYLOAD_SIZE);    
	
	protected boolean terminate = false;
	
	protected BlockingQueue<Message> receivedMessages = null;
	
	protected Thread myThread;
	
	public UDPGroup(Membership m, String n, String ip, int port) {

		super(m, n,port);

		//Get ready to send ...
		try{
			sndSock = new MulticastSocket(null);
			//multicasts ...
			if(null != ip){
				mcastAddr = InetAddress.getByName(ip);
				mSndPack = new DatagramPacket(new byte[1], 1, mcastAddr, port);
				msockAddr = new InetSocketAddress(mcastAddr,port);
			}
			//and unicasts.
			dSndPack = new DatagramPacket(new byte[1], 1);
			dSndPack.setPort(port);
		} catch (Exception e) {e.printStackTrace();}

		if(ConstantPool.MEMBERSHIP_AGGREGATE_MULTICASTS){
			multicastQueue = CollectionUtils.newBlockingQueue();
			new Thread(new MultiJoiner() ,n+":multicast joiner").start();
		}
	}

	//
	// Node management
	//
	
	public synchronized boolean leaveGroup(){
		throw new RuntimeException("NYI");
	}
	
	public synchronized boolean joinGroup(){
		if(!swid2ip.containsKey(membership.myId())) return false;
		ifaceAddrToListen = swid2ip.get(membership.myId()).getAddress().getHostAddress();
		dsockAddr = swid2ip.get(membership.myId());
		
		try{
			receivedMessages = CollectionUtils.newBlockingQueue();
			//multicasts ...
			if(null != ip){
				mRcvSock = new MulticastSocket(msockAddr);
				mRcvSock.setTimeToLive(3);

				//Which interface to bind?
						if(null != ifaceAddrToListen){
							InetAddress x = InetAddress.getByName(ifaceAddrToListen);
							mRcvSock.setInterface(x);
							sndSock.setInterface(x);
						}
						mRcvSock.joinGroup(mcastAddr);
						mRcvSock.setLoopbackMode(false); //true to enable (hint).

						multiListener = new SocketListener(mRcvSock, receivedMessages );
						multiListener.start();
			}

			//and unicasts.
			uniListener = unicastListeners.get(dsockAddr.toString());
			if(null == uniListener){
				DatagramSocket dRcvSock = new DatagramSocket(null);
				dRcvSock.setReuseAddress(false);
				dRcvSock.bind(dsockAddr);
				uniListener = new SocketListener(dRcvSock, receivedMessages );
				unicastListeners.put(dsockAddr.toString(), uniListener);
				uniListener.start();
			}else{
				uniListener.addQueue(receivedMessages);
			}

		} catch (IOException e) {
			e.printStackTrace( );
			throw new RuntimeException("UNABLE TO CREATE/JOIN GROUP!!!!");
		}
		
		//Get ready to receive ...
		myThread = new Thread(this, name);
		myThread.start();
		return true;

	}
	
	public void unicast(int dstSWID, Message msg) {
		try{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(msg);
			byte[] buf = baos.toByteArray();
			dSndPack.setData(buf, 0, buf.length);            
			dSndPack.setSocketAddress(swid2ip.get(dstSWID));

			if(ConstantPool.MEMBERSHIP_DL > 2) 
				System.out.println(this+"Unicasting object (" + buf.toString()+ "] ==> " + name  +"[" +dstSWID + "@"+ dSndPack.getSocketAddress() + "] through" + sndSock.getInetAddress());

			sndSock.send(dSndPack);
			//     	}
		} catch (Exception e) {e.printStackTrace();}

	}
	
	@Override
	synchronized public void broadcastToOthers(Message msg) {
		throw new RuntimeException("NYI");
	}
	

	@Override
	synchronized public void broadcastTo(Message msg, Set<Integer> swids) {
		throw new RuntimeException("NYI");
	}

	
	synchronized public void broadcast(Message msg) {
		try {
			// Serialize to a byte array
			multiBos.reset();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(msg);
			byte[] buf = baos.toByteArray();
			if(buf.length > ConstantPool.NETWORK_MAX_PAYLOAD_SIZE){
				throw new RuntimeException("Message is too big! " + buf.length + ">" + ConstantPool.NETWORK_MAX_PAYLOAD_SIZE + "\n"+msg);
			}
			if(ConstantPool.MEMBERSHIP_DL > 2) System.out.println(this+"Multicasting message (" + buf.length + "b) to " + name);

			if(ConstantPool.MEMBERSHIP_AGGREGATE_MULTICASTS){
				multicastQueue.add(buf);

			}else{
				// And send them.
				if(null != ip) {        
					mSndPack.setData(buf, 0, buf.length);
					sndSock.send(mSndPack);
				}else{ 
					dSndPack.setData(buf, 0, buf.length);
					for(int gwid : swid2ip.keySet()){
//						if(!loader.membership.myIds.contains(gw2swid.get(gwid))){
						dSndPack.setSocketAddress(swid2ip.get(gwid));
						sndSock.send(dSndPack);
//						}else{
//						try {
//						receivedMessages.put(msg);
//						} catch (InterruptedException e) {
//						e.printStackTrace();
//						}
//						}
					}
				}
			}
		} catch (IOException e) {e.printStackTrace();System.out.println(msg);}

	}

	@SuppressWarnings("unchecked")
	public void run() {
		Message m = null;
		while(! terminate) {
			try {
				m = receivedMessages.take();
				try {
					deliver(m);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			} catch (InterruptedException e) {e.printStackTrace();}
			Thread.yield();
		}
	}
	
	public void stopThread() {
		terminate = true;
		myThread.interrupt();
		multiListener.stopThread();
		if(null != uniListener){
			uniListener.removeQueue(receivedMessages);
			if(uniListener.isEmpty())
				uniListener.stopThread();
		}
		try {
			myThread.join();
		} catch (InterruptedException e) {}
	}

	public void sockInfo(){
		System.out.println("Address:\n\t" + ((msockAddr!=null)?msockAddr:"") +"\n\t" + dsockAddr + "\n\tObject: " + ((mRcvSock!=null)?mRcvSock.toString():""));
	}  

	public String toString() { 
			return "UDOGroup:"+name+"@"+membership.myId();
	}

	class MultiJoiner extends Thread{
		public void terminate(){
			terminate = true;
		}

		public MultiJoiner(){
			super(name + ":MultiJoiner");
		}

		public void run() {
			byte[] buf = new byte[ConstantPool.NETWORK_MAX_PAYLOAD_SIZE],
			bufP = buf;
			int used, 
			free;
			while(!terminate){
				try {
					bufP = buf;
					used = 0;
					free = buf.length;
					byte[] buf1 = null, buf2 = null;

					//Get the first and check the second.
					try {
						buf1 = multicastQueue.take();
						buf2 = multicastQueue.peek();
					} catch (InterruptedException e) {e.printStackTrace();}

					if(null == buf2 || buf2.length + buf1.length > ConstantPool.NETWORK_MAX_PAYLOAD_SIZE){ //there will be no other message.
						bufP = buf1;
						used = buf1.length;
					}else{ //there will be more messages
						System.arraycopy(buf1,0,bufP,0,buf1.length);
						used += buf1.length;
						free -= buf1.length;

						while(buf2 != null && buf2.length <= free){ //while there is a next that fits.
							System.arraycopy(buf2,0,bufP,used,buf2.length);
							used += buf2.length;
							free -= buf2.length;

							try {
								multicastQueue.take();
							} catch (InterruptedException e) {e.printStackTrace();}

							buf2 = multicastQueue.peek();
						}                    
					}

					if(null != ip) {        
						if(ConstantPool.MEMBERSHIP_DL > 2) 
							System.out.println(this+"Multicasting object (" + used + "b) ==> " + name);

						//mSndPack = new DatagramPacket(buf, buf.length, mcastAddr, port);
						mSndPack.setData(bufP, 0, used);
						sndSock.send(mSndPack);
					}else{ 
						if(ConstantPool.MEMBERSHIP_DL > 2) System.out.println(this+"Multi-Unicasting object (" + used + "b) ==> " + name);

						dSndPack.setData(bufP, 0, used);
						for(InetSocketAddress i: swid2ip.values() ){
							dSndPack.setSocketAddress(i);
							sndSock.send(dSndPack);
						}
					}

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}                
			}
		}
	}


	@Override
	public void closeConnections() {
	}


	@Override
	public void start() {
	}


	@Override
	public void stop() {
	}


	@Override
	public void broadcast(ByteBuffer bb) {
		throw new RuntimeException();
	}

	@Override
	public void broadcastTo(ByteBuffer bb, Set<Integer> swids) {
		throw new RuntimeException("NYIT"); 
	}


}
