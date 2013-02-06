package net.sourceforge.fractal;

import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.abcast.ABCastMessage;
import net.sourceforge.fractal.abcast.ABCastStream;
import net.sourceforge.fractal.ftwanamcast.FTWanAMCastStream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.wanabcast.WanABCastStream;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;
/**   
* @author N.Schiper
* @author P. Sutra
* 
*/
public class TPCC implements Runnable{

	
	WanABCastStream wanab;
	WanAMCastStream wanam;
	ABCastStream paxos; 
	FTWanAMCastStream ftwanab;
	
	FractalManager fl;
	ArrayList<Group> allGroups;
	ArrayList<String> allGroupsNames, otherGroupsNames;
	Group myGroup;
	int mySWid;
	float rho;


	/*
	 * algType:
	 * 0 => wanamcast
	 * 1 => wanabcast
	 * 2 => abcast
	 */
	int algType;
	
	int rate, duration; // rate in transaction per second, duration in minutes	
	float ratio;
	
	Delivery delivery;
	Thread tpccThread;

	@SuppressWarnings("unchecked")
	public TPCC(FractalManager fl, int algType, int mySWid, int rate, int duration, float rho){

		this.fl = fl;
		this.algType = algType;
		this.rate = rate;
		this.duration = duration;
		this.mySWid = mySWid;
		this.allGroups = new ArrayList<Group>(FractalManager.getInstance().membership.allGroups());
		allGroupsNames = new ArrayList<String>();
		for(Group g : allGroups) {
			allGroupsNames.add(g.name());
			if(g.members().contains(FractalManager.getInstance().membership.myId()))
				this.myGroup = g;
		}
		this.otherGroupsNames = (ArrayList<String>)allGroupsNames.clone();
		this.otherGroupsNames.remove(myGroup.name());
		this.rho = rho;
		
		if(algType == 2 )
			throw new RuntimeException("Compatibility issue with ABCastStream !"); // TOD
		
		switch(algType){
			case 0 : wanam = fl.wanamcast.stream("WANAMCAST"); break;  
			case 1 : wanab = fl.wanabcast.stream("WANABCAST"); break;
			case 2 : paxos = fl.abcast.stream("ABCAST"); break;
			case 3 : ftwanab = fl.ftwanamcast.stream("FTWANAMCAST"); break;
			
			default : throw new RuntimeException("Incorrect parameters");
		}
		
		delivery = new Delivery(this.rate);
		tpccThread = new Thread(this,"TPCC:main@"+this.mySWid);
	}

	private void runTest(){
		
		int msgSize;  //multicast msg size in bytes
		DatagramSocket rcvSocket = null;
		byte[] msg = new byte[12];
		DatagramPacket packet = new DatagramPacket(msg, msg.length);
		HashSet<String> dest = new HashSet<String>();
		ArrayList<Object> message = new ArrayList<Object>();
	
		long startTime = System.currentTimeMillis();
		int nbMsgCast = 0;
		
		try {
			rcvSocket = new DatagramSocket(6666);			
		} catch (Exception e) {
			throw new RuntimeException("Unable to listen.");
		}
		
		do {
			
			try {
				rcvSocket.receive(packet);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			msg = (byte[]) packet.getData();
			
			dest = new HashSet<String>();
			message = new ArrayList<Object>();

			for (int i = 0; i < msg.length - 2; i++) {
				if (msg[i] == 1)
					dest.add((new Integer(i)).toString());
			}
					
			message.add(0,System.currentTimeMillis());
			message.add(1,dest);
			message.add(2,mySWid);
			
			switch (msg[10]) {
				case 1 : msgSize = (4 * 4) + 2*(15 * 4);;
						 break;
				case 2 : msgSize = (6 * 4) + 20;;
						 break;
				case 3 : msgSize = 2 * 4;
						 break;
				case 4 : msgSize = 3 * 4;
						 break;
				case 5 : msgSize = (4 * 4) + 20;
						 break;
				default : msgSize = 0;
						 break;
			}
			message.add(3,new ArrayList<Integer>((8*msgSize)/Integer.SIZE)); // we add a payload of size <msgSize> bytes.
			message.add(4,msg[11]); // client ID
			
			switch(algType){
				case 0 : wanam.atomicMulticast(message, dest); break;
				case 1 : wanab.atomicMulticastNG(message,dest); break;
				// case 2 : if(mySWid==0) paxos.atomicBroadcast(new ABCastMessage(message,mySWid)); break; // FIXME compatibility
				case 3 : ftwanab.atomicMulticastNG(message,dest); break;			
			}
			if (nbMsgCast == 0)
				startTime = System.currentTimeMillis();
			
			nbMsgCast++;
		} while ((System.currentTimeMillis() - startTime) <= (this.duration * 60000));

		dest = new HashSet<String>();
		message = new ArrayList<Object>();

		message.add(0,(long)0);

		for(Group g : allGroups){
			dest.add(g.name()); 
		}
		
		switch(algType){
			case 0 : wanam.atomicMulticast(message, dest); break;
			case 1 : wanab.atomicMulticastNG(message,dest); break;
			// case 2 : paxos.atomicBroadcast(new ABCastMessage(message,mySWid)); break; // FIXME compatibility
			case 3 : ftwanab.atomicMulticastNG(message,dest); break;		
		}
		
	}

	public void run() {
		runTest();
	}


	public void launchTest(){

		// Starting test
		switch(algType){
			case 0 : wanam.start(); break;
			case 1 : wanab.start(); break;
			case 2 : paxos.start(); break;
			case 3 : ftwanab.start(); break;
		}
		
		delivery.start();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		tpccThread.start();
		long startTime = System.currentTimeMillis();
		delivery.stop();
		long totalTime = System.currentTimeMillis()-startTime;
		
		System.out.println("( "+mySWid +") Checksum " + delivery.checksum+"  "+delivery.totalMessageDel);
		
		float latency = delivery.myMessagesLat/((float)delivery.myMessagesDel);
		float tpm = ((float)delivery.myMessagesDel/(float)totalTime)*(float)60000; 
		System.out.println("( " + mySWid +" ) "+
				"[tpm, avrgLatency(ms)]"
				+ "=" + "["
				+ tpm + ","
				+ latency
				+ "]"
		);

		try {
			// We have to wait that all the packets for consensus are sent.
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 

		switch(algType){
			case 0 : wanam.stop(); break;
			case 1 : wanab.stop(); break;
			case 2 : paxos.stop(); break;
			case 3 : ftwanab.stop(); break;
		}
		fl.stop();

	}

	class Delivery implements Runnable, Learner{

		float myMessagesLat = 0f; // in ms
		int myMessagesDel = 0; // only to me
		int totalMessageDel = 0; // for everybody
		long checksum=0;
		Thread deliveryThread;
		boolean terminate=false;
		int nbClients;
		BlockingQueue<UMessage> q;
		
		public Delivery(int nbClients) {
			deliveryThread = new Thread(this,"TPCC:delivery@"+mySWid);
			this.nbClients = nbClients;
			q = CollectionUtils.newBlockingQueue();
			switch(algType){
				case 0 : wanam.registerLearner("WanAMCastMessage", this);
				case 1 : wanab.registerLearner("WanABCastMessage", this); 
				case 3 :ftwanab.registerLearner("FTWanAMCastMessage", this); 
			}
		}

		@SuppressWarnings("unchecked")
		public void run() {
			long deliverTS;
			ArrayList<Object> message = null;
			boolean isMultigroups;
			byte[] ack = new byte[1];
			DatagramPacket[] ackPacket = new DatagramPacket[this.nbClients];
			DatagramSocket[] sendSocket = new DatagramSocket[this.nbClients];
			InetAddress host = null;
			byte clientID;
			
			try {
				host = InetAddress.getByName("localhost");
				for (int i = 0; i < this.nbClients; i++) {
					sendSocket[i] = new DatagramSocket();
					ackPacket[i] = new DatagramPacket(ack, ack.length, host, 6667 + i);
				}
				
				/*for (int i = 100; i < this.nbClients+100; i++) {
					sendSocket[i] = new DatagramSocket();
					ackPacket[i] = new DatagramPacket(ack, ack.length, host, 6667 + i);
				}*/

			} catch (Exception e) {
				e.printStackTrace();
			}
			
			while (!terminate) {
				isMultigroups=false;
				try {
					message = (ArrayList<Object>)(q.take().serializable);
					
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				
					
				if((Long)message.get(0)==0){
					terminate=true;
				}else{
					// We compute the stats = average latency for the messages i sent
					if((Integer)message.get(2)==mySWid){
						deliverTS = System.currentTimeMillis();
						if( ((HashSet<String>)message.get(1)).size()>1){
							isMultigroups=true;
						}
						myMessagesDel++;
						myMessagesLat += deliverTS-(Long)message.get(0);
						System.out.println(
								"TPCC ( "+mySWid+" ) "+"Latency="
								+(deliverTS-(Long)message.get(0))+" ("+isMultigroups+")");
						
						//client which A-MCasts msg
						clientID = (Byte) message.get(4);
						
						int clientIDint = (int) clientID;
						
						if (clientIDint < 0)
							clientIDint = ((int) clientID) + 256;
						else
							clientIDint = clientID;
							
						System.out.println("Delivered msg from client: " + clientID + " nbClients: " + this.nbClients + " sendSocket: " + sendSocket[clientID] + " packet: " + ackPacket[clientID]);
						
						// Send ack to tell client a new msg can be A-MCast
						try {
							//System.err.println("Sending ack to client: "+ clientIDint);
							sendSocket[clientIDint].send(ackPacket[clientIDint]);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					// We compute the checksum
					totalMessageDel++;
					checksum += ((Long)message.get(0)) % totalMessageDel;
				}
			}

		}

		public void start(){
			deliveryThread.start();
		}

		public void stop(){
			try{
				deliveryThread.join();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void learn(Stream s, Serializable value) {
			q.add((UMessage)value);
		}

	}

}




