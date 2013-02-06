package net.sourceforge.fractal.membership;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

/**
 * An unreliable TCP connection.
 * @author otrack
 *
 */

public class TCPGroupP2PConnection{

	private int remoteId;
	private String remoteHost;

	private String localHost;
	private int localId;
	private SocketChannel sc;

	private boolean isTerminated;
	private BlockingQueue<ByteBuffer> queue;

	private TCPGroup group;
	private Thread p2pThreadR, p2pThreadS;
	
	private TimeRecorder deliverTime, recvTime, sendTime;
	private ValueRecorder sendUsage, recvUsage, recvBatching, sendBatching;

	public TCPGroupP2PConnection(SocketChannel channel, TCPGroup g)	throws TCPConnectionDownException { // incomming connection

		try {
			sc = channel;
			sc.socket().setTcpNoDelay(true);
			sc.configureBlocking(true);
		} catch (Exception e) {
			throw new TCPConnectionDownException(e.getMessage());
		}

		localHost = sc.socket().getLocalAddress().getHostAddress();
		localId = g.membership.myId();

		group = g;
		try {
			ByteBuffer bb = ByteBuffer.allocate(4);
			while(bb.hasRemaining())
				remoteId = sc.read(bb);
			bb.flip();
			remoteId = bb.getInt();
		} catch (Exception e) {
			e.printStackTrace();
			throw new TCPConnectionDownException(e.getMessage());
		}

		remoteHost = sc.socket().getInetAddress().getHostAddress();
		init();

	}

	public TCPGroupP2PConnection(int swid, InetSocketAddress r, TCPGroup g) throws TCPConnectionDownException { // outgoing connection

		try {
			sc = SocketChannel.open();
			sc.configureBlocking(true);
			group = g;
			sc.socket().bind(new InetSocketAddress(g.membership.myIP(),0));
			sc.socket().setTcpNoDelay(true);
		} catch (Exception e) {
			throw new TCPConnectionDownException(e.getMessage());
		}

		localHost = sc.socket().getLocalAddress().getHostAddress();
		localId = g.membership.myId();

		try {
			sc.connect(r);
			ByteBuffer bb = ByteBuffer.allocate(4);
			bb.putInt(g.membership.myId());
			bb.flip();
			while(bb.hasRemaining())
				sc.write(bb);
		} catch (IOException e) {
			throw new TCPConnectionDownException(e.getMessage());
		}

		remoteId = swid;
		remoteHost = r.getAddress().getHostAddress();
		init();

	}

	public synchronized void start() {
		isTerminated = false;
		if(p2pThreadR.getState()==Thread.State.NEW){
			p2pThreadR.start();
			p2pThreadS.start();
		}
	}

	public synchronized void stop() {
		isTerminated = true;
		try {
			sc.close();
			p2pThreadR.interrupt();
			p2pThreadS.interrupt();
		} catch (Exception e) {
			if(ConstantPool.MEMBERSHIP_DL>1)
				System.out.println(this+" error while stopping the connection: "+e.getMessage());
		}
	}

	public void unicast(ByteBuffer buff) throws TCPConnectionDownException {

		if(isTerminated)
			throw new TCPConnectionDownException("TCPGroupP2PConnection is terminated");

		try {
			queue.put(buff);
		} catch (InterruptedException  e) {
			if(ConstantPool.MEMBERSHIP_DL>1)
				System.out.println(this+" (InterruptedException) "+e.getMessage());
			throw new TCPConnectionDownException(e.getMessage());
		}

	}

	public String getRemoteHost(){
		return remoteHost.toString();
	}

	public int getRemoteId(){
		return remoteId;
	}

	public String toString() {
		return group.toString()+" "+localId+"("+localHost+") to "+ remoteId +"("+remoteHost+")";
	}

	//
	// INNER METHODS
	//

	private synchronized void terminate(){
		if(!isTerminated){
			isTerminated=true;
			try {sc.close();} catch (IOException e) {}
			group.unregisterConnection(remoteId, this);
		}
	}

	private void init(){
		p2pThreadR = new Thread(new RecvTask(this), this+":receptionThread");
		p2pThreadS = new Thread(new SendTask(this), this+":emissionThread");
		queue = CollectionUtils.newBlockingQueue();

		if (ConstantPool.MEMBERSHIP_DL > 2 ){
			System.out.println(this+" connected ");
			sendUsage = new ValueRecorder(this+"#sendUsage(Bytes)");
			sendUsage.setFormat("%a");
			recvUsage = new ValueRecorder(this+"#recvUsage(Bytes)");
			recvUsage.setFormat("%a");
			recvTime = new TimeRecorder(this+"#recvTime");
			deliverTime = new TimeRecorder(this+"#deliverTime");
			sendTime = new TimeRecorder(this+"#sendTime");
			recvBatching = new ValueRecorder(this+"#recvBatching");
			recvBatching.setFormat("%a");
			sendBatching  = new ValueRecorder(this+"#sendBatching");
			sendBatching.setFormat("%a");
		}
		
	}

	//
	// INNER CLASSES
	//

	private class SendTask implements Runnable{

		private TCPGroupP2PConnection connection;
		private List<ByteBuffer> toSend;
		private ByteBuffer header, mheader; 
		private int len;

		public SendTask(TCPGroupP2PConnection c) {
			connection = c;
			toSend = new ArrayList<ByteBuffer>();
			header = ByteBuffer.allocateDirect(4+4); 
			mheader = ByteBuffer.allocateDirect(4);
		}

		@Override
		public void run() {

			try{

				int n=0;
				
				while (!isTerminated) {

					toSend.clear();
					toSend.add(queue.take());

					queue.drainTo(toSend);					

					if(ConstantPool.MEMBERSHIP_DL>2){
						sendBatching.add(toSend.size());
						sendTime.start();
					}

					// write header
					header.clear();
					header.putInt(ConstantPool.PROTOCOL_MAGIC);
					header.putInt(toSend.size());
					header.flip();
					while(header.hasRemaining())
						sc.write(header);
									
					if(ConstantPool.MEMBERSHIP_DL>10)
						System.out.println(this+"#"+(++n)+" sending ("+toSend.size()+") messages");
					
					// write data
					for(int i=0;i<toSend.size();i++){
						
						len = toSend.get(i).limit();
						mheader.clear();
						mheader.putInt(len);
						mheader.flip();
						while(mheader.hasRemaining())
							sc.write(mheader);
						
						toSend.get(i).rewind();
						while(toSend.get(i).hasRemaining())
							sc.write(toSend.get(i));
						
						if(ConstantPool.MEMBERSHIP_DL>2)
							sendUsage.add(len);
						
						if(ConstantPool.MEMBERSHIP_DL>10)
							System.out.println(this+" send a "+len+" message");
						
					}		

					if(ConstantPool.MEMBERSHIP_DL>2)
						sendTime.stop();

				} // while(!terminated)

			} catch(ClosedChannelException e){
				if(ConstantPool.MEMBERSHIP_DL>1)
					System.out.println(this+" ClosedChannelException");
			} catch (IOException e) {
				if(ConstantPool.MEMBERSHIP_DL>1)
					System.out.println(this+" (IOException) "+e.getMessage());
			} catch (IllegalArgumentException e) {
				if(ConstantPool.MEMBERSHIP_DL>1)
					System.out.println(this+" ProtocolMagicException ");
			} catch (BufferUnderflowException e){
				if(ConstantPool.MEMBERSHIP_DL>1)
					System.out.println(this+" BufferUnderflowException");
			} catch (InterruptedException e) {
				if(ConstantPool.MEMBERSHIP_DL>1)
					System.out.println(this+" InterruptedException");
			} finally {
				terminate();
			}

		}
		
		@Override
		public String toString(){
			return connection.toString();
		}
		
	}

	private class RecvTask implements Runnable{

		private TCPGroupP2PConnection connection;

		private ByteBuffer[] toRecv;
		private ByteBuffer header, mheader, data; 
		private int read=-1, size, len;

		public RecvTask(TCPGroupP2PConnection c){
			connection = c;
			header = ByteBuffer.allocateDirect(4+4);
			mheader = ByteBuffer.allocateDirect(4);
		}

		public void run() {

			try{

				int n=0;
				while (!isTerminated) {
					
					header.clear();
					do{
						read = sc.read(header);
					}while(header.hasRemaining() && read!=-1);
					if(read==-1) break;
					header.flip();
					TCPGroup.validateMagic(header.getInt());
					size = header.getInt();

					if(ConstantPool.MEMBERSHIP_DL>10)
						System.out.println(this+"#"+(++n)+" receiving ("+size+") messages");
					
					if(ConstantPool.MEMBERSHIP_DL>2){
						recvTime.start();
						recvBatching.add(size);
					}
					
					toRecv = new ByteBuffer[size];
					for(int i=0;i<size;i++){
						
						mheader.clear();
						do{
							read = sc.read(mheader);
						}while(mheader.hasRemaining() && read!=-1);
						if(read==-1) break;
						mheader.flip();
						len = mheader.getInt();

						data= ByteBuffer.allocate(len);
						do{
							read = sc.read(data);
						}while(data.hasRemaining() && read!=-1);
						if(read==-1) break;
						
						toRecv[i]=data;
						
						if(ConstantPool.MEMBERSHIP_DL>2)
							recvUsage.add(len);
						
						if(ConstantPool.MEMBERSHIP_DL>10)
							System.out.println(this+" received a "+len+" message");
						
					}

					if(ConstantPool.MEMBERSHIP_DL>2)
						deliverTime.start();
					group.deliver(toRecv,connection);
					if(ConstantPool.MEMBERSHIP_DL>2)
						deliverTime.stop();
					
					if(ConstantPool.MEMBERSHIP_DL>2)
						recvTime.stop();

				}

			} catch(ClosedChannelException e){
				if(ConstantPool.MEMBERSHIP_DL>1)
					System.out.println(this+" ClosedChannelException");
			} catch (IOException e) {
				if(ConstantPool.MEMBERSHIP_DL>1)
					System.out.println(this+" (IOException) "+e.getMessage());
			} catch (IllegalArgumentException e) {
				if(ConstantPool.MEMBERSHIP_DL>1)
					System.out.println(this+" ProtocolMagicException");
			} catch (BufferUnderflowException e){
				if(ConstantPool.MEMBERSHIP_DL>1)
					System.out.println(this+" BufferUnderflowException");
			} catch (InterruptedException e) {
				if(ConstantPool.MEMBERSHIP_DL>1)
					System.out.println(this+" InterruptedException");
			} finally {
				terminate();
			}

		}

		@Override
		public String toString(){
			return connection.toString();
		}
		
	}

}
