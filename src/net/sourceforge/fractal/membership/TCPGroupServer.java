package net.sourceforge.fractal.membership;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import net.sourceforge.fractal.ConstantPool;

/**   
* @author P. Sutra
* 
*/
class TCPGroupServer implements Runnable{

		private ServerSocketChannel ssc;
		private boolean terminate;
		private TCPGroup group;
		private Thread serverThread;

		// FIXME handle exception
		public TCPGroupServer(TCPGroup group) {
			this.group = group;
			serverThread = new Thread(this,"TCPGroupServer");
			try{
				ssc = ServerSocketChannel.open();
				ssc.configureBlocking(true);
				if(ConstantPool.TEST_DL==1)
					ssc.socket().bind(new InetSocketAddress(group.membership.myIP(),group.port));
				else
					ssc.socket().bind(new InetSocketAddress("0.0.0.0",group.port));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void start(){
			terminate = false;
			serverThread = new Thread(this);
			serverThread.start();
		}
		
		public void stop(){
			terminate = true;
			try{
				ssc.close();
				serverThread.interrupt();
				serverThread.join();
			}catch (Exception e) {
				if(ConstantPool.MEMBERSHIP_DL>1)
					System.out.println(this+" error while stopping server: "+e.getMessage());
			}
		}
		
		public void run() {
			TCPGroupP2PConnection clientConnection;
			SocketChannel sc;
			InetAddress clientAddress=null;
			
			while(!terminate){
				try {
					sc = ssc.accept();
					clientAddress = sc.socket().getInetAddress();
					clientConnection = new TCPGroupP2PConnection(sc,group);
					group.registerConnection(clientConnection.getRemoteId(), clientConnection);
					clientConnection.start();
				} catch (IOException e) {
					try {
						ssc.close();
					} catch (IOException e1) {
					}
				} catch (TCPConnectionDownException e) {
					if (!terminate && ConstantPool.MEMBERSHIP_DL > 1 ) 
						System.out.println(this+" connection to"+ clientAddress + " lost; reason: "+e.getMessage());
				}

			}
		}
		
		public String toString(){
			return group.toString();
		}
}
