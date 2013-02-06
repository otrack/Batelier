//
//  Client.java
//  
//
//  Created by Nicolas Schiper on 03.11.08.
//

package net.sourceforge.fractal;

/**   
* @author L. Camargos
* 
*/


import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Random;

public class Client extends Thread {
	
	int nbGroups;
	int myGroup;
	int rate;
	float rho;
	int id;
	
	public Client(int id, int nbGroups, String myIP, int rate, float rho) {
		this.id = id;
		this.nbGroups = nbGroups;
		
		if (myIP.equals("node2"))
			this.myGroup = 0;
		else if (myIP.equals("node3"))
			this.myGroup = 0;
		else if (myIP.equals("node4"))
			this.myGroup = 0;
		else if (myIP.equals("node5"))
			this.myGroup = 1;
		else if (myIP.equals("node6"))
			this.myGroup = 1;
		else if (myIP.equals("node7"))
			this.myGroup = 1;
		else if (myIP.equals("node9"))
			this.myGroup = 2;
		else if (myIP.equals("node10"))
			this.myGroup = 2;
		else if (myIP.equals("node11"))
			this.myGroup = 2;
		else if (myIP.equals("node12"))
			this.myGroup = 3;
		else if (myIP.equals("node13"))
			this.myGroup = 3;
		else if (myIP.equals("node14"))
			this.myGroup = 3;
		else if (myIP.equals("node15"))
			this.myGroup = 4;
		else if (myIP.equals("node16"))
			this.myGroup = 4;
		else if (myIP.equals("node17"))
			this.myGroup = 4;
		else if (myIP.equals("node18"))
			this.myGroup = 5;
		else if (myIP.equals("node19"))
			this.myGroup = 5;
		else if (myIP.equals("node20"))
			this.myGroup = 5;
		else if (myIP.equals("node21"))
			this.myGroup = 6;
		else if (myIP.equals("node22"))
			this.myGroup = 6;
		else if (myIP.equals("node23"))
			this.myGroup = 6;
		else if (myIP.equals("node24"))
			this.myGroup = 7;
		else if (myIP.equals("node25"))
			this.myGroup = 7;
		else if (myIP.equals("node26"))
			this.myGroup = 7;
		this.rate = rate;
		this.rho = rho;
	}
	
	public void run() {
	
		int msgType;
		float nb;		
		Random rand = new Random();
		DatagramSocket sendSocket = null, rcvSocket = null;
		byte[] msg = new byte[12];
		byte[] ack = new byte[1];
		InetAddress host = null;
		DatagramPacket packet, ackPacket;
		
		ackPacket = new DatagramPacket(ack, ack.length);
		
		//wait until server is started
		try {
			Thread.sleep(60000);
			host = InetAddress.getByName("localhost");
			sendSocket = new DatagramSocket();
			rcvSocket = new DatagramSocket(6667 + this.id);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		while (true) {
		
			for (int i = 0; i < msg.length; i++)
				msg[i] = 0;
				
			nb = rand.nextFloat();

			// TPCC
			/* NewOrder */
			if (nb <= 0.45)
				msgType = 1; 
			/* Payment */
			else if ((0.45 < nb) && (nb <= 0.88))
				msgType = 2;
			/* Delivery */
			else if ((0.88 < nb) && (nb <= 0.92))
				msgType = 3;
			/* Stock_level */
			else if ((0.92 < nb) && (nb <= 0.96))
				msgType = 4;
			/* Order_status */
			else
				msgType = 5;

			switch(msgType) {
			case 1 :
				for (int j = 0; j < 10; j++) {
					if (rand.nextFloat() <= (this.rho/15.0)) {
						/* randomly select a remote group */
						int other;
						do{
							other = rand.nextInt(this.nbGroups);
						}
						while( other == this.myGroup );
						msg[other] = 1;
					}
				}
				msg[10] = 1;
				break;
			case 2 :	
				if (rand.nextFloat() <= this.rho) {
					/* randomly select a remote group */
					int other;
					do{
						other = rand.nextInt(this.nbGroups);
					}
					while( other == this.myGroup );
					msg[other] = 1;
				}
				msg[10] = 2;
				break;
			case 3 :
				msg[10] = 3;
				break;
			case 4 :
				msg[10] = 4; 
				break;
			case 5 :
				msg[10] = 5;
				break;
			default:
				msg[10] = 0;
			}
			msg[this.myGroup] = 1;
			msg[11] = (new Integer(this.id)).byteValue();
					
			try {
				/*System.out.print("sent: ");
				for (int i = 0; i < msg.length; i++)
					System.out.print(msg[i]);
				System.out.println("");*/
					
				packet = new DatagramPacket(msg, msg.length, host, 6666);
				sendSocket.send(packet);
				
				/*
				if (this.rate > 1000){				
					Thread.sleep(0, 1000000000/this.rate);
				}else{
					double millis = 1000.0/((double) this.rate);
					int remainder = 1000 % this.rate;
					Thread.sleep(((long) Math.floor(millis)), (remainder / this.rate) * 1000000);
				}*/
				
				// Wait until a msg was A-Delivered
				rcvSocket.receive(ackPacket);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {

		int nbClients, nbGroups, rate, instance;
		String myGroup;
		
		float rho;
		
		if (args.length != 5)
			System.err.println("Syntax: java Client nbClients nbGroups myGroup rho rate");
		
		else {
			nbClients = Integer.parseInt(args[0]);
			nbGroups = Integer.parseInt(args[1]);
			myGroup = args[2];
			rho = Float.parseFloat(args[3]);
			rate = Integer.parseInt(args[4]);
			//instance = Integer.parseInt(args[5]);
			
			//System.out.println(nbClients + " client(s) launched, clients from group: " + myGroup + " with " + nbGroups + " groups, and rho: " + rho + " rate: " + rate);
			
			Client[] clients = new Client[nbClients];
			
			try {								
				for (int i = 0; i < nbClients; i++) {
					//if (instance == 1)
						clients[i] = new Client(i, nbGroups, myGroup, rate, rho);
					//else
					//	clients[i] = new Client(i+100, nbGroups, myIP, rate, rho);
					clients[i].start();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
}
