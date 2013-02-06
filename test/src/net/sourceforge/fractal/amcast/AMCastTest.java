package net.sourceforge.fractal.amcast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import net.sourceforge.fractal.MyLearner;
import net.sourceforge.fractal.consensus.paxos.PaxosStream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.utils.DummyNetwork;
import net.sourceforge.fractal.utils.Node;
import net.sourceforge.fractal.wanamcast.WanAMCastMessage;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AMCastTest {

	private static final int nnodes=3;
	private static final int ngroups=1;
	private static final int nmessagesPerNode=1000;
	
	private static Map<Node,Membership> network;
	private static Map<Node,WanAMCastStream> streams;
	private static Map<Integer,MyLearner<WanAMCastMessage>> learners;
	
	@BeforeClass
	public static void init(){
		network = DummyNetwork.create(nnodes,ngroups);
		streams = new HashMap<Node, WanAMCastStream>();
		learners = new HashMap<Integer, MyLearner<WanAMCastMessage>>();
		for(Node n : network.keySet()){
			Group g = network.get(n).groupsOf(n.id).iterator().next();
			MulticastStream multicast = new MulticastStream("rmcast"+n.id,g,network.get(n));
			PaxosStream paxos = new PaxosStream("paxos",n.id,"LEADER_CONSTANT",g.name(),g.name(),g.name(),network.get(n));
			WanAMCastStream amstream = new WanAMCastStream(n.id, g, "amcast"+n.id, multicast,paxos, false);
			streams.put(n,amstream);
			MyLearner<WanAMCastMessage> l = new MyLearner<WanAMCastMessage>(new LinkedBlockingQueue<WanAMCastMessage>());
			learners.put(n.id, l);
			amstream.registerLearner("WanAMCastMessage", learners.get(n.id));
			amstream.start();
		}
	}

	@Test
	public void simpleAtomicMulticastTest() throws InterruptedException{
		
		for(int k=0; k<nmessagesPerNode; k++){
			for(Node n: network.keySet()){

				// Build some random recipient groups
				Collection<String> dst = new ArrayList<String>();
				Random rnd = new Random();
				for(int i=0; i<Math.max(1,rnd.nextInt(ngroups)); i++){
					dst.add((network.get(n).allGroups().toArray(new Group[ngroups])[i]).name());
				}

				// Multicast the message
				WanAMCastMessage msg = new WanAMCastMessage(new Byte[100000],dst,network.get(n).groupsOf(n.id).iterator().next().name(),n.id);
				System.out.println("Sending message "+msg);
				streams.get(n).atomicMulticast(msg);
				
				// Check it was received
				for(String name: dst){
					Group g = network.get(n).group(name);
					for(int swid: g.members()){	
						WanAMCastMessage m = null; 
						try {
							m = learners.get(swid).q.take();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						Assert.assertTrue(m.equals(msg));
					}
				}
				
			}
		}
	}
	

	@AfterClass
	public static void cleanup(){
		DummyNetwork.destroy();
	}
}
