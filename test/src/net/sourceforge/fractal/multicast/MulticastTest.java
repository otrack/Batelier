package net.sourceforge.fractal.multicast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import net.sourceforge.fractal.MyLearner;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.utils.DummyNetwork;
import net.sourceforge.fractal.utils.Node;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MulticastTest {
	
	private static final int nnodes=2;
	private static final int ngroups=2;

	private static Map<Node,Membership> network;
	private static Map<Node,MulticastStream> streams;
	private static Map<Integer,MyLearner<MulticastMessage>> learners;
	
	@BeforeClass
	public static void init(){
		network = DummyNetwork.create(nnodes,ngroups);
		streams = new HashMap<Node, MulticastStream>();
		learners = new HashMap<Integer, MyLearner<MulticastMessage>>();
		
		for(Node n : network.keySet()){
			Group g = network.get(n).groupsOf(n.id).iterator().next();
			MyLearner<MulticastMessage> l = new MyLearner<MulticastMessage>(new LinkedBlockingQueue<MulticastMessage>());
			learners.put(n.id, l);
			MulticastStream stream = new MulticastStream("rmcast",g,network.get(n));
			stream.registerLearner("MulticastMessage", l);
			streams.put(n,stream);
			stream.start();
		}
	}
	
	@Test
	public void testMulticastStream(){
		
		for(int j=0;j<1000;j++){
			for(Node n: network.keySet()){

				// Build some random recipient groups
				Collection<String> dst = new ArrayList<String>();
				Random rnd = new Random();
				for(int i=0; i<Math.max(1,rnd.nextInt(ngroups)); i++){
					dst.add((network.get(n).allGroups().toArray(new Group[ngroups])[i]).name());
				}

				// Multicast the message
				MulticastMessage msg = new MulticastMessage(null,dst,network.get(n).groupsOf(n.id).iterator().next().name(),n.id);
				streams.get(n).multicast(msg);

				// Check it was received
				for(String name: dst){
					Group g = network.get(n).group(name);
					for(int swid: g.allNodes()){	
						MulticastMessage m = null; 
						try {
							m = learners.get(swid).q.take();
						} catch (InterruptedException e) {
							// unreachable code
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
