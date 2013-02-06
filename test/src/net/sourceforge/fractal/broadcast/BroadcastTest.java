package net.sourceforge.fractal.broadcast;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import net.sourceforge.fractal.MyLearner;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.utils.DummyNetwork;
import net.sourceforge.fractal.utils.Node;
import net.sourceforge.fractal.utils.Progressbar;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BroadcastTest {

	private static final int nnodes=6;
	
	private static Map<Node,Membership> network;
	private static Map<Node,BroadcastStream> streams;
	private static Map<Node,MyLearner<BroadcastMessage>> learners;
	
	@BeforeClass
	public static void init(){
		network = DummyNetwork.create(nnodes);
		streams = new HashMap<Node, BroadcastStream>();
		learners = new HashMap<Node, MyLearner<BroadcastMessage>>();
		for(int j=0;j<100;j++){
			for(Node n : network.keySet()){
				Group g = network.get(n).getOrCreateTCPDynamicGroup("allnodes", 4488);
				for(Node m: network.keySet()){
					g.putNode(m.id, m.ip);
				}
				g.start();
				MyLearner<BroadcastMessage> l = new MyLearner<BroadcastMessage>(new LinkedBlockingQueue<BroadcastMessage>());
				learners.put(n, l);
				BroadcastStream stream = new BroadcastStream("rbcast", g, n.id);
				stream.registerLearner("BroadcastMessage", l);
				streams.put(n,stream);
				stream.start();
			}
		}
	}

	@Test
	public void testReliableBroadcast() throws InterruptedException{
		Progressbar pg = new Progressbar(100, "progress");
		for(int j=0;j<100;j++){
			for(Node n : network.keySet()){
				BroadcastMessage msg = new BroadcastMessage(null,n.id);
				streams.get(n).broadcast(msg);
				for(Node p : network.keySet()){
					Assert.assertTrue(msg.equals(learners.get(p).q.take()));
					System.out.print("-");
				}
			}
			pg.setVal(j);
		}
		pg.finish();
	}
	

	@AfterClass
	public static void cleanup(){
		DummyNetwork.destroy();
	}
	
	
}
