package net.sourceforge.fractal.membership;

import java.util.Map;

import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.utils.DummyNetwork;
import net.sourceforge.fractal.utils.Node;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class MembershipTest {

	private static Map<Node,Membership> network;
	
	@BeforeClass
	public static void init(){
		network = DummyNetwork.create(3);
		for(Node n : network.keySet()){
			Group g = network.get(n).getOrCreateTCPDynamicGroup("allnodes", 4488);
			g.putNode(n.id, n.ip);
			g.start();
		}
	}

	@Test
	public void testIPDiscovery(){
		Membership m = new Membership();
		m.loadIdenitity(null);
	}
	
	@Test
	public void testTCPDynamicGroup(){
	
		Node n = network.keySet().iterator().next();
		Membership m = network.get(n);

		for(Node p : network.keySet()){
			if( !p.equals(n) )
				Assert.assertTrue(m.addNode(p.id, p.ip));
			else
				Assert.assertFalse(m.addNode(p.id, p.ip));
		}

		for(Node p : network.keySet()){
			if( !p.equals(n) )
				m.group("allnodes").unicast(p.id, new Message());
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}
		
		
		for(Node p : network.keySet()){
			if( p.equals(n) )
				Assert.assertTrue(network.get(p).allNodes().size()==3);
			else
				Assert.assertTrue(p+"=>"+network.get(p).allNodes().toString(),network.get(p).allNodes().size()==2);
		}
		
		for(Node p : network.keySet()){
			if(p.equals(n))
				m.group("allnodes").closeConnections();
		}

	}	
	
	@AfterClass
	public static void cleanup(){
		DummyNetwork.destroy();
	}



}
