package net.sourceforge.fractal.amcast;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import net.sourceforge.fractal.MyLearner;
import net.sourceforge.fractal.consensus.paxos.PaxosStream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.utils.DummyNetwork;
import net.sourceforge.fractal.utils.ExecutorPool;
import net.sourceforge.fractal.utils.Node;
import net.sourceforge.fractal.wanamcast.WanAMCastMessage;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class WanAMCastTest {

	private static final int nnodes=20;
	private static final int ngroups=20;
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
			WanAMCastStream amstream = new WanAMCastStream(n.id, g, "amcast"+n.id, multicast,paxos);
			streams.put(n,amstream);
			MyLearner<WanAMCastMessage> l = new MyLearner<WanAMCastMessage>(new LinkedBlockingQueue<WanAMCastMessage>());
			learners.put(n.id, l);
			amstream.registerLearner("WanAMCastMessage", learners.get(n.id));
			amstream.start();
		}
	}

	@Test
	public void simpleAtomicMulticastTest() throws InterruptedException{
		
		List<Future<Integer>> l = new ArrayList<Future<Integer>>();
		ExecutorPool pool = ExecutorPool.getInstance();

		for(Node n: network.keySet()){
			AMCastTestJob j = new AMCastTestJob(n);
			l.add(pool.submit(j));
		}
		
		for(Future<Integer> f : l){
			try {
				f.get();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
	}
	

	@AfterClass
	public static void cleanup(){
		DummyNetwork.destroy();
	}
	
	private class AMCastTestJob implements Callable<Integer>{

		private Node n;
		
		public AMCastTestJob(Node node){
			n = node;
		}
		
		@Override
		public Integer call() throws Exception {

			Random rnd = new Random();
			
			for(int k=0; k<nmessagesPerNode; k++){

				System.out.println(this+", still "+(nmessagesPerNode-k));

				// Build some random (biased) recipient groups
				Set<String> dst = new HashSet<String>(ngroups-1);
				int ngs =  rnd.nextInt(ngroups) + 1;
				for(int i=0; i<=ngs ; i++){
					Integer g = rnd.nextInt(ngroups);
					dst.add((network.get(n).allGroups().toArray(new Group[ngroups])[g]).name());
				}

				// Multicast the message
				WanAMCastMessage msg = new WanAMCastMessage(new Byte[50],dst,network.get(n).groupsOf(n.id).iterator().next().name(),n.id);
				System.out.println("Sending message "+msg);
				streams.get(n).atomicMulticast(msg);

				// Wait it is received
				if(dst.contains(network.get(n).groupsOf(n.id).iterator().next().name())){
					WanAMCastMessage m = null;
					do{
						m = learners.get(n.id).q.take();
					} while(!m.equals(msg));
				}else{
					learners.get(n.id).q.clear();
				}	
					
			}

			System.out.println(this+", over");
			
			return 0;
			
		}
		
		@Override
		public String toString(){
			return n.toString();
		}

	}
	
}
