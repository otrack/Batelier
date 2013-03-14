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
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.utils.DummyNetwork;
import net.sourceforge.fractal.utils.ExecutorPool;
import net.sourceforge.fractal.utils.Node;
import net.sourceforge.fractal.utils.PerformanceProbe.FloatValueRecorder;
import net.sourceforge.fractal.wanamcast.WanAMCastMessage;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class WanAMCastTest {

	private static final int nnodes=9;
	private static final int ngroups=3;
	private static final int nmessagesPerNode=5000;
	
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
			WanAMCastStream amstream = new WanAMCastStream(n.id, g, "amcast"+n.id, multicast);
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
		
		for(Node n: network.keySet()){
			assert streams.get(n).isClean() : ""+streams.get(n).detailedInformation();
		}
		
	}
	

	@AfterClass
	public static void cleanup(){
		DummyNetwork.destroy();
	}
	
	public static void main(String args[]){
		init();
		WanAMCastTest test = new WanAMCastTest();
		try {
			test.simpleAtomicMulticastTest();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cleanup();
		System.exit(0);
	}
	
	private class AMCastTestJob implements Callable<Integer>{

		private Node n;
		private FloatValueRecorder amcastTime;
		
		public AMCastTestJob(Node node){
			n = node;
			amcastTime = new FloatValueRecorder(this+"#amcastTime(ms)");
			amcastTime.setFormat("%a");
		}
		
		@Override
		public Integer call() throws Exception {
			
			Random rnd = new Random();
			long start;
			int missingBarriers=nnodes;
			
			for(int k=1; k<=nmessagesPerNode; k++){

				if((nmessagesPerNode-k)%500==0)
					System.out.println(this+", still "+(nmessagesPerNode-k)+" messages");

				// Build some random (biased) recipient groups
				// The last message acts as a barrier
				Set<String> dst = new HashSet<String>(ngroups-1);
				WanAMCastMessage msg = null;
				if(k<nmessagesPerNode){
					int ngs =  rnd.nextInt(ngroups) + 1;
					for(int i=0; i<=ngs ; i++){
						Integer g = rnd.nextInt(ngroups);
						dst.add((network.get(n).allGroups().toArray(new Group[ngroups])[g]).name());
					}
					msg = new WanAMCastMessage(new Byte[100],dst,network.get(n).groupsOf(n.id).iterator().next().name(),n.id);
				}else{
					dst.addAll(network.get(n).allGroupNames());
					msg = new WanAMCastMessage(null,dst,network.get(n).groupsOf(n.id).iterator().next().name(),n.id);
				}

				// Multicast the message
				start=System.currentTimeMillis();
				streams.get(n).atomicMulticast(msg);

				// Wait it is received
				if(dst.contains(network.get(n).groupsOf(n.id).iterator().next().name())){
					WanAMCastMessage m = null;
					do{
						m = learners.get(n.id).q.take();
						if(m.serializable==null) missingBarriers--;
					} while(!m.equals(msg));
					amcastTime.add(System.currentTimeMillis()-start);
				}	
					
			}
						
			System.out.println(this+", over, grabbing missing ("+missingBarriers+") barrier(s) now");
			
			while(missingBarriers!=0){
				WanAMCastMessage m = learners.get(n.id).q.take();
				if(m.serializable==null){
					missingBarriers--;
				}
			}
			
			return 0;
			
		}
		
		@Override
		public String toString(){
			return "@"+n.id;
		}

	}
	
}
