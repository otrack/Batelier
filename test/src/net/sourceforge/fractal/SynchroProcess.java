package net.sourceforge.fractal;

import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.broadcast.BroadcastMessage;
import net.sourceforge.fractal.broadcast.BroadcastStream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.CollectionUtils;

/**   
* @author P. Sutra
* 
*/
public class SynchroProcess {

	private static Group all;
	private static BroadcastStream stream;
	private static BlockingQueue q;
	private static MyLearner learner;
	
	static{
		all = FractalManager.getInstance().membership.getOrCreateTCPGroup("SYNCHROPROCESS", 8314);
		for(int swid : FractalManager.getInstance().membership.allNodes()){
			FractalManager.getInstance().membership.addNodeTo(swid, all);
		}
		stream = FractalManager.getInstance().broadcast.getOrCreateBroadcastStream("SYNCHROPROCESS", all.name());
		q = CollectionUtils.newBlockingQueue();
		learner = new MyLearner(q);
		stream.registerLearner("SynchroMessage",learner);
		
		// We must delay to wait that all processes create the group 
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted");
		}
		
	}
	
	public static void synchronise(){
		
		all.start();
		stream.start();
		
		try {

			long start = System.currentTimeMillis();
			
			SynchroMessage m = new SynchroMessage(FractalManager.getInstance().membership.myId());
			if(ConstantPool.MEMBERSHIP_DL>0)
				System.out.println("Starting synchronization");
			for(int i=0;i<all.size();i++){
				if(all.get(i)==FractalManager.getInstance().membership.myId()) stream.broadcast(m);
				((BlockingQueue<Message>) q).take();
			}
			
			if(ConstantPool.MEMBERSHIP_DL>0)
				System.out.println("Synchronization in : " + (System.currentTimeMillis() - start) +" ms");

		} catch (InterruptedException e) {
			System.err.println("Interrupted while waiting for synchronization messages");
		}
				
	}
	
	public static class SynchroMessage extends BroadcastMessage{

		private static final long serialVersionUID = 1L;

		public SynchroMessage(){}
		
		public SynchroMessage(int id) {
			super(null,id);
		}
		
	}
	
}
