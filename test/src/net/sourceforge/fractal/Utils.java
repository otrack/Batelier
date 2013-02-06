package net.sourceforge.fractal;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.multicast.MulticastMessage;
/**   
* @author P. Sutra
* 
*/
public class Utils {

	static class MyRMCastLearner implements Learner{

		BlockingQueue<MulticastMessage> q;

		public MyRMCastLearner(BlockingQueue<MulticastMessage> q){
			this.q = q;
		}

		public void learn(Stream s, Serializable value) {
			MulticastMessage m = (MulticastMessage) value;
			try {
				q.put(m);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
}
