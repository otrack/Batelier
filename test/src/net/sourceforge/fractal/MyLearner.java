package net.sourceforge.fractal;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

/**   
* @author L. Camargos
* @author P. Sutra
* 
*/

public class MyLearner<E extends Message> implements Learner{

	public BlockingQueue<E> q;

	public MyLearner(BlockingQueue<E> queue){
		q = queue;
	}
		
	@SuppressWarnings("unchecked")
	public void learn(Stream s, Serializable m) {
		if(ConstantPool.DUMMY_NET>1)
			System.out.println("I receive a "+((UMessage)m).getMessageType() +" message from "+((UMessage)m).source);
		try {
			q.put((E) m);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
		
}
