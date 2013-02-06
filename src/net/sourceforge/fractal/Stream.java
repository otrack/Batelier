package net.sourceforge.fractal;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**   
* @author L. Camargos
* @author P. Sutra
* 
*/ 


public abstract class Stream {

	protected Map<String, Set<Learner>> learners;
	
	public Stream(){
		learners = new HashMap<String,Set<Learner>>();
	}
	
	public boolean registerLearner(String msgType, Learner learner){
		if(learners.get(msgType)==null)
			learners.put(msgType,new HashSet<Learner>());
		return learners.get(msgType).add(learner);
	}
	
	public boolean unregisterLearner(String msgType, Learner learner){
		return learners.get(msgType).remove(learner);
	}
	
	public abstract void deliver(Serializable s);
	
	public abstract void start();
	
	public abstract void stop();
	
}
