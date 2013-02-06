package net.sourceforge.fractal;

import java.io.Serializable;

/**   
* @author P. Sutra
* 
*/ 


public abstract class MutedStream extends Stream {

	@Override
	public final void deliver(Serializable s) {
		throw new IllegalArgumentException("Invalid usage");
	}

	@Override
	public final boolean registerLearner(String msgType, Learner learner){
		throw new IllegalArgumentException("Invalid usage");
	}
	
	@Override
	public final boolean unregisterLearner(String msgType, Learner learner){
		throw new IllegalArgumentException("Invalid usage");
	}
	
}
