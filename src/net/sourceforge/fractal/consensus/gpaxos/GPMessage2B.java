package net.sourceforge.fractal.consensus.gpaxos;

import net.sourceforge.fractal.Messageable;

/**   
* @author P. Sutra
* 
*/ 


public abstract class GPMessage2B extends GPMessage {

	private static final long serialVersionUID = Messageable.FRACTAL_MID;

	public GPMessage2B(){}

	public GPMessage2B(int source, int ballot){
		super(source, ballot);
	}
	
}
