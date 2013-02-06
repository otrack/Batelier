package net.sourceforge.fractal.consensus.gpaxos;

import net.sourceforge.fractal.Messageable;

/**   
* @author P. Sutra
* 
*/ 


public abstract class GPMessage2A  extends GPMessage {
	
	private static final long serialVersionUID = Messageable.FRACTAL_MID;
	
	public GPMessage2A(){}
	
	public GPMessage2A(int source, int ballot){
		super(source, ballot);
	}

}
