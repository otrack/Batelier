package net.sourceforge.fractal.consensus.gpaxos.cstruct;

public class IncompatibleCStructException extends Exception {
	
	/**   
	* @author P. Sutra
	* 
	*/ 

	
	
	private static final long serialVersionUID = 1L;
	
	public IncompatibleCStructException(){
		super("Incompatible cstruct set");
	}
}
