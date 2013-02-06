package net.sourceforge.fractal.consensus.gpaxos.cstruct;

public class EmptyCStructSetException extends Exception{

	/**   
	* @author P. Sutra
	* 
	*/ 

	
	private static final long serialVersionUID = 1L;

	public EmptyCStructSetException(){
		super("Empty cstruct set");
	}
	
}
