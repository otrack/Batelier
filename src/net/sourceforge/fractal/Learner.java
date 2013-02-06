package net.sourceforge.fractal;

import java.io.Serializable;

/**   
* @author L. Camargos
* @author P. Sutra
* 
*/ 


public interface Learner{
	
	public abstract void learn(Stream s, Serializable value);
	
}
