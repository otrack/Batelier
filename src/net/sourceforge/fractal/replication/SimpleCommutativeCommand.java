package net.sourceforge.fractal.replication;

import net.sourceforge.fractal.Messageable;
/**   
* @author P. Sutra
* 
*/
public class SimpleCommutativeCommand extends CommutativeCommand {

	private static final long serialVersionUID = Messageable.FRACTAL_MID;
	
	@Deprecated
	public SimpleCommutativeCommand(){};
	
	public SimpleCommutativeCommand(int source){
		super(source,nextUniqueCounterFor(source));
	}

	public boolean commuteWith(CommutativeCommand c){
		return true;
	}
	
}
