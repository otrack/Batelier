package net.sourceforge.fractal.replication;

import net.sourceforge.fractal.replication.Command;
/**   
* @author P. Sutra
* 
*/
public class SimpleCommand extends Command{

	private static final long serialVersionUID = 1L;

	public SimpleCommand(){}
	
	public SimpleCommand(int source) {
		super(source);
	}
	
}
