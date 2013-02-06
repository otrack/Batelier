package net.sourceforge.fractal.replication;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.sourceforge.fractal.Messageable;

public abstract class CommutativeCommand extends Command{
	/**   
	* @author P. Sutra
	* 
	*/
	private static final long serialVersionUID = Messageable.FRACTAL_MID;
	
	/*
	 * 	By convention if key=0, then the command does not commute with everything except itself.
	 */
	public long key; 
	
	@Deprecated
	protected CommutativeCommand(){}

	protected CommutativeCommand(int source, long k) {
		super(source);
		key=k;
	}
	
	public abstract boolean commuteWith(CommutativeCommand c);
	
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		key = in.readLong();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeLong(key);
	}
	
}
