package net.sourceforge.fractal.replication;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.sourceforge.fractal.Messageable;

/**   
* @author P. Sutra
* 
*/
public final class CheckpointCommand extends CommutativeCommand {

	private static final long serialVersionUID = Messageable.FRACTAL_MID;
	
	private int checkpointNumber;
	
	@Deprecated
	public CheckpointCommand(){}
	
	public CheckpointCommand(int n){
		super(0,0);
		checkpointNumber=n;
	}
	
	@Override
	public String toString(){
		return "CHK#"+checkpointNumber;
	}
	
	public int checkpointNumber(){
		return checkpointNumber;
	}

	@Override
	public boolean commuteWith(CommutativeCommand c){
		if(equals(c)) return true;
		if(key==0 || ((CommutativeCommand)c).key == 0) return false;
		return key != ((CommutativeCommand)c).key;
	}

	
	public boolean equals(Object o){
		if(super.equals(o)) return true;
		if(o instanceof CheckpointCommand && checkpointNumber == ((CheckpointCommand)o).checkpointNumber) return true;
		return false;
	}
	
	@Override
	public int hashCode(){
		return checkpointNumber; 
	}
	
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		source = in.readInt();
		seq = in.readInt();
		key = in.readLong();
		checkpointNumber = in.readInt();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(source);
		out.writeInt(seq);
		out.writeLong(key);
		out.writeInt(checkpointNumber);
	}
	
}
