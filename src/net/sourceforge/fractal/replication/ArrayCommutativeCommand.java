package net.sourceforge.fractal.replication;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import net.sourceforge.fractal.Messageable;
/**   
* @author P. Sutra
* 
*/
public class ArrayCommutativeCommand extends CommutativeCommand implements Iterable<CommutativeCommand> {

	private static final long serialVersionUID = Messageable.FRACTAL_MID;
	
	public ArrayList<CommutativeCommand> list;
	public Integer lastDelivered; // this give a Lamport's clock for GC.
	
	@Deprecated
	public ArrayCommutativeCommand(){}
	
	@SuppressWarnings("deprecation")
	public ArrayCommutativeCommand(int source, ArrayList<CommutativeCommand> l, int last){
		super(source,1);
		list=l;
		lastDelivered = last;
	}

	public Iterator<CommutativeCommand> iterator() {
		return list.iterator();
	}
	
	@Override
	public boolean commuteWith(CommutativeCommand c) {	
		if(super.equals(c)) return true;
		if((c).key == 0) return false;
		if(this.source == c.source && this.lastDelivered != ((ArrayCommutativeCommand)c).lastDelivered ) return false; //small speed-up 
		for(CommutativeCommand a: this ){
			for(CommutativeCommand b: (ArrayCommutativeCommand)c){
				if(!a.commuteWith(b)) return false;
			}
		}
		return true;
		
	}
	
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		source = in.readInt();
		seq = in.readInt();
		key = in.readLong();
		list = (ArrayList<CommutativeCommand>) in.readObject();
		lastDelivered = in.readInt();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(source);
		out.writeInt(seq);
		out.writeLong(key);
		out.writeObject(list);
		out.writeInt(lastDelivered);
	}

	
}

