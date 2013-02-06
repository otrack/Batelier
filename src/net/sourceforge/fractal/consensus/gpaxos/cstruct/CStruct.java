package net.sourceforge.fractal.consensus.gpaxos.cstruct;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;

import net.sourceforge.fractal.Messageable;
import net.sourceforge.fractal.replication.CheckpointCommand;
import net.sourceforge.fractal.replication.Command;

/**
 * 
 * @author Pierre Sutra
 *
 * Notes:
 * 
 * The iterator() must return the commands contained in the cstruct
 * in causal order, i.e. 
 * if append(c1) \happensBefore append(c2), then c1 appears before c2 in the iterator.
 * 
 * If u and v have the same chk number, then their glb must contain it.
 * 
 * If u and v are compatible and have the same chk number, then their lub must contain it.
 * 
 */

public abstract class CStruct extends AbstractCollection<Command> implements Messageable, Comparable<CStruct>, Cloneable {

	private static final long serialVersionUID = Messageable.FRACTAL_MID;
	
	private int checkpointNumber;
		
	protected CStruct(){
		checkpointNumber = 0;
	}
	
	//
	// Class's interface
	//
		
	public static CStruct glb(Collection<CStruct> collection) throws EmptyCStructSetException {
		throw new RuntimeException("NYI");
	}
	
	public static CStruct lub(Collection<CStruct> collection) throws IncompatibleCStructException, EmptyCStructSetException{
		throw new RuntimeException("NYI");
	}
	
	public static boolean isCompatible(Collection<CStruct> collection){
		throw new RuntimeException("NYI");
	}
	
	/**
	 * 
	 * @param u
	 * @param v
	 * @return the left least upper bound of two c-structs u and v
	 * 	                 is the least upper bound of the set composed of u and the greatest prefix of v compatible with u.  
	 */
	public static CStruct leftLubOf(CStruct u, CStruct v) throws ClassCastException {
		throw new RuntimeException("NYI");
	}

	//
	// Object's interface
	//
	
	protected abstract boolean append(Command c);
	protected abstract boolean remove(Command c);
	protected abstract boolean contains(Command c);
	public abstract int size(); 
	public abstract void clear();
	public abstract boolean removeAll(CStruct u);
	public abstract boolean removeAll(Collection<?> collection);
	
	public final boolean isPrefixOf(CStruct u) throws ClassCastException{
		int ret = this.compareTo(u);
		return  ret == -1 || ret == 0 ;
	}
	
	public final boolean isStrictPrefixOf(CStruct u) throws ClassCastException {
		return this.compareTo(u) == -1;
	}
	
	public final boolean appendAll(Collection<Command> s){
		boolean ret = false;
		for(Command c : s){
			if(c instanceof CheckpointCommand)
				ret |= append((CheckpointCommand)c);
			else
				ret |= append(c); 
		}
		return ret;
	}
	
	@SuppressWarnings("unchecked")
	public final boolean containsAll(Collection<?> collection) {
		for(Command c : (Collection<Command>)collection){
			if(c instanceof CheckpointCommand){
				if(((CheckpointCommand)c).checkpointNumber()<checkpointNumber) return false;
			}
			if(!contains(c)) return false;	
		}
		return true;
	}
	
	public final ArrayList<Command> retainsAll(Collection<Command> collection) {
		ArrayList<Command> ret = new ArrayList<Command>();
		for(Command c : collection){
			if( 
				( !(c instanceof CheckpointCommand) || ((CheckpointCommand)c).checkpointNumber()>checkpointNumber)
				&&
				! contains(c)
			  )
			  ret.add(c);
		}
		return ret;
	}
	
	protected final boolean append(CheckpointCommand c){
		if( c.checkpointNumber()<=checkpointNumber ) return false;
		if( !append((Command)c)) return false; 
		if(checkpointNumber<c.checkpointNumber()){
			checkpointNumber=c.checkpointNumber();
		}
		return true;
	}
	
	public final int checkpointNumber(){
		return checkpointNumber;
	}

	public LinkedHashSet<Command> checkpoint(){
		if(!isCheckpointable()) return null;

		if(checkpointNumber>1){
			LinkedHashSet<Command> checkpoint = new LinkedHashSet<Command>(this.size()/2);
			for(Command d : this){
				checkpoint.add(d);
				if(d instanceof CheckpointCommand){
					assert ((CheckpointCommand)d).checkpointNumber()==checkpointNumber-1 : this;
					break;	
				}
			}
			removeAll(checkpoint);
			return checkpoint;
		}
		
		return null;
	}
	
	public boolean isCheckpointable(){
		return checkpointNumber > 1 && contains(new CheckpointCommand(checkpointNumber-1));
	}
	
	public boolean isEmpty() {
		return size()==0;
	}
	
	// Others
	
	@Override
	protected Object clone() {
		CStruct u;
		try {
			u = (CStruct)super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException("Impossible to reach this state");
		}
		u.checkpointNumber = checkpointNumber;
		return u;
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		checkpointNumber = in.readInt();
	}
	
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(checkpointNumber);
	}
	
	public String toString(){
		return checkpointNumber+"#"+(isCheckpointable() ? "C" : "");
	}

	
}
