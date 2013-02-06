package net.sourceforge.fractal.consensus.gpaxos.cstruct;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;

import net.sourceforge.fractal.Messageable;
import net.sourceforge.fractal.replication.Command;

/**   
* @author P. Sutra
* 
*/ 


public class CSet extends CStruct {

	private static final long serialVersionUID = Messageable.FRACTAL_MID;
	
	private LinkedHashSet<Command> set;
	
	public CSet(){
		set = new LinkedHashSet<Command>();
	}
	
	public static CStruct lub(Collection<CSet> collection) throws EmptyCStructSetException{

		if(collection.isEmpty()) throw new EmptyCStructSetException();
		
		if(collection.size()==1) return (CStruct) collection.iterator().next().clone();

		CSet u = (CSet) collection.iterator().next().clone();
		
		for(CSet v : collection){
			u.set.addAll(v.set);
		}
		
		return u;
	
	}
	
	public static CStruct glb(Collection<CSet> collection) throws EmptyCStructSetException {
		
		if(collection.isEmpty()) throw new EmptyCStructSetException();
		
		if(collection.size()==1) return (CStruct) collection.iterator().next().clone();

		CSet u = (CSet) collection.iterator().next().clone();
		
		for(CSet v : collection){
			u.set.retainAll(v.set);
		}
		
		return u;
		
	}
	
	public static boolean isCompatible(Collection<CHistory> collection){
		return true;
	}
	
	public static CStruct leftLubOf(CSet u, CSet v){
		CSet ret = (CSet)u.clone();
		ret.set.addAll(v.set);
		return ret;
	}
	
	//
	// Object's methods
	//
	
	@Override
	public boolean append(Command c) {
		return set.add(c);
	}

	@Override
	public boolean contains(Command c) {
		return set.contains(c);
	}

	@Override
	protected boolean remove(Command c) {
		return set.remove(c);
	}
	
	@Override
	public boolean removeAll(CStruct u){
		return set.removeAll(((CSet)u).set);
	}
	
	@Override
	public void clear() {
		set.clear();
	}

	@Override
	public int size() {
		return set.size();
	}

	public int compareTo(CStruct u) throws ClassCastException {
		if(!(u instanceof CSet)) throw new ClassCastException();
		if(size()==((CSet)u).size()){
			if(set.containsAll(((CSet)u).set)) return 0;
			throw new ClassCastException();
		}
		if(set.containsAll(((CSet)u).set)) return 1;
		if(((CSet)u).set.containsAll(set)) return -1;
		throw new ClassCastException();
	}

	public Iterator<Command> iterator() {
		return set.iterator();
	}

	public boolean equals(CSet u){
		if(this==u) return true;
		return set.equals(u.set);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Object clone(){
		CSet u = (CSet)super.clone();
		u.set = (LinkedHashSet<Command>) set.clone();
		return u;
	}
	
	@Override
	public String toString(){
		return "#"+checkpointNumber()+set.toString();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		set = (LinkedHashSet<Command>) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(set);
	}
	
	@Override
	public boolean removeAll(Collection<?> collection) {
		return set.removeAll((Collection<Command>) collection);
	}
	
}
