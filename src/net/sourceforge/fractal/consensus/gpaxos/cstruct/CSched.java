package net.sourceforge.fractal.consensus.gpaxos.cstruct;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import net.sourceforge.fractal.Messageable;
import net.sourceforge.fractal.replication.Command;

/**
 * 
 * An implementation of sequence of commands with non-duplicated content.
 * FIXME linked hash set.
 * 
 * 
 * @author Pierre Sutra
 *
 */

public class CSched extends CStruct{

	private static final long serialVersionUID = Messageable.FRACTAL_MID;

	private LinkedList<Command> list;
	private HashSet<Command> cache;
	
	public CSched(){
		super();
		list = new LinkedList<Command>();
		cache = new HashSet<Command>();
	}
	
	//
	// Class'interfaces
	//

	public static CStruct lub(Collection<CSched> collection) throws IncompatibleCStructException, EmptyCStructSetException{
		
		if(collection.isEmpty()) throw new EmptyCStructSetException();
		
		if(collection.size()==1) return (CStruct) collection.iterator().next().clone();
		
		CSched lub = collection.iterator().next();
		
		for(CSched s : collection){
			try{
				if(s.compareTo(lub)==1)	lub = s;
			}catch(ClassCastException e){
				throw new IncompatibleCStructException();
			}
		}
		
		return (CStruct) lub.clone();
		
	}
	
	@SuppressWarnings("unchecked")
	public static CStruct glb(Collection<CSched> collection) throws EmptyCStructSetException{
		
		if(collection.isEmpty()) throw new EmptyCStructSetException();
		
		if(collection.size()==1) return (CStruct) collection.iterator().next().clone();

		CSched s = collection.iterator().next();
		CSched ret = new CSched();
		
		for(int i=0; i<s.list.size(); i++){
			boolean toAppend = true;
			for(CSched s0 : collection){
				if( i >= s0.list.size()
					||
					! s0.list.get(i).equals(s.list.get(i)) 
				  ){
					toAppend = false; 
					break;
				}
			}
			if(toAppend){
				ret.append(s.list.get(i));
			}else{
				break;
			}
		}
		
		return ret;
	}
	
	public static boolean isCompatible(Collection<CSched> collection) throws ClassCastException, EmptyCStructSetException{

		if(collection.size()==1) return true;

		try {
			lub(collection);
		} catch (IncompatibleCStructException e) {
			return false;
		}
		
		return true;
	}

	public static CStruct leftLubOf(CSched u, CSched v){
		return (CStruct)u.clone();
	}

	
	//
	// Object's interface
	//
	
	@Override
	public boolean append(Command c) {
		if(cache.contains(c)) return false;
		list.add(c);
		cache.add(c);
		return true;
	}

	@Override
	protected boolean remove(Command c) {
		if(!cache.contains(c)) return false;
		list.remove(c);
		cache.remove(c);
		return true;
	}
	

	@SuppressWarnings("unchecked")
	@Override
	public boolean removeAll(Collection<?> collection) {
		cache.removeAll((Collection<Command>)collection);
		return list.removeAll((Collection<Command>) collection);
	}
	
	
	@Override
	public boolean removeAll(CStruct u){
		cache.removeAll(((CSched)u).cache);	
		return list.removeAll(((CSched)u).cache);
	}
	
	
	@Override
	public boolean contains(Command c) {
		return cache.contains(c);
	}
	
	/**
	 * 	0=equals, 1=this is higher, -1=u is higher
	 * 
	 * {@inheritDoc}
	 * 
	 */
	public int compareTo(CStruct u) throws ClassCastException{
		
		if( this == u ) return 0;
		if( !(u instanceof CSched) ) throw new ClassCastException();
		
		CSched s0 = (CSched)u;
		
		for(int i = 0; i < s0.list.size(); i++){
			if( list.size()==i) return -1;
			if( !s0.list.get(i).equals(list.get(i)) ) throw new ClassCastException();
		}
		
		if(this.list.size() == s0.list.size()) return 0;
		
		return 1;
		
	}

	public Iterator<Command> iterator() {
		return list.iterator();
	}
	
	@Override
	public int size(){
		return list.size();
	}
	
	@Override
	public void clear() {
		list.clear();
		cache.clear();
	}

	//
	// Others
	//


	@Override
	public String toString(){
		return super.toString()+list.toString();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Object clone(){
		CSched u = (CSched)super.clone();
		u.list = new LinkedList<Command>(list);
		u.cache = new HashSet<Command>(cache);
		return u;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		list = (LinkedList<Command>) in.readObject();
		cache = new HashSet<Command>(list);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(list);
	}
	
}
