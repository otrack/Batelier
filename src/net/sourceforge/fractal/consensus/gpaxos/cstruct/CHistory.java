package net.sourceforge.fractal.consensus.gpaxos.cstruct;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;

import net.sourceforge.fractal.Messageable;
import net.sourceforge.fractal.replication.CheckpointCommand;
import net.sourceforge.fractal.replication.Command;
import net.sourceforge.fractal.replication.CommutativeCommand;

/**
 * 
 * @author Pierre Sutra
 *
 */

public class CHistory extends CStruct {

	private static final long serialVersionUID = Messageable.FRACTAL_MID;
	
	private LinkedHashSet<Command> list;
	
	public CHistory(){
		super();
		list = new LinkedHashSet<Command>();
	}	
	
	//
	// Class' interface
	//

	public static CStruct glb(final Collection<CHistory> collection) throws EmptyCStructSetException {

		if(collection.isEmpty()) throw new EmptyCStructSetException();

		if(collection.size()==1) return (CStruct) collection.iterator().next().clone();

		CHistory h = collection.iterator().next();

		CHistory ret = new CHistory();
		boolean toAdd=false;

		for(Command c : h.list){

			for(CHistory h1 : collection){

				toAdd=false;
				
				if(!h1.list.contains(c)) break;
				
				for(Command d : h1){
					
					if( ret.list.contains(d) )
						continue;
					
					if(c.equals(d)){
						toAdd=true;
						break;
					}
					
					if(	! ((CommutativeCommand)c).commuteWith((CommutativeCommand)d)  ){
						toAdd=false;
						break;
					  }
					
				}
				
				if(!toAdd) break;

			}

			if(toAdd){
				if(c instanceof CheckpointCommand)
					ret.append((CheckpointCommand)c);
				else
					ret.append(c);
			}

		}

		return ret;
		
	}
	
	public static CStruct lub(final Collection<CHistory> collection) throws IncompatibleCStructException, EmptyCStructSetException{
		throw new RuntimeException("Not implemented");
	}
	
	/**
	 * 
	 * @param u
	 * @param v
	 * @return The lub of u and the greatest prefix of v compatible with u.
	 * 
	 * This function moodifies u.
	 * 
	 */
	public static CStruct leftLubOf( CHistory u, final CHistory v){
		
		CHistory ret = u;
		
		// We compute commands in v not in ret that we can add to the end of ret.
		ArrayList<Command> added = new ArrayList<Command>();
		HashSet<Command> notAdded = new HashSet<Command>();
		HashSet<Command> seen = new HashSet<Command>(v.size());
		boolean toAdd;
		for(Command c : v){
			
			if( ! ret.contains(c) ){
			
				// can we add c at the end of ret ?
				// we can = (i) there is no commmand in ret non-commuting with c that are not prior to c in v.
				//          and  
				//          (ii) every command in v prior to c and non-commuting with c is added.
				
				toAdd=true;
				 
				// (i)
				for(Command d : ret){
					if( seen.contains(d)) continue;
					if( ! ((CommutativeCommand)c).commuteWith((CommutativeCommand)d) ){
						toAdd=false;
						break;
					}
				}
				
				// (ii)
				if(toAdd){ 
					for(Command d : notAdded){
						if( ! ((CommutativeCommand)c).commuteWith((CommutativeCommand)d) ){
							toAdd=false;
							break;
						}
					}
				}

				if(toAdd){
					added.add(c);
				}else{
					notAdded.add(c);
				}
				
			}
			
			seen.add(c);
			
		}
		
		ret.appendAll(added);

		return ret;
			
	}
	
	public static boolean isCompatible(final Collection<CHistory> collection){
		
		if(collection.isEmpty()) return true;
		
		if(collection.size()==1) return true;
		
		CHistory h = collection.iterator().next();
		
		for(CHistory h1 : collection){
			
			if(h==h1) continue;
			
			if(!isPairWiseCompatible(h, h1)) return false;
			
			if(!isPairWiseCompatible(h1, h)) return false;
			
		}
		
		return true;
		
	}
	
	private static boolean isPairWiseCompatible(final CHistory h, final CHistory h1){
		
		if(h.size()==0 || h1.size()==0) return true;
				
		HashSet<Command> seen = new HashSet<Command>();
		
		for(Command c : h){
			
			for(Command d : h1){
				
				if(c.equals(d))
					break;
				
				if( ! seen.contains(d)
					&&
					! ((CommutativeCommand)c).commuteWith((CommutativeCommand)d))
					return false;
				
			}
			
			seen.add(c);
			
		}
		
		return true;
		
	}


	@Override
	protected boolean append(Command c) {
		return list.add(c);
	}
	
	@Override
	public void clear() {
		list.clear();
	}

	@Override
	protected boolean contains(Command c) {
		return list.contains(c);
	}

	@Override
	protected boolean remove(Command c) {
		return list.remove(c);
	}
	
	@Override
	public boolean removeAll(CStruct u){
		return list.removeAll(((CHistory)u).list);
	}

	@Override
	public int size() {
		return list.size();
	}

	public int compareTo(CStruct arg0) {
		throw new RuntimeException("Not yet implemented");
	}

	public Iterator<Command> iterator() {
		return list.iterator();
	}


	@Override
	public int hashCode(){
		return list.hashCode();
	}

	@Override
	public String toString(){
		return super.toString()+list.toString();
	}
	
	public boolean equals(CHistory u){
		return this==u;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Object clone(){
		CHistory u = (CHistory)super.clone();
		u.list = new LinkedHashSet<Command>(list);
		return u;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		list = (LinkedHashSet<Command>) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(list);
	}

	@Override
	public boolean removeAll(Collection<?> collection) {
		return list.removeAll((Collection<Command>) collection);
	}

}
