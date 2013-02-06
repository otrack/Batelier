package net.sourceforge.fractal.replication;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.sourceforge.fractal.Messageable;

/**
 * 
 * @author Pierre Sutra
 *
 * It exists a total order over the set of commands such that 
 * if c.source== d.source && c > d, then c.source delivered cbefore proposing d.
 * 
 * If the application does not follows this, some commands might never be 
 * delivered. 
 *
 */

public abstract class Command implements Messageable, Comparable<Command> {
	
	private static final long serialVersionUID = Messageable.FRACTAL_MID;
	private static ConcurrentSkipListMap<Integer,AtomicInteger> counters; 
	
	static{
		counters = new ConcurrentSkipListMap<Integer,AtomicInteger>();
	}
	protected int source;
	protected int seq;
	
	@Deprecated
	protected Command(){}
	
	protected Command(int s){
		source = s;
		seq = nextUniqueCounterFor(s);
	}
	
	protected  final static Integer nextUniqueCounterFor(int s){
		counters.putIfAbsent(s,new AtomicInteger(0));
		return counters.get(s).addAndGet(1);
	}
	
	public final int source(){
		return source;
	}
	
	public final int sequenceNumber(){
		return seq;
	}
		
	@Override
	public boolean equals(Object o){
		if(o == this) return true;
		return source == ((Command)o).source && seq == ((Command)o).seq;
	}
	
	@Override
	public int hashCode(){
		return 100000*source+seq;
	}
	
	public final int compareTo(Command c){
		if( seq > c.seq ) return 1;
		if( seq < c.seq ) return -1;
		if( source > c.source ) return 1;
		if( source < c.source ) return -1;
		return 0;
	}
		
	@Override
	public String toString(){
		return source+":"+seq;
	}
	
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		source = in.readInt();
		seq = in.readInt();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(source);
		out.writeInt(seq);
	}
	
}
