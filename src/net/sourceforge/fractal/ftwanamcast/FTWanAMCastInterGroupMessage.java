package net.sourceforge.fractal.ftwanamcast;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.HashSet;

import net.sourceforge.fractal.multicast.MulticastMessage;
/**   
* @author P. Sutra
* 
*/ 

public class FTWanAMCastInterGroupMessage extends MulticastMessage implements Cloneable {

	private static final long serialVersionUID = 1L;
	int round;
	
	public FTWanAMCastInterGroupMessage(){
		  
	}
	
	public FTWanAMCastInterGroupMessage(Serializable s, HashSet<String> dest, String gSource, int swidSource, 
			int round){
		super(s,dest,gSource,swidSource);
		this.round = round;
	}
		
	public Object clone(){
		FTWanAMCastInterGroupMessage m = (FTWanAMCastInterGroupMessage)super.clone();
		m.round = this.round;
		return m;
	}
	
	public void writeExternal(ObjectOutput out) throws IOException {
		 super.writeExternal(out);
		 out.writeObject(this.round);
	}
		 
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		this.round = (Integer) in.readObject();
	}
		
	public String toString(){
		return "<"+round+","+getGSource()+">";
	}

	public boolean equals(Object o){
		return super.equals(o);
	}
}
