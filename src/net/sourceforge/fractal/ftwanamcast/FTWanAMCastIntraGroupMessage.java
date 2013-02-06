package net.sourceforge.fractal.ftwanamcast;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.broadcast.BroadcastMessage;
/**   
* @author P. Sutra
* 
*/ 

public class FTWanAMCastIntraGroupMessage extends BroadcastMessage implements Cloneable{

	private static final long serialVersionUID = 1L;

	public HashSet<String> dest;
	
	public FTWanAMCastIntraGroupMessage(){
		
	}
	
	public FTWanAMCastIntraGroupMessage(Serializable s, Set<String> dest, int swidSource){
		super(s,swidSource);
		this.dest = new HashSet<String>(dest);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		 super.writeExternal(out);
		 out.writeObject(dest);
	}
		 
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		dest=(HashSet<String>)in.readObject();
	}
		
	public Object clone(){
		return super.clone();
	}
	
}
