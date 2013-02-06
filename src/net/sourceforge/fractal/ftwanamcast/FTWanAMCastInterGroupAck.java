package net.sourceforge.fractal.ftwanamcast;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;

import net.sourceforge.fractal.multicast.MulticastMessage;

public class FTWanAMCastInterGroupAck extends MulticastMessage {

	/**   
	* @author P. Sutra
	* 
	*/ 

	
	private static final long serialVersionUID = 1L;
	int round;
	String gAcked;
	
	public FTWanAMCastInterGroupAck(){
		  
	}
	
	public FTWanAMCastInterGroupAck(
				HashSet<String> dest,
				String gSource,
				int swidSource,
				int round,
				String gAcked){
		super(null,dest,gSource,swidSource);
		this.round = round;
		this.gAcked = gAcked;
	}
	
	public void writeExternal(ObjectOutput out) throws IOException {
		 super.writeExternal(out);
		 out.writeObject(this.round);
		 out.writeObject(this.gAcked);
	}
		 
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		this.round = (Integer) in.readObject();
		this.gAcked= (String) in.readObject();
	}
		
	public String toString(){
		return "ack<"+round+","+gAcked+">";
	}

	public boolean equals(Object o){
		return super.equals(o);
	}
}
