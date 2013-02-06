package net.sourceforge.fractal.replication;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import net.sourceforge.fractal.Messageable;

public class ReadWriteRegisterCommand extends CommutativeCommand {
	/**   
	* @author P. Sutra
	* 
	*/
	private static final long serialVersionUID = Messageable.FRACTAL_MID;
	
	private final static int PAYLOADSIZE = 0;
	
	public boolean isRead;
//	public byte[] payload;
	
	@Deprecated
	public ReadWriteRegisterCommand(){}
	
	public ReadWriteRegisterCommand(int source, int r, boolean read){
		super(source, r);
		isRead=read;
//		payload= ByteBuffer.allocate(PAYLOADSIZE).array();
	}
	
	@Override
	public boolean commuteWith(CommutativeCommand c) {
		if(equals(c)) return true;
		if(c.key == 0) return false;
		if(this.source == c.source) return false; // small speed-up
		if( key != c.key ) return true;
		if(isRead && ((ReadWriteRegisterCommand)c).isRead) return true;
		return false;
	}	
		
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		source = in.readInt();
		seq = in.readInt();
		key = in.readLong();
		isRead = in.readBoolean();
//		payload = new byte[PAYLOADSIZE];
//		in.read(payload);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(source);
		out.writeInt(seq);
		out.writeLong(key);
		out.writeBoolean(isRead);
//		out.write(payload);
	}
	
	@Override
	public String toString(){
		return super.toString()+":"+(isRead? "R" : "W")+":"+key;
	}

}
