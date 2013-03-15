package net.sourceforge.fractal.consensus.primary;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.sourceforge.fractal.Message;

public class PrimaryBasedLongLivedConsensusMessage<C> extends Message{

	C c;
	
	public PrimaryBasedLongLivedConsensusMessage(){}
	
	public PrimaryBasedLongLivedConsensusMessage(C d) {
		c=d;
	}
	
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        source = in.readInt();
        c = (C)in.readObject();
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(source);
        out.writeObject(c);
    }

	
}
