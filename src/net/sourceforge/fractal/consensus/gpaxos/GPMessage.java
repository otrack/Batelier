package net.sourceforge.fractal.consensus.gpaxos;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.sourceforge.fractal.Message;

/**   
* @author P. Sutra
* 
*/ 


public abstract class GPMessage extends Message {

    int ballot;
    
    public GPMessage(){}
    
    public GPMessage(int swid, int ballot){
    	super();
    	this.source = swid;
    	this.ballot = ballot;
    }
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        ballot = in.readInt();
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
    	super.writeExternal(out);
    	out.writeInt(ballot);
    }
    
    
    
}
