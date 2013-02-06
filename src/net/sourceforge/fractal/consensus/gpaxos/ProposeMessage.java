package net.sourceforge.fractal.consensus.gpaxos;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.replication.Command;

/**   
* @author P. Sutra
* 
*/ 


public class ProposeMessage extends Message {

	private static final long serialVersionUID = 1L;
	
	Command command;
	
	public ProposeMessage(){}
	
	public ProposeMessage(int swid, Command command){
		super();
		this.source = swid;
		this.command = command;
	}
	
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        command = (Command) in.readObject();
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
    	super.writeExternal(out);
    	out.writeObject(command);
    }
	
}
