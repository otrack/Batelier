package net.sourceforge.fractal.consensus.gpaxos;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.sourceforge.fractal.Messageable;
import net.sourceforge.fractal.consensus.gpaxos.cstruct.CStruct;

/**   
* @author P. Sutra
* 
*/ 


public class GPMessage1B extends GPMessage {

	private static final long serialVersionUID = Messageable.FRACTAL_MID;;

	int ballot;
	int lastBallot;
	CStruct lastCStructAccepted;
	
	public GPMessage1B(){}
	
	public GPMessage1B(int source, int ballot, int lastBallot, CStruct lastCStructAccepted){
		super(source, ballot);
		this.lastBallot = lastBallot;
		this.lastCStructAccepted = lastCStructAccepted;
	}
	
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        lastBallot = in.readInt();
        lastCStructAccepted = (CStruct) in.readObject();
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
    	super.writeExternal(out);
    	out.writeInt(lastBallot);
    	out.writeObject(lastCStructAccepted);
    }
	
}
