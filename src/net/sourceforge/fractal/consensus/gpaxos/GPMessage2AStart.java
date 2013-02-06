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


public class GPMessage2AStart extends GPMessage2A {
	
	private static final long serialVersionUID = Messageable.FRACTAL_MID;

	CStruct cstructToAccept;
	
	public GPMessage2AStart(){}
	
	public GPMessage2AStart(int source, int ballot, CStruct cstruct){
		super(source, ballot);
		cstructToAccept = cstruct;
	}
	
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        cstructToAccept = (CStruct) in.readObject();
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
    	super.writeExternal(out);
    	out.writeObject(cstructToAccept);
    }

	
}
