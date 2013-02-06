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


public class GPMessage2BStart extends GPMessage2B{

	private static final long serialVersionUID = Messageable.FRACTAL_MID;
	
	CStruct accepted;
	
	public GPMessage2BStart(){}
	
	public GPMessage2BStart(int source, int ballot, CStruct toLearn){
		super(source, ballot);
		this.accepted = toLearn;
	}
	
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        accepted= (CStruct) in.readObject();
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
    	super.writeExternal(out);
    	out.writeObject(accepted);
    }
	
}
