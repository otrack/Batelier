package net.sourceforge.fractal.consensus.gpaxos;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import net.sourceforge.fractal.Messageable;

/**   
* @author P. Sutra
* 
*/ 


public class GPMessage1A extends GPMessage{

	private static final long serialVersionUID = Messageable.FRACTAL_MID;;
	
	
	public GPMessage1A(){}
	
	public GPMessage1A(int source, int ballot){
		super(source, ballot);
	}
	
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
    	super.writeExternal(out);
    }
	
}
