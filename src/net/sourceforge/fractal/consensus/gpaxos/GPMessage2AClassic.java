package net.sourceforge.fractal.consensus.gpaxos;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import net.sourceforge.fractal.Messageable;
import net.sourceforge.fractal.replication.Command;

/**   
* @author P. Sutra
* 
*/ 


public class GPMessage2AClassic extends GPMessage2A {

	private static final long serialVersionUID = Messageable.FRACTAL_MID;

	List<Command> cmds;
	
	public GPMessage2AClassic(){}
	
	public GPMessage2AClassic(int source, int ballot, List<Command> cmds){
		super(source, ballot);
		this.cmds = cmds;
	}
	
	@SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        cmds = (List<Command>) in.readObject();
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
    	super.writeExternal(out);
    	out.writeObject(cmds);
    }
	
}
