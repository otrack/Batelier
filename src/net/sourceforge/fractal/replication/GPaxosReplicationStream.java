package net.sourceforge.fractal.replication;

import java.io.Serializable;
import java.util.Set;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream;

public class GPaxosReplicationStream extends ReplicationStream implements Learner {
	/**   
	* @author P. Sutra
	* 
	*/
	public GPaxosReplicationStream(FractalManager manager, Set<Integer> c,GPaxosStream gp, int nbObjects){
		super(manager, c,gp,nbObjects);
	}

	@Override
	protected void register() {
		stream.registerLearner("*", this);
	}
	
	@Override
	protected void unregister() {
		stream.unregisterLearner("*", this);
	}
	
	@Override
	protected Command receiveCommand(Serializable value) {
		return (Command) value;
	}


	@Override
	protected void sendCommand(Command c) {
		((GPaxosStream)stream).propose(c);
	}


	
}
