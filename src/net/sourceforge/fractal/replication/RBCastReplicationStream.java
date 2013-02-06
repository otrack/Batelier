package net.sourceforge.fractal.replication;

import java.io.Serializable;
import java.util.Set;

import net.sourceforge.fractal.broadcast.BroadcastMessage;
import net.sourceforge.fractal.broadcast.BroadcastStream;

public class RBCastReplicationStream extends ReplicationStream {
	/**   
	* @author P. Sutra
	* 
	*/
	public RBCastReplicationStream(Set<Integer> c, BroadcastStream rbcast, int nbObjects){
		super(c,rbcast,nbObjects);
	}

	@Override
	protected void register() {
		stream.registerLearner("RBCastMessage", this);
	}

	@Override
	protected void unregister() {
		stream.unregisterLearner("RBCastMessage", this);
	}
	
	@Override
	protected void sendCommand(Command c) {
		((BroadcastStream)stream).broadcast(new BroadcastMessage(c,myId));
	}

	@Override
	protected Command receiveCommand(Serializable value) {
		return (Command)((BroadcastMessage)value).serializable;
	}



}
