package net.sourceforge.fractal.replication;


/**
 * @author Pierre Sutra
 */

public abstract class ReplicationStreamClient{

	protected ReplicationStream stream;
	
	public ReplicationStreamClient(ReplicationStream s){
		stream = s;
	}

}
