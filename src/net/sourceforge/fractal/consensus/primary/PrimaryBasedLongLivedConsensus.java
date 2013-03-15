package net.sourceforge.fractal.consensus.primary;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.consensus.LongLivedConsensus;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.CollectionUtils;

public class PrimaryBasedLongLivedConsensus<C> extends LongLivedConsensus<C> implements Learner {

	// FIXME check that the group supports FIFO communications
	private Group group; 
	private BlockingQueue<C> q;
	
	public PrimaryBasedLongLivedConsensus(Group g){
		group = g;
		q=CollectionUtils.newBlockingQueue();
		g.registerLearner("PrimaryBasedLongLivedConsensusMessage", this);
	}

	@Override
	public C propose(C c) {
		
		if(group.iLead()){
			// FIXME
			group.broadcastToOthers(new PrimaryBasedLongLivedConsensusMessage<C>(c));
			return c;
		}
		
		try {
			return q.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return null;
		
	}

	@Override
	public void start() {
		group.start();
	}

	@Override
	public void stop() {
		group.stop();
	}

	@Override
	public void learn(Stream s, Serializable value) {
		// FIXME ugly
		q.offer((C)((PrimaryBasedLongLivedConsensusMessage)value).c);
	}

}
