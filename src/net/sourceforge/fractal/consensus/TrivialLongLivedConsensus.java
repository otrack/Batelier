package net.sourceforge.fractal.consensus;


public class TrivialLongLivedConsensus<C> extends LongLivedConsensus<C> {

	@Override
	public C propose(C c) {
		return c;
	}

	@Override
	public void start() {}

	@Override
	public void stop() {}

}
