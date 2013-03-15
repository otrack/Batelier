package net.sourceforge.fractal.consensus;

import net.sourceforge.fractal.MutedStream;

public abstract class LongLivedConsensus<C> extends MutedStream{
	
	public abstract C propose(C c);

}
