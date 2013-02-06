
package net.sourceforge.fractal.commit;

public interface CommitLearner{
    /**
     * Tell's the commit learner that a transaction in stream @param streamName terminated with result @param result, and the values in @param values.
     */
	/**   
	* @author L. Camargos
	* 
	*/ 

	
	
    public void learnCommitTerminated(String streamName, int tid, boolean result, CommitValue[] values);
}
