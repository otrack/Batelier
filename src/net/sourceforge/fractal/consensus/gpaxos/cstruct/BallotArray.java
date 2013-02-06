package net.sourceforge.fractal.consensus.gpaxos.cstruct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream.RECOVERY;
import net.sourceforge.fractal.replication.CheckpointCommand;
import net.sourceforge.fractal.replication.Command;
import net.sourceforge.fractal.utils.CollectionUtils;

/**
 * 
 * @author Pierre Sutra
 * 
 * An implementation of the ballot array abstraction.
 * It contains as well a mapping from ballots to acceptors and coordinators.
 * 
 * This implementation maintains incrementally for a ballot m,
 * the c-struct learned at m, i.e., what would return provedSafe for this ballot.
 * It is saved in the map learned.
 * 
 * There is a single quorum in the ballot which is a majority centered on the coordinator of m; this allows the following optimizations:
 * (1) given a ballot m and an acceptor a, ballotArray.get(m).get(a) contains the difference between what a accepted at m and what is learned at m.
 *     More precisely, for every acceptor a, the c-struct accepted by a at ballot m equals learned.get(m).append(ballotArray.get(m).get(a)).
 *     This heavily decrease the time to update learned.
 * (2) any c-struct accepted by any acceptor a at some ballot m is safe (provided ofc that a joins a higher ballot, 
 *     which is assumed when the provedSafe() is callled). 
 *
 */

public class BallotArray {
	
	private static final int CONSECUTIVE_COORDINATED_BALLOTS=100000;
	
	private CStructFactory cfactory;
	private Set<Integer> acceptors;
	
	private Map<Integer, TreeMap<Integer, CStruct>> ballotArray;
	private CStruct learned;
	private int highestAcceptedBallot; // the highest ballot during which one acceptor voted
	private int highestLearnedBallot; // the highest ballot during which something was learned

	private boolean useFastBallot;
	private int quorumSize;
	private Map<Integer,Set<Integer>> writeQUorums;
	
	private RECOVERY recovery;
		
	// methods'variables
	private List<Integer> toRemove;
	private List<Integer> sortedAcceptors;

	
	public BallotArray(CStructFactory factory, Set<Integer> acceptorSet, boolean fastBallot, RECOVERY rec){
		cfactory = factory;
		acceptors = acceptorSet;
			
		useFastBallot = fastBallot;
		recovery = rec;
		quorumSize = (acceptors.size()/2) +1;
		writeQUorums = CollectionUtils.newMap();
		writeQUorums.put(0, acceptors);
		
		ballotArray = new TreeMap<Integer, TreeMap<Integer,CStruct>>();
		ballotArray.put(0, new TreeMap<Integer, CStruct>());
		for(int a :  acceptors){
			ballotArray.get(0).put(a,cfactory.newCStruct());
		}
		learned = cfactory.newCStruct();
		highestAcceptedBallot=0;
		highestLearnedBallot=0;
		
		// methods'variables
		toRemove = new ArrayList<Integer>();
		sortedAcceptors = new ArrayList<Integer>(acceptors);
		java.util.Collections.sort(sortedAcceptors);
		
	}
	
	//
	// INTERFACES
	//
	
	// Ballot and quorums related
	
	public boolean isFast(int ballot){
		if( ballot!=0 && useFastBallot )
			return true;
		return false;	
	}

	// FIXME Improve memory-management by using a fixed quorums set.
	public Set<Integer> writeQuorumOf(int ballot){
		
		assert ballot >= highestLearnedBallot;
		
		if(writeQUorums.containsKey(ballot)) return writeQUorums.get(ballot);
		
		Set<Integer> q = new HashSet<Integer>();
		q.add(coordinatorOf(ballot));
		for(int i=sortedAcceptors.indexOf(coordinatorOf(ballot)); q.size()!=quorumSize ;i=(i+1)%sortedAcceptors.size()){
			q.add(sortedAcceptors.get(i));
		}
		writeQUorums.put(ballot, q);
		
		return q;
	}	

	public boolean isReadQuorumOf(Set<Integer> quorum, int ballot){
		assert ballot >= highestLearnedBallot;
		if( recovery==RECOVERY.DEFAULT )
			return quorum.containsAll(writeQuorumOf(ballot)); // we simulate 
		return CollectionUtils.isIntersectingWith(quorum, writeQuorumOf(ballot));
	}
	
	public boolean isWriteQuorumOf(Set<Integer> quorum, int ballot){
		assert ballot >= highestLearnedBallot;
		if(quorum.containsAll(writeQuorumOf(ballot)) ) return true;
		return false;
	}
	
	public boolean writeQuorumVotedAt(int ballot){
		assert ballot >= highestLearnedBallot;
		return ballotArray.get(ballot)!= null && isWriteQuorumOf(ballotArray.get(ballot).keySet(),ballot);
	}
	
	public int coordinatorOf(int ballot){
		assert ballot >= highestLearnedBallot;
		return  sortedAcceptors.get(((ballot/CONSECUTIVE_COORDINATED_BALLOTS)%sortedAcceptors.size()));
	}
	
	/**
	 * 
	 * @param acceptor
	 * @param ballot
	 * @return <i>true</i> if <i>acceptor</i> coordinate <i>ballot</i>. 
	 */
	public boolean isCoordinatorOf(int acceptor, int ballot){
		assert ballot >= highestLearnedBallot;
		if(ballot==0) return true;
		return ((ballot/CONSECUTIVE_COORDINATED_BALLOTS)%sortedAcceptors.size())
				== sortedAcceptors.indexOf(acceptor);
	}
	
	/**
	 * 
	 * @param acceptor
	 * @param ballot
	 * @return the next ballot <i>acceptor</i> coordinates after <i>ballot</i>.
	 */
	public int nextBallotToCoordinateAfter(int acceptor, int ballot){
		
		assert ballot >= highestLearnedBallot;
		
		int interval = ballot/CONSECUTIVE_COORDINATED_BALLOTS;
		
		int index = ballot - interval*CONSECUTIVE_COORDINATED_BALLOTS;
		
		if( isCoordinatorOf(acceptor, ballot)
			&& index+1 < CONSECUTIVE_COORDINATED_BALLOTS ){
			
			return ballot+1;
			
		}else{
			
			if( interval%sortedAcceptors.size() < sortedAcceptors.indexOf(acceptor) ){
				return (
						interval
						- interval%sortedAcceptors.size()
						+ sortedAcceptors.indexOf(acceptor)
						) * CONSECUTIVE_COORDINATED_BALLOTS;  
			}else{ // >=
				return (
							interval
							- interval%sortedAcceptors.size() 
							+ sortedAcceptors.size()
							+ sortedAcceptors.indexOf(acceptor)
						) * CONSECUTIVE_COORDINATED_BALLOTS;  
			}
				
			
		}
	}
	
	/**
	 * 
	 * @param acceptor
	 * @param ballot
	 * @return true if acceptor <i>a</i> can execute a coordinated recovery for ballot <i>ballot</i>
	 * 
	 * An acceptor a can execute a two-step recovery for ballot b, 
	 * only if 
	 * (i) a is the coordinator of both ballots b-1 and b,
	 * (ii) b-1 is centered over a
	 * and (iii) a has accepted a c-cstruct at ballot b-1. 
	 * 
	 */
	public boolean canTwoStepRecoverAt(int ballot, int a){
		assert ballot >= highestLearnedBallot;
		return recovery == RECOVERY.COORDINATED
			   && isFast(ballot-1)
			   && isFast(ballot)
			   && isCoordinatorOf(a, ballot-1)
			   && isCoordinatorOf(a, ballot)
		       && ballotArray.containsKey(ballot-1)
		       && ballotArray.get(ballot-1).containsKey(a) 
		       && collide(ballot); 
	}

	public boolean canOneStepRecoverAt(int ballot, int a) {
		assert ballot >= highestLearnedBallot;
		return recovery == RECOVERY.FGGC
		   			&& isFast(ballot)
		   			&& isFast(ballot+1)
		   			&& ballotArray.containsKey(ballot)
		   			&& ballotArray.get(ballot).containsKey(coordinatorOf(ballot))
		   			&& writeQuorumOf(ballot).contains(a)
		   			&& collide(ballot);
	}
	
	public boolean canDefaultRecoverAt(int ballot, int a) {
		assert ballot >= highestLearnedBallot;
		return recovery == RECOVERY.DEFAULT
				&& isFast(ballot)
		   		&& isCoordinatorOf(a, ballot)
		   		&& collide(ballot);
	}
	
	// acceptor related	

	/**
	 * 
	 * Consider a ballot b. 
	 * Let b' be the highest ballot smaller than b at which an acceptor has accepted a c-struct.
	 * Since there is a single (b')-quorum Q , every c-struct accepted at b' by a member of Q ' is safe.
	 * 
	 * @return a c-struct safe at ballot highestAcceptedBallot 
	 */
	public CStruct provedSafe() {

		if(highestAcceptedBallot==0)
			return cfactory.newCStruct();
		
		CStruct ret= cfactory.clone(learned);
		ret.appendAll(acceptedAtBy(highestAcceptedBallot,ballotArray.get(highestAcceptedBallot).keySet().iterator().next()));
		return ret;

	}
	
	public CStruct accept(int ballot, int acceptor, CStruct cstruct){
		assert ballot >= highestLearnedBallot;
		assert cstruct!= null;
		assert writeQuorumOf(ballot).contains(acceptor);
				
		if( !ballotArray.containsKey(ballot) )
			ballotArray.put(ballot, new TreeMap<Integer,CStruct>());
		
		ballotArray.get(ballot).put(acceptor, cstruct);
		
		if( ballot > highestAcceptedBallot ) highestAcceptedBallot=ballot;
		
		return update(ballot);
		
	}

	public CStruct append(int ballot, int acceptor, List<Command> cmds) {
		assert ballot >= highestLearnedBallot;
		assert ballotArray.containsKey(ballot) && ballotArray.get(ballot).containsKey(acceptor);
		
		ballotArray.get(ballot).get(acceptor).appendAll(cmds);
		
		return update(ballot);
	}
	
	/**
	 * 
	 * @param ballot
	 * @param a
	 * 
	 * @return true if acceptor <i>a</i> has accepted a c-struct at ballot <i>ballot</i>
	 */
	public boolean hasVotedAtBy(int ballot, int a){
		assert ballot >= highestLearnedBallot;
		return ballotArray.containsKey(ballot) && ballotArray.get(ballot).containsKey(a);
	}
	
	/**
	 * 
	 * @param ballot
	 * @param a
	 * @return
	 */
	public CStruct acceptedAtBy(int ballot, int a){
		
		assert ballot >= highestLearnedBallot
			  	&& ballotArray.get(ballot).containsKey(a)
			  	&& ballotArray.get(ballot).get(a)!=null : a+"\t"+"ballot"+"\t"+ballotArray;
		return cfactory.clone(ballotArray.get(ballot).get(a)); 
	}
	
	public Map<Integer,CStruct> acceptedAt(int ballot){	
		return ballotArray.get(ballot);
	}
	
	public boolean isInstalled(int checkpointNumber){
		return learned.checkpointNumber() >= checkpointNumber;
	}

	// helpers
	
	public int highestAcceptedBallot() {
		return highestAcceptedBallot;
	}
	
	public int highestLearnedBallot() {
		return highestLearnedBallot;
	}
		
	public CStruct learned() {
		return learned;
	}
		
	// others
	
	public void clear(){
		ballotArray.clear();
		ballotArray.put(0, new TreeMap<Integer, CStruct>());
		for(int a :  acceptors){
			ballotArray.get(0).put(a,cfactory.newCStruct());
		}
		highestAcceptedBallot=0;
		learned.clear();
	}
	
		
	public String toString(){
		return "BallotArray:\t"+ballotArray.toString()+"\t Learned:\t"+learned;
	}

	//
	// inner methods
	//
	
	
	private boolean collide(int ballot){
		assert ballot >= highestLearnedBallot;
		if(!writeQuorumVotedAt(ballot)) return false;
		return !cfactory.isCompatible(ballotArray.get(ballot).values());
	}
	

	private CStruct update(int ballot){
		
		CStruct glb = null;
		
		if( !writeQuorumVotedAt(ballot) )
			return null;
		
		if( ballot > highestLearnedBallot ){
		    for(CStruct u : ballotArray.get(ballot).values()){ 
		    	u.removeAll(learned);
		    }
		}
		
		glb = cfactory.glb(ballotArray.get(ballot).values());
				
		if(ballot > highestLearnedBallot){
			// FIXME catchup mechanism.
			assert glb.checkpointNumber() <= learned.checkpointNumber() || glb.contains(new CheckpointCommand(learned.checkpointNumber()+1)) 
			   : glb.checkpointNumber() +" ! " + learned.checkpointNumber() +"\t" + this.toString();
		}

		learned.appendAll(glb);
		
		for(int a: ballotArray.get(ballot).keySet()){
			ballotArray.get(ballot).get(a).removeAll(glb);
		}
		
		learned.checkpoint();
		
		if(ballot>highestLearnedBallot){
			highestLearnedBallot = ballot;
			purge();
		}
		
		return glb;
	}

	/**
	 * Purge the ballot array up to <i>highestLearnedBallot</i> not included.
	 * 
	 * @param ballot
	 */
	private void purge(){
		
		toRemove.clear();
		
		for(int b : ballotArray.keySet()){
			if(b>=highestLearnedBallot) break;
			toRemove.add(b);
		}
		
		for(int b : toRemove){
			ballotArray.remove(b);
			writeQUorums.remove(b);
		}
	}
	
}
