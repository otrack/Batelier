package net.sourceforge.fractal.consensus.gpaxos;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.MutedStream;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream.RECOVERY;
import net.sourceforge.fractal.consensus.gpaxos.cstruct.BallotArray;
import net.sourceforge.fractal.consensus.gpaxos.cstruct.CStruct;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.replication.Command;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

/**
 * 
 * @author Pierre Sutra
 * 
 * An acceptor a joins a ballot m only if there exists a m-quorum Q s.t. a belongs to Q. 
 *
 */

public class GPAcceptor extends MutedStream implements Runnable{

	private static TimeRecorder phase1BTime;
	private static TimeRecorder phase2BStartTime;
	private static TimeRecorder phase2BClassicTime;
	private static TimeRecorder phase2BFastTime;
	private static TimeRecorder oneStepTime1;
	private static TimeRecorder oneStepTime2;
	private static TimeRecorder oneStepTime3;
	
	private static ValueRecorder phase2BFastSize;
	private static ValueRecorder oneStepSize;
	private static ValueRecorder acceptedSize;

	static{
		if( ConstantPool.PAXOS_DL > 1 ){	
			phase1BTime = new TimeRecorder("GPAcceptor#phase1B()");
			phase2BStartTime = new TimeRecorder("GPAcceptor#phase2BStart()");
			phase2BClassicTime = new TimeRecorder("GPAcceptor#phase2BClassic()");
			phase2BFastTime = new TimeRecorder("GPAcceptor#phase2BFast()");
			oneStepTime1 = new TimeRecorder("GPAcceptor#oneStep()LeftLub");
			oneStepTime2 = new TimeRecorder("GPAcceptor#oneStep()Append");
			oneStepTime3 = new TimeRecorder("GPAcceptor#oneStep()Send");
			
			phase2BFastSize = new ValueRecorder("GPAcceptor#phase2BFastSize");
			phase2BFastSize.setFormat("%m/%a/%M");
			oneStepSize = new ValueRecorder("GPAcceptor#oneStep()size");
			oneStepSize.setFormat("%m/%a/%M");
			acceptedSize  = new ValueRecorder("GPAcceptor#acceptedSize");
			acceptedSize.setFormat("%m/%a/%M");
			
		}
	}

	private int swid;
	private Group agroup, lgroup;
	private GPaxosStream stream;	
	private boolean isTerminated;
	private BlockingQueue<Message> msgQueue;
	private Thread GPAcceptorThread;

	private BallotArray ballotArray;
	private int currentBallot;
	private int lastBallot; // The last ballot at which the proposer accepted a non-null cstruct
	private CStruct lastCStructAccepted;
	
	private Map<Integer,LinkedHashSet<Command>> checkpoints;
	private int highestCheckpoint;
	
	public GPAcceptor(int id, Group ag, Group lg, GPaxosStream s, boolean useFastBallot, RECOVERY recovery, int chkSize){
		stream = s;
		agroup = ag;
		lgroup = lg;
		swid = id;
		msgQueue =  CollectionUtils.newBlockingQueue();
		GPAcceptorThread = new Thread(this,"GPaxosAcceptorThread@"+swid);
		isTerminated = true;

		currentBallot = 0;
		lastBallot = 0;
		lastCStructAccepted = stream.cstructFactory().newCStruct();
		
		ballotArray = new BallotArray(s.cstructFactory(),agroup.members(),useFastBallot,recovery);
		
		checkpoints = new LinkedHashMap<Integer, LinkedHashSet<Command>>(3, 0.75f, true) {
			private static final long serialVersionUID = 1L;

			@SuppressWarnings("unchecked")
			protected boolean removeEldestEntry(Map.Entry eldest) {
				return size() > 3;
			}
		};
		
		checkpoints.put(0, new LinkedHashSet<Command>());
		highestCheckpoint=0;
		
		agroup.registerQueue("GPMessage1A", msgQueue);
		agroup.registerQueue("GPMessage2AStart", msgQueue);
		agroup.registerQueue("GPMessage2AClassic", msgQueue);

	}

	@Override
	public void start() {
		if(!isTerminated) throw new RuntimeException("Invalid usage");
		isTerminated = false;
		if( GPAcceptorThread.getState()==Thread.State.NEW )
			GPAcceptorThread.start();
	}

	@Override
	public void stop() {
		if(isTerminated) throw new RuntimeException("Invalid usage");
		isTerminated = true;
		GPAcceptorThread.interrupt();
		try {
			GPAcceptorThread.join();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		agroup.unregisterQueue("GPMessage1A", msgQueue);
		agroup.unregisterQueue("GPMessage2AStart", msgQueue);
		agroup.unregisterQueue("GPMessage2AClassic", msgQueue);
	}

	public void run() {

		try {

			while(!isTerminated){

				Message m;

				m = msgQueue.take();

				if( m instanceof GPMessage1A ){
					
					phase1B((GPMessage1A)m);
					
				}else if( m instanceof GPMessage2AStart){
					
					phase2BStart((GPMessage2AStart)m);

				}else if( m instanceof GPMessage2AClassic){
					
					phase2BClassic((GPMessage2AClassic)m);
					
				}

			}

		} catch (InterruptedException e) {
			if( !isTerminated ){
				e.printStackTrace();
				throw new RuntimeException(this + " thread "+Thread.currentThread().getName()
						+ " interrupted while waiting");
			}
		}

	}

	public synchronized void phase2BFast(final Set<Command> proposed) {

		if(isTerminated) return;
		
		if( 
			ballotArray.isFast(currentBallot)
			&&
			lastBallot == currentBallot
		){

			if( ConstantPool.PAXOS_DL > 1 ) phase2BFastTime.start();

			ArrayList<Command> toSend = newlyProposed(lastCStructAccepted.retainsAll(proposed));
			if(toSend.isEmpty()) return;
			lastCStructAccepted.appendAll(toSend);
			checkpoint(lastCStructAccepted);

			if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I send a GPMessage2BFast");
			if( ConstantPool.PAXOS_DL > 3 ) System.out.println(this+" It contains "+toSend);
			ByteBuffer bb = Message.pack(new GPMessage2BFast(swid, currentBallot, toSend),swid);
			lgroup.broadcast(bb);
			agroup.broadcast(bb);
			if( ConstantPool.PAXOS_DL > 1 ) phase2BFastSize.add(toSend.size());

			if( ConstantPool.PAXOS_DL > 1 ) phase2BFastTime.stop();

		}

	}

	public boolean needCheckpoint(int checkpointSize) {
		return lastCStructAccepted.size()>checkpointSize;
	}

	public synchronized CStruct twoStepRceovery(int ballot){
		
		if( joinBallot(ballot) ){
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I two-step recovered ballot "+currentBallot);
			return stream.cstructFactory().clone(lastCStructAccepted);	
		}
		
		return null;
		
	}
	
	public synchronized boolean oneStepRecover(int ballot, CStruct acceptedByCoordinator, CStruct learned){

		assert ballotArray.coordinatorOf(ballot)==swid  || acceptedByCoordinator!= null;
		
		if(  ballot != currentBallot
			 || currentBallot != lastBallot	
			 || ! ballotArray.writeQuorumOf(ballot+1).contains(swid)
			 || ! ballotArray.isFast(ballot+1)
			) return false;

		joinBallot(currentBallot+1);
		
		if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I one-step recover ballot "); 
		if( ConstantPool.PAXOS_DL > 3 ) System.out.println(this+" with "+acceptedByCoordinator);
		if( ConstantPool.PAXOS_DL > 3 ) System.out.println(this+" and "+learned);
		if( ConstantPool.PAXOS_DL > 3 ) System.out.println(this+" current state "+lastCStructAccepted);
		
		if( ConstantPool.PAXOS_DL > 1 )  oneStepTime1.start();
		CStruct llub = null;
		if(ballotArray.coordinatorOf(ballot)!=swid){
			if(lastCStructAccepted.checkpointNumber()<acceptedByCoordinator.checkpointNumber()){
				assert acceptedByCoordinator.checkpointNumber()-1 == lastCStructAccepted.checkpointNumber();
				lastCStructAccepted = stream.cstructFactory().clone(learned);
				llub = acceptedByCoordinator;
				lastCStructAccepted.appendAll(acceptedByCoordinator);
			}else{
				lastCStructAccepted.removeAll(learned);
				if( ConstantPool.PAXOS_DL > 3 ) System.out.println(this+" after remove  (lastCStructAccepted) "+ acceptedByCoordinator);
				if( lastCStructAccepted.checkpointNumber() > acceptedByCoordinator.checkpointNumber() ){
					assert acceptedByCoordinator.checkpointNumber()+1 == lastCStructAccepted.checkpointNumber();
					assert checkpoints.containsKey(acceptedByCoordinator.checkpointNumber());
					acceptedByCoordinator.removeAll(checkpoints.get(acceptedByCoordinator.checkpointNumber()));
					if( ConstantPool.PAXOS_DL > 3 ) System.out.println(this+" after remove (acceptedByCoord)"+ acceptedByCoordinator);
				}
				llub = stream.cstructFactory().leftLubOf(acceptedByCoordinator, lastCStructAccepted);
				lastCStructAccepted = stream.cstructFactory().clone(learned);
				lastCStructAccepted.appendAll(llub);
			}
		}else{
			llub = stream.cstructFactory().clone(lastCStructAccepted);
			llub.removeAll(learned);
		}
		if( ConstantPool.PAXOS_DL > 1 )  oneStepTime1.stop();
		
		if( ConstantPool.PAXOS_DL > 1 )  oneStepTime2.start();
		lastBallot=currentBallot;
		checkpoint(lastCStructAccepted);
		checkpoint(llub);
		ArrayList<Command> toSend = newlyProposed(new ArrayList<Command>(stream.proposed()));
		lastCStructAccepted.appendAll(toSend);
		llub.appendAll(toSend);
		checkpoint(lastCStructAccepted);
		checkpoint(llub);
		if( ConstantPool.PAXOS_DL > 1 )  oneStepTime2.stop();
		
		if( ConstantPool.PAXOS_DL > 1 )  oneStepTime3.start();
		if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I send a GPMessage2BStart");
		if( ConstantPool.PAXOS_DL > 3 ) System.out.println(this+" It contains "+lastCStructAccepted);
		if( ConstantPool.PAXOS_DL > 1 ) oneStepSize.add(lastCStructAccepted.size());
		ByteBuffer bb = Message.pack(new GPMessage2BStart(swid, currentBallot,llub),swid); 
		lgroup.broadcast(bb);
		agroup.broadcast(bb);
		if( ConstantPool.PAXOS_DL > 1 )  oneStepTime3.stop();
		
		return true;
		
	}
	
	public int getCurrentBallot() {
		return currentBallot;
	}
	
	@Override
	public String toString(){
		return "GPAcceptor@"+swid+":"+System.currentTimeMillis()+" At ballot "+currentBallot+",";
	}
	
	//
	// inner methods
	//
	
	/**
	 *  I join only if (1) there exists a (m.ballot)-quorum Q s.t. I belong to Q,
     *                 and (2) I did not join a higher ballot
	 */
	private synchronized void phase1B(GPMessage1A m) {

		if( ConstantPool.PAXOS_DL > 1 ) phase1BTime.start();	
		if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I receive a 1A message from "+m.source);

		if( joinBallot(m.ballot) ){

			if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I send a 1B message");
			if( ConstantPool.PAXOS_DL > 7 ) System.out.println(this+" It contains "+lastCStructAccepted);

			agroup.unicast( m.source, new GPMessage1B(swid, currentBallot, lastBallot, null));

		}

		if( ConstantPool.PAXOS_DL > 1 ) phase1BTime.stop();

	}
	
	/**
	 *  I join only if (1) there exists a (m.ballot)-quorum Q s.t. I belong to Q,
     *                 and (2) I did not join a higher ballot
	 */
	private synchronized void phase2BStart(GPMessage2AStart m) {

		if( ConstantPool.PAXOS_DL > 1 ) phase2BStartTime.start();
		if( ConstantPool.PAXOS_DL > 2) System.out.println(this+" I receive a GPMessage2AStart");
		if( ConstantPool.PAXOS_DL > 7 ) System.out.println(this+" It contains "+m.cstructToAccept); 
		
		if( currentBallot != m.ballot ) joinBallot(m.ballot);
		
		if( currentBallot == m.ballot ){
						
			lastCStructAccepted = m.cstructToAccept;
			lastBallot = currentBallot;
			
			if(ballotArray.isFast(currentBallot)){	
				ArrayList<Command> toSend = newlyProposed(new ArrayList<Command>(stream.proposed()));
				lastCStructAccepted.appendAll(toSend);
				checkpoint(lastCStructAccepted);
			}
			
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I send a GPMessage2BStart");
			if( ConstantPool.PAXOS_DL > 7 ) System.out.println(this+" It contains "+lastCStructAccepted);
			
			ByteBuffer bb = Message.pack(new GPMessage2BStart(swid, currentBallot,lastCStructAccepted),swid);
			lgroup.broadcast(bb);
			agroup.broadcast(bb);

		}else{
			
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I discard this message");
			
		}

		if( ConstantPool.PAXOS_DL > 1 ) phase2BStartTime.stop();

	}

	private synchronized void phase2BClassic(GPMessage2AClassic m) {

		if( ConstantPool.PAXOS_DL > 1 ) phase2BClassicTime.start();
		if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I receive a GPMessage2AClassic");
		if( ConstantPool.PAXOS_DL > 7 ) System.out.println(this+" It contains "+((GPMessage2AClassic)m).cmds);

		if( 
				! ballotArray.isFast(currentBallot)
				&&
				lastBallot == currentBallot
				&&
				currentBallot == m.ballot
		){
		
			lastCStructAccepted.appendAll(((GPMessage2AClassic)m).cmds);
			checkpoint(lastCStructAccepted);
			
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I send a GPMessage2BClassic");
			ByteBuffer bb = Message.pack(new GPMessage2BClassic(swid, currentBallot, ((GPMessage2AClassic)m).cmds),swid);
			lgroup.broadcast(bb);
			agroup.broadcast(bb);

		}else{
			
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I discard this message");

		}

		if( ConstantPool.PAXOS_DL > 1 ) phase2BClassicTime.stop();

	}	
	
	/**
	 * 
	 * Join ballot <i>ballot</i>.
	 * 
	 * @param ballot
	 * 
	 * @return true if this acceptor join this ballot.
	 */
	private boolean joinBallot(int ballot) {
		
		if( currentBallot < ballot && ballotArray.writeQuorumOf(ballot).contains(swid) ){
				
			currentBallot = ballot;

			if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I join ballot "+ ballot);
				
			return true;
			
		}
			
		if( ConstantPool.PAXOS_DL > 2 )
			System.out.println(this+" I do not join ballot "+ ballot+" because "
					+ ( (ballot < currentBallot) ? "I joined a higher ballot":"" )
					+ ( !ballotArray.writeQuorumOf(ballot).contains(swid) ? "I do not belong to any quorum": "IMPOSSIBLE"));
			
		return false;
		
	}
	
	private void checkpoint(CStruct u){
		// FIXME catch-up if I miss checkpoints ??
		LinkedHashSet<Command> checkpoint =  u.checkpoint();
		if( checkpoint !=null && !checkpoints.containsKey(u.checkpointNumber()-1) ){
			assert checkpoints.containsKey(u.checkpointNumber()-2) || u.checkpointNumber() == 1;
			checkpoints.put(u.checkpointNumber()-1, checkpoint);
			highestCheckpoint = u.checkpointNumber() -1;
		}
		
	}
	
	private ArrayList<Command> newlyProposed(ArrayList<Command> proposed) {
		assert lastCStructAccepted.checkpointNumber() ==0 || checkpoints.containsKey(lastCStructAccepted.checkpointNumber()-1):
				     "checkpoint "+ (lastCStructAccepted.checkpointNumber()-1) + " missing ! ";
		ArrayList<Command> toRemove = new ArrayList<Command>();
		for(Command c : proposed){
			boolean toAdd=true;
			for(int i=highestCheckpoint; (i>=highestCheckpoint-3 && i>=0 ); i--){
				LinkedHashSet<Command> checkpoint = checkpoints.get(i);
				if(checkpoint.contains(c)){
					toAdd = false;
					break;
				}
			}
			if(!toAdd)
				toRemove.add(c);
		}
		proposed.removeAll(toRemove);
		return proposed;
	}

	
}
