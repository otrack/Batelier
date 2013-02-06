package net.sourceforge.fractal.consensus.gpaxos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.MutedStream;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream.RECOVERY;
import net.sourceforge.fractal.consensus.gpaxos.cstruct.BallotArray;
import net.sourceforge.fractal.consensus.gpaxos.cstruct.CStruct;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.replication.CheckpointCommand;
import net.sourceforge.fractal.replication.Command;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.PerformanceProbe.SimpleCounter;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;

/**
 * 
 * @author Pierre Sutra
 *
 *
 * We add checkpoint k when
 * (i) f+1 acceptors learn a cstruct u checkpointed k-1 ,
 * and (ii) the size of u >= checkpointSize
 *  
 * When one add a checkpoint k in a cstruct u,  
 * the cseq before checkpoint k-1  is saved locally, and removed from u.
 *  
 * 
 */

public class GPLeader extends MutedStream implements Runnable{

	private static TimeRecorder startBallotTime;
	private static TimeRecorder phase1ATime;
	private static TimeRecorder phase2AStartTime;
	private static TimeRecorder phase2AClassicTime;
	private static TimeRecorder recoveryTime;
	private static TimeRecorder checkpointingTime;
	
	private static SimpleCounter coordinatedRecoveryCounter;
	private static SimpleCounter timeoutCounter;
	private static SimpleCounter ballotCounter;
	private static SimpleCounter checkpointDone;
	
	static{
		if (ConstantPool.PAXOS_DL > 1){
			startBallotTime = new TimeRecorder("GPLeader#startBalot()");
			phase1ATime = new TimeRecorder("GPLeader#phase1A()");
			phase2AStartTime = new TimeRecorder("GPLeader#phase2AStart()");
			phase2AClassicTime = new TimeRecorder("GPLeader#phase2AClassic()");
			recoveryTime = new TimeRecorder("GPLeader#recovery()");
			checkpointingTime = new TimeRecorder("GPLeader#checkpointing()");
			
			coordinatedRecoveryCounter = new SimpleCounter("GPLeader#coordinatedRecovery");
			timeoutCounter = new SimpleCounter("GPLeader#to");
			ballotCounter = new SimpleCounter("GPLeader#ballots");
			checkpointDone = new SimpleCounter("GPLeader#checkpointDone");
		}
	}
	
	private int swid;
	private Group agroup, lgroup;
	private GPaxosStream stream;
	
	private boolean isTerminated;
	private BlockingQueue<Message> msgQueue;
	private Thread GPLeaderThread;
	
	private int currentBallot; // The ballot I am leading
	private int highestBallotSeen;
	private Set<Integer> quorum1B;
	private CStruct maxTried;
	private BallotArray ballotArray;
	
	private int nextCheckpointNumber;
	private int checkpointSize;
	
	public GPLeader( int id, Group ag, Group lg, GPaxosStream s, 
			         boolean useFastBallot, RECOVERY recovery, int chkSize){
		
		stream = s;
		agroup = ag;
		lgroup = lg;
		swid = id;
				
		isTerminated = true;
		msgQueue =  CollectionUtils.newBlockingQueue();
		GPLeaderThread = new Thread(this,"GPLeaderThread@"+swid);
		
		currentBallot = 0;
		highestBallotSeen = 0;
		maxTried = null;
		quorum1B = new HashSet<Integer>();
		ballotArray = new BallotArray(stream.cstructFactory(), agroup.members(), useFastBallot, recovery);
		
		nextCheckpointNumber = 0;
		checkpointSize=chkSize;
		
		agroup.registerQueue("GPMessage1B", msgQueue);
		agroup.registerQueue("GPMessage2BStart", msgQueue);
		agroup.registerQueue("GPMessage2BClassic", msgQueue);
		agroup.registerQueue("GPMessage2BFast", msgQueue);
		
	}
	
	@Override
	public synchronized void start() {
		if(!isTerminated) throw new RuntimeException("Invalid usage");
		isTerminated = false;
		if(GPLeaderThread.getState()==Thread.State.NEW)
			GPLeaderThread.start();
		if( agroup.isLeading(swid) )
			startNextBallot();
	}

	@Override
	public synchronized void stop() {
		if(isTerminated) throw new RuntimeException("Invalid usage");
		isTerminated = true;
		GPLeaderThread.interrupt();
		try {
			GPLeaderThread.join();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		agroup.unregisterQueue("GPMessage1B", msgQueue);
		agroup.unregisterQueue("GPMessage2BStart", msgQueue);
		agroup.unregisterQueue("GPMessage2BClassic", msgQueue);
		agroup.unregisterQueue("GPMessage2BFast", msgQueue);
	}

	@SuppressWarnings("unchecked")
	public void run() {
		
		GPMessage msg;
		List<Message> lm = new ArrayList<Message>();
		
		try{
		
			while(!isTerminated){
				
				// Start a new ballot if :
				// (i)  I lead the group and ltimeout ms passed wihtout receiving new messages from remote acceptors 
				// or
				// (ii)  I coordinate m.ballot
			    //    and m.ballot  is fast 
				//    and a collision happened at m.ballot
				//    and two-steps recovery mode is on

				msg = (GPMessage)msgQueue.poll(stream.timeout(), TimeUnit.MILLISECONDS);

				if( msg==null ){ 

					if( ConstantPool.PAXOS_DL>2 ) System.out.println( this+" Ballot "+currentBallot+" timed out");
					if( ConstantPool.PAXOS_DL>10 ) System.out.println( this+" Current state "+ballotArray);
					if (ConstantPool.PAXOS_DL >1 ) timeoutCounter.incr();
					if( agroup.isLeading(swid) ) startNextBallot(); // (i)
					continue;				
				}
				
				lm.clear();
				lm.add(msg);
				msgQueue.drainTo(lm);
					
				for(Message messsage : lm){
					
					GPMessage m = (GPMessage) messsage;
					
					if(m.ballot > highestBallotSeen){
						if( ConstantPool.PAXOS_DL > 1 ){
							for(int b=highestBallotSeen; b<m.ballot; b++) ballotCounter.incr();
						}
						highestBallotSeen = m.ballot;
					}

					if(highestBallotSeen>currentBallot && ballotArray.isCoordinatorOf(swid, highestBallotSeen))
						currentBallot=highestBallotSeen;
					
					if(m.ballot<ballotArray.highestLearnedBallot())
						continue;
										
					if( m instanceof GPMessage1B ){
						
						if(m.ballot == currentBallot){
						
							if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" Received a GPMessage1B from "+m.source+ " for ballot "+ ((GPMessage)m).ballot);

							// Already send a Phase2A for this ballot ?
							if( ballotArray.isReadQuorumOf(quorum1B,currentBallot) ){
								if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" I drop this message because I already have a read quorum");
								continue;
							}

							// ballotArray.accept(((GPMessage1B)m).lastBallot, m.source, ((GPMessage1B)m).lastCStructAccepted);

							quorum1B.add(m.source);

							if( ballotArray.isReadQuorumOf(quorum1B,currentBallot) ){
								if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" Got read quorum for ballot "+currentBallot+" "+quorum1B);
								phase2AStart(ballotArray.provedSafe());
							}
							
						}
						
					}else{
						
						if(m instanceof GPMessage2BClassic){
							
							if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" Received a GPMessage2BClassic from "+m.source+ " for ballot "+ m.ballot);
							ballotArray.append(m.ballot , m.source, ((GPMessage2BClassic)m).cmds);
							
						}else if(m instanceof GPMessage2BStart){

							if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" Received a GPMessage2BStart from "+m.source+ " for ballot "+ m.ballot);
							ballotArray.accept(m.ballot , m.source, ((GPMessage2BStart)m).accepted);
								
						}else{

							if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" Received a GPMessage2BFast from "+m.source+ " for ballot "+ m.ballot);
							ballotArray.append(m.ballot , m.source, ((GPMessage2BFast)m).cmds);

						}
						
						if( ConstantPool.PAXOS_DL > 3 ) System.out.println( this+" At ballot "+m.ballot+", current state: "+ballotArray.toString());
						
					}				
										
				} // iterations over lm. 
				
				recovery(stream.getAcceptor().getCurrentBallot());
				
				checkpointing();
				
			} // while(!terminated)
			
		} catch (InterruptedException e) {
			if( !isTerminated ){
				e.printStackTrace();
				throw new RuntimeException(this + " thread "+Thread.currentThread().getName()
						+ " interrupted while waiting");
			}
		}	
	}
	
	public String toString(){
		return "GPLeader@"+swid+":"+System.currentTimeMillis();
	}
	
	public synchronized void phase2AClassic(final Set<Command> newlyProposed) {
		
		if(isTerminated) return;
		
		if( maxTried!=null
			&&
			!ballotArray.isFast(currentBallot) ){
			
			if( ConstantPool.PAXOS_DL > 1 ) phase2AClassicTime.start();
			
			ArrayList<Command> toSend = maxTried.retainsAll(newlyProposed);
			if(toSend.isEmpty()){
				if( ConstantPool.PAXOS_DL > 1 ) phase2AClassicTime.stop();
				return;
			}
			maxTried.appendAll(toSend);
			maxTried.checkpoint();
			
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I send a GPMessage2AClassic for ballot "+currentBallot);
			if( ConstantPool.PAXOS_DL > 7 ) System.out.println(this+" It contains "+ toSend);
			agroup.broadcastTo(new GPMessage2AClassic(swid, currentBallot,toSend), ballotArray.writeQuorumOf(currentBallot));
			
			if( ConstantPool.PAXOS_DL > 1 ) phase2AClassicTime.stop();
			
		}
		
	}
	
	//
	// INNER METHODS
	//
	
	private synchronized void startNextBallot() {
		
		if( ConstantPool.PAXOS_DL > 1 ) startBallotTime.start();

		maxTried=null;
		quorum1B.clear();
		
		if(ballotArray.canTwoStepRecoverAt(currentBallot+1, swid)){ 
			
			CStruct safe = stream.getAcceptor().twoStepRceovery(currentBallot+1);
			
			if(safe!=null){ // yes we can !
				
				currentBallot++;
				ballotCounter.incr();
				highestBallotSeen=currentBallot;
				
				if( ConstantPool.PAXOS_DL > 1 ) coordinatedRecoveryCounter.incr();
				if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I start ballot " + currentBallot+" (true)");
				
				phase2AStart(safe);
				
				if( ConstantPool.PAXOS_DL > 1 ) startBallotTime.stop();
			
				return;
				
			}
			
		}

		// If the two-step recovery fails.
		
		currentBallot = ballotArray.nextBallotToCoordinateAfter(swid, highestBallotSeen);
		if( ConstantPool.PAXOS_DL > 1 ) ballotCounter.incr();
		highestBallotSeen=currentBallot;
		if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I start ballot " + currentBallot+" (false)");

		phase1A();

		if( ConstantPool.PAXOS_DL > 1 ) startBallotTime.stop();

	}		

	private synchronized void phase1A( ){

		if( ConstantPool.PAXOS_DL > 1 ) phase1ATime.start();
		
		if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I send a 1A message for ballot "+currentBallot);
		
		agroup.broadcast(new GPMessage1A(swid, currentBallot));
		// agroup.broadcastTo(new GPMessage1A(swid, currentBallot),ballotArray.writeQuorumOf(currentBallot));

		if( ConstantPool.PAXOS_DL > 1 ) phase1ATime.stop();
		
	}

	private synchronized void phase2AStart(CStruct safe) {

		if( ConstantPool.PAXOS_DL > 1 ) phase2AStartTime.start();

		assert safe != null;
		assert maxTried==null;
		
		maxTried = safe;
		maxTried.appendAll(stream.proposed());
		
		if( ConstantPool.PAXOS_DL > 2 ) System.out.println(this+" I send a GPMessage2AStart for ballot "+currentBallot);
		if( ConstantPool.PAXOS_DL > 7 ) System.out.println(this+" It contains "+ maxTried);
		
		if( ConstantPool.PAXOS_DL > 1 ) phase2AStartTime.stop();
		
		agroup.broadcast(new GPMessage2AStart(swid, currentBallot, maxTried));
		// agroup.broadcastTo(new GPMessage2AStart(swid, currentBallot, maxTried,chk),ballotArray.writeQuorumOf(currentBallot));

	}
	
	private void recovery(int ballot){
		
		if(!ballotArray.isFast(ballot)) return;
		
		if( ConstantPool.PAXOS_DL > 1 ) recoveryTime.start();
		
		if ( stream.getAcceptor().getCurrentBallot() < ballot+1 && ballotArray.canOneStepRecoverAt(ballot,swid) ){
		
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" I see that ballot "+ ballot+ " collides");	
			if( ConstantPool.PAXOS_DL > 7 ) System.out.println( this+" I see that ballot "+ ballot+ " collides; collision = "+ballotArray.acceptedAt(ballot));
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" I try to recover in one step.");
			stream.getAcceptor().oneStepRecover(ballot,
												ballotArray.acceptedAtBy(ballot, ballotArray.coordinatorOf(ballot)),
												 ballotArray.learned());

		}else if( ballotArray.canTwoStepRecoverAt(ballot+1, swid) ){
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" I see that ballot "+ ballot+ " collides");	
			if( ConstantPool.PAXOS_DL > 7 ) System.out.println( this+" I see that ballot "+ ballot+ " collides; collision = "+ballotArray.acceptedAt(ballot));
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" I try to recover in two steps.");
			startNextBallot();
			
			
		}else if( ballotArray.canDefaultRecoverAt(ballot,swid) ){
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" I see that ballot "+ ballot+ " collides");	
			if( ConstantPool.PAXOS_DL > 7 ) System.out.println( this+" I see that ballot "+ ballot+ " collides; collision = "+ballotArray.acceptedAt(ballot));
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" I try to recover in one step.");			
			startNextBallot();
		}

		if( ConstantPool.PAXOS_DL > 1 ) recoveryTime.stop();
		
	}
	
	private void checkpointing(){
		
		if( ConstantPool.PAXOS_DL > 1 ) checkpointingTime.start();
		
		if(ballotArray.learned().checkpointNumber()>nextCheckpointNumber){
			nextCheckpointNumber=ballotArray.learned().checkpointNumber();
		}

		if( ConstantPool.PAXOS_DL > 3 )
			System.out.println( this+" Current state of checkpointing: #" +
					nextCheckpointNumber +
					", isInstalled="+ ballotArray.isInstalled(nextCheckpointNumber) +
					", reached=" + 
					(ballotArray.learned().size()>checkpointSize
					||
					(ballotArray.learned().size()>checkpointSize/2 && nextCheckpointNumber==1) )  );

		if(
				ballotArray.isInstalled(nextCheckpointNumber)
				&&
				(
						ballotArray.learned().size()>checkpointSize
						||
						(ballotArray.learned().size()>checkpointSize/2 && nextCheckpointNumber==1)  
				)
		){
			nextCheckpointNumber++;
			if( ConstantPool.PAXOS_DL > 1 ) checkpointDone.incr();
			if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" I install checkpoint "+nextCheckpointNumber);
			stream.propose(new CheckpointCommand(nextCheckpointNumber));
		}
		
		if( ConstantPool.PAXOS_DL > 1 ) checkpointingTime.stop();
		
	}
	
}
