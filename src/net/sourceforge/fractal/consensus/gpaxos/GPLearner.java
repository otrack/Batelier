package net.sourceforge.fractal.consensus.gpaxos;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream.RECOVERY;
import net.sourceforge.fractal.consensus.gpaxos.cstruct.BallotArray;
import net.sourceforge.fractal.consensus.gpaxos.cstruct.CStruct;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

/**
 * 
 * @author Pierre Sutra
 *
 */

public class GPLearner extends Stream implements Runnable{

	private static ValueRecorder learnSize;
	private static TimeRecorder learnTime1,learnTime2,learnTime3;
	
	static{
		if(ConstantPool.PAXOS_DL>1){
			learnSize = new ValueRecorder("GPLearner#learnedSize");
			learnSize.setFormat("%m/%a/%M");
			learnTime1 = new TimeRecorder("GPLearner#learnTimeAccept");
			learnTime2 = new TimeRecorder("GPLearner#learnTimeAppend");
			learnTime3 = new TimeRecorder("GPLearner#learnTimeDeliver");
		}
	}
	
	private int swid;
	private Group agroup, lgroup;
	private GPaxosStream stream;
	private boolean isTerminated;
	private BlockingQueue<Message> msgQueue;
	private Thread GPLearnerThread;
	
	private BallotArray ballotArray;

	// TODO implements a catchup mechanism for checkpoints, and order for 2BF messages
	
	public GPLearner(int id, Group ag, Group lg, GPaxosStream s, boolean useFastBallot, RECOVERY recovery){
		
		stream = s;
		agroup = ag;
		lgroup = lg;
		swid = id;			
		isTerminated = true;
		msgQueue =  CollectionUtils.newBlockingQueue();
		GPLearnerThread = new Thread(this,"GPLearnerThread@"+swid);
		
		ballotArray = new BallotArray(stream.cstructFactory(), agroup.allNodes(), useFastBallot, recovery);
		
		lgroup.registerQueue("GPMessage2BStart", msgQueue);
		lgroup.registerQueue("GPMessage2BClassic", msgQueue);
		lgroup.registerQueue("GPMessage2BFast", msgQueue);
		
	}
	
	@Override
	public void deliver(Serializable s) {
		if( learners.containsKey(s.getClass().getName()) && !learners.get(s.getClass().getName()).isEmpty() ){
			for(Learner l : learners.get(s.getClass().getName())){
				l.learn(this,s);
			}
		}else{
			System.err.println(this+" got a "+ s.getClass().getName() +" to nobody");
		}
	}

	@Override
	public void start() {
		if(!isTerminated) throw new RuntimeException("Invalid usage");
		isTerminated = false;
		if(GPLearnerThread.getState()==Thread.State.NEW)
			GPLearnerThread.start();
	}

	@Override
	public void stop() {
		if(isTerminated) throw new RuntimeException("Invalid usage");
		isTerminated = true;
		GPLearnerThread.interrupt();
		try{
			GPLearnerThread.join();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		lgroup.unregisterQueue("GPMessage2BStart", msgQueue);
		lgroup.unregisterQueue("GPMessage2BClassic", msgQueue);
		lgroup.unregisterQueue("GPMessage2BFast", msgQueue);
	}

	public void run() {
		
		CStruct learned;
		
		try{
			
			while(!isTerminated){
							
				GPMessage m = (GPMessage)msgQueue.take();
				
				if(m.ballot < ballotArray.highestLearnedBallot()) 
					continue;
				
				if(m instanceof GPMessage2BStart){

					if( ConstantPool.PAXOS_DL > 1 ) learnTime1.start();
					if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" Received a GPMessage2BStart from "+m.source+ " for ballot "+ m.ballot);
					learned = ballotArray.accept(m.ballot, m.source, ((GPMessage2BStart)m).accepted);
					if( ConstantPool.PAXOS_DL > 1 ) learnTime1.stop();
					
				}else if(m instanceof GPMessage2BClassic){

					if( ConstantPool.PAXOS_DL > 1 ) learnTime2.start();
					if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" Received a GPMessage2BClassic from "+m.source+ " for ballot "+ m.ballot);
					learned = ballotArray.append(m.ballot, m.source, ((GPMessage2BClassic)m).cmds);
					if( ConstantPool.PAXOS_DL > 1 ) learnTime2.stop();

				}else{

					if( ConstantPool.PAXOS_DL > 1 ) learnTime2.start();
					if( ConstantPool.PAXOS_DL > 2 ) System.out.println( this+" Received a GPMessage2BFast from "+m.source+ " for ballot "+ m.ballot);
					learned = ballotArray.append(m.ballot, m.source, ((GPMessage2BFast)m).cmds);
					if( ConstantPool.PAXOS_DL > 1 ) learnTime2.stop();

				}
				
				if( ConstantPool.PAXOS_DL > 1 ) learnTime3.start();
				if( ConstantPool.PAXOS_DL > 3 ) System.out.println(this+" Something new to learn ? "+ (learned!=null && !learned.isEmpty()) );
				
				if( learned!=null && !learned.isEmpty()){
						stream.deliver(learned);
						if( ConstantPool.PAXOS_DL > 3 ) System.out.println(this+" I learned: "+ learned+" at ballot "+m.ballot);
						if(ConstantPool.PAXOS_DL > 1)  learnSize.add(learned.size());	
				}

				if( ConstantPool.PAXOS_DL > 1 ) learnTime3.stop();
				
			}
			
		} catch (InterruptedException e) {
			if( !isTerminated ){
				e.printStackTrace();
				throw new RuntimeException(this + " thread "+Thread.currentThread().getName()
						+ " interrupted while waiting");
			}
		}	
	}
	
	public String toString(){
		return "GPLearner@"+swid+":"+System.currentTimeMillis();
	}

}