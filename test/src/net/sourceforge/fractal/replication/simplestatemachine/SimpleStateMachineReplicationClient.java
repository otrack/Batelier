package net.sourceforge.fractal.replication.simplestatemachine;

import static net.sourceforge.fractal.ConstantPool.TEST_DL;

import java.util.Random;

import net.sourceforge.fractal.replication.CommutativeCommand;
import net.sourceforge.fractal.replication.ReadWriteRegisterCommand;
import net.sourceforge.fractal.replication.ReplicationStream;
import net.sourceforge.fractal.replication.ReplicationStreamClient;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;

/**
 * 
 * @author Pierre Sutra
 *
 */

public class SimpleStateMachineReplicationClient extends ReplicationStreamClient implements Runnable{
	
	private int nbCommands;
	private int clientId;
	private Random rand;
	private int nbRegister;
	
	private Thread SimpleStateMachineReplicationClientThread;

	public TimeRecorder latencyRecorder;
	
	public SimpleStateMachineReplicationClient (int id, ReplicationStream s, int cmds, int reg) {
		super(s);
		nbCommands = cmds;
		nbRegister = reg;
		clientId = id;
		latencyRecorder = new TimeRecorder("Client-"+clientId+"#latency");
		rand = new Random(System.nanoTime()*id);
			
		SimpleStateMachineReplicationClientThread = new Thread(this,"SimpleStateMachineReplicationClientThread#"+clientId);
	}

	public void start(){
		SimpleStateMachineReplicationClientThread.start();
	}
	
	public void join(){
		try {
			SimpleStateMachineReplicationClientThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
	}

	public void run() {
		
		CommutativeCommand c;
		long lat=0;
			
		try {
			Thread.sleep(clientId%10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		for(int i=0; i<nbCommands; i++){
		
			c = new ReadWriteRegisterCommand(clientId,rand.nextInt(nbRegister)+1,rand.nextBoolean());

			if(i>=1000 && i<= 2000 ){
				latencyRecorder.start();
				if(TEST_DL>0){
					lat = System.currentTimeMillis();
					System.out.println("Client "+clientId+" is sending command "+c+" to the state machine");
				}
			}
			
			stream.execute(c, clientId);
			
			if(i>=1000 && i<=2000 ){
				latencyRecorder.stop();
				if(TEST_DL>0){
					System.out.println((c.toString()+":"+(System.currentTimeMillis()-lat)));
				}
			}
			
		}
		
		if(TEST_DL>0){
			System.out.println("Client "+clientId+" finishes.");
		}
	}
	
}
