/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.    
This library is free software; you can redistribute it and/or modify it under     
the terms of the GNU Lesser General Public License as published by the Free Software 	
Foundation; either version 2.1 of the License, or (at your option) any later version.
This library is distributed in the hope that it will be useful, but WITHOUT      
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      
PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      
You should have received a copy of the GNU Lesser General Public License along      
with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use, edit or distribute it.
 */


package net.sourceforge.fractal.replication.database;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.SynchroProcess;
import net.sourceforge.fractal.replication.database.PSTORE.TERMINATION_PROTOCOL_TYPE;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.PerformanceProbe.FloatValueRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

import com.sleepycat.db.DatabaseException;
/**   
* @author P. Sutra
* 
*/

public class TPCB {
	
	private FloatValueRecorder commitTime;
	private FloatValueRecorder commitTimeStdDeviation;
	private FloatValueRecorder commitTimeLocal;
	private FloatValueRecorder commitTimeLocalStdDeviation;
	private FloatValueRecorder commitTimeGlobal;
	private FloatValueRecorder commitTimeGlobalStdDeviation;
	
	private FloatValueRecorder throughput, throughputLocal, throughputGlobal;
	private FloatValueRecorder ratioGlobalTransactions;
	private FloatValueRecorder ratioAbortedTransactions;
	
	private FloatValueRecorder totalTime;
	
	private ValueRecorder nbCommitted, nbCommittedLocal, nbCommittedGlobal;
	private ValueRecorder nbAborted, nbAbortedLocal, nbAbortedGlobal;
	
	private FractalManager fl;
	private PSTORE pstore;
	private Map<Integer,TpcbClient> clients;


	public TPCB(String fractalXMLFile, String dbPath,
			 			int nbBranches, float ratioGlobal, int nbGroups,  int replicationFactor, 
			            int nclients, int ntrans,
			            int algType, int delay)
		   throws FileNotFoundException, DatabaseException{
		
		fl = FractalManager.init(fractalXMLFile,null);
		fl.start();
		
		assert ratioGlobal>=0 && ratioGlobal<=1;
		assert replicationFactor<=nbGroups;
		assert replicationFactor!=nbGroups || ratioGlobal==0;
		
		File dbDirectory = new File(dbPath);
		if(!dbDirectory.isDirectory())
			dbDirectory.mkdir();
		
		pstore = new PSTORE(dbPath, nbBranches, ratioGlobal, nbGroups, replicationFactor, delay, algType, 
											 TERMINATION_PROTOCOL_TYPE.GENUINE);
		
		SynchroProcess.synchronise();
		
		pstore.open();
		pstore.populate();
		clients = CollectionUtils.newMap();
		System.out.println("nb clients "+nclients+" nb trans "+ntrans);
		for (int i=0; i<nclients; i++) {
				clients.put(i,new TpcbClient(i,ntrans,pstore));
		}
		
		throughput = new FloatValueRecorder("TPCB#throughput");
		throughput.setFormat("%t"); // output only the total

		throughputLocal = new FloatValueRecorder("TPCB#throughputLocal");
		throughputLocal.setFormat("%t"); // output only the total

		throughputGlobal = new FloatValueRecorder("TPCB#throughputGlobal");
		throughputGlobal.setFormat("%t"); // output only the total
						
		commitTime = new FloatValueRecorder("TPCB#commitTime");
		commitTime.setFormat("%a"); // output the average commit time (ms)

		commitTimeStdDeviation = new FloatValueRecorder("TPCB#commitTimeStdDeviation");
		commitTimeStdDeviation.setFormat("%a"); // output the standad deviation of the commit time (ms)
				
		commitTimeLocal = new FloatValueRecorder("TPCB#commitTimeLocal");
		commitTimeLocal.setFormat("%a"); // output the average commit time of local transactions (ms)
		
		commitTimeLocalStdDeviation = new FloatValueRecorder("TPCB#commitTimeLocalStdDeviation");
		commitTimeLocalStdDeviation.setFormat("%a"); // output the standad deviation of the commit time of local transactions (ms)		
		
		commitTimeGlobal = new FloatValueRecorder("TPCB#commitTimeGlobal");
		commitTimeGlobal.setFormat("%a"); // output the average commit time of global transactions (ms)
		
		commitTimeGlobalStdDeviation = new FloatValueRecorder("TPCB#commitTimeGlobalStdDeviation");
		commitTimeGlobalStdDeviation.setFormat("%a"); // output the standad deviation of the commit time of global transactions (ms)															
		
		ratioGlobalTransactions = new FloatValueRecorder("TPCB#ratioGlobalTransactions");
		ratioGlobalTransactions.setFormat("%a"); // output the average ratio of global transactions
		
		ratioAbortedTransactions = new FloatValueRecorder("TPCB#ratioAbortedTransactions");
		ratioAbortedTransactions.setFormat("%a"); // output the average abort rate
		
		totalTime = new FloatValueRecorder("TPCB#executionTime");
		totalTime.setFormat("%a");   //output the average of client execution time
		
		nbCommitted = new ValueRecorder("TPCB#nbCommitted");
		nbCommitted.setFormat("%t");
		
		nbCommittedLocal = new ValueRecorder("TPCB#nbCommittedLocal");
		nbCommittedLocal.setFormat("%t");
		
		nbCommittedGlobal = new ValueRecorder("TPCB#nbCommittedGlobal");
		nbCommittedGlobal.setFormat("%t");
		
		nbAborted = new ValueRecorder("TPCB#nbAborted");
		nbAborted.setFormat("%t");
		
		nbAbortedLocal = new ValueRecorder("TPCB#nbAbortedLocal");
		nbAbortedLocal.setFormat("%t");
		
		nbAbortedGlobal = new ValueRecorder("TPCB#nbAbortedGlobal");
		nbAbortedGlobal.setFormat("%t");
	}

	public void launch() {
		
		SynchroProcess.synchronise();
		
		for(TpcbClient c : clients.values())
			c.start();
		
		for(TpcbClient c : clients.values()){
			
			c.join();
		
			c.commitTime.setFormat("%a");
			commitTime.add(Double.valueOf(c.commitTime.toString()));
			throughput.add(1000/Double.valueOf(c.commitTime.toString()));;			
			c.commitTime.setFormat("%d");
			commitTimeStdDeviation.add(Double.valueOf(c.commitTime.toString()));
			c.commitTime.setFormat("%t");
			totalTime.add(Double.valueOf(c.commitTime.toString()));
			
			c.commitTimeLocal.setFormat("%a");
			if(c.commitTimeLocal.toString().equals("0")){
				throughputLocal.add(0);
				commitTimeLocal.add(0);
				commitTimeLocalStdDeviation.add(0);
			}else{
				throughputLocal.add(1000/Double.valueOf(c.commitTimeLocal.toString()));
				commitTimeLocal.add(Double.valueOf(c.commitTimeLocal.toString()));
				c.commitTimeLocal.setFormat("%d");
				commitTimeLocalStdDeviation.add(Double.valueOf(c.commitTimeLocal.toString()));
			}

			c.commitTimeGlobal.setFormat("%a");
			if(c.commitTimeGlobal.toString().equals("0")){
				throughputGlobal.add(0);
				commitTimeGlobal.add(0);
				commitTimeGlobalStdDeviation.add(0);
			}else{
				throughputGlobal.add(1000/Double.valueOf(c.commitTimeGlobal.toString()));
				commitTimeGlobal.add(Double.valueOf(c.commitTimeGlobal.toString()));
				c.commitTimeGlobal.setFormat("%d");
				commitTimeGlobalStdDeviation.add(Double.valueOf(c.commitTimeGlobal.toString()));
			}
																
			nbCommitted.add(Integer.valueOf(c.nbCommittedTransactions.toString()));
			nbCommittedLocal.add(Integer.valueOf(c.nbCommittedLocalTransactions.toString()));
			nbCommittedGlobal.add(Integer.valueOf(c.nbCommittedGlobalTransactions.toString()));
			nbAborted.add(Integer.valueOf(c.nbAbortedTransactions.toString()));
			nbAbortedLocal.add(Integer.valueOf(c.nbAbortedLocalTransactions.toString()));
			nbAbortedGlobal.add(Integer.valueOf(c.nbAbortedGlobalTransactions.toString()));
			
			ratioGlobalTransactions.add(
					( Double.valueOf(c.nbCommittedGlobalTransactions.toString()) +  Double.valueOf(c.nbAbortedGlobalTransactions.toString()) ) 
					/ ( Double.valueOf(c.nbCommittedTransactions.toString()) + Double.valueOf(c.nbAbortedTransactions.toString()) )
					);
			
			ratioAbortedTransactions.add(
					Double.valueOf(c.nbAbortedTransactions.toString())
					/ ( Double.valueOf(c.nbCommittedTransactions.toString()) + Double.valueOf(c.nbAbortedTransactions.toString()) ) 
					);
			
			
			// Comment this if one wants per client statistics 
			// and change output format accordingly
			c.commitTime.release();
			c.commitTimeLocal.release();
			c.commitTimeGlobal.release();
			c.nbGlobalTransactions.release();
			c.nbLocalTransactions.release();
			c.nbAbortedTransactions.release();
			c.nbAbortedLocalTransactions.release();
			c.nbAbortedGlobalTransactions.release();
			c.nbCommittedTransactions.release();
			c.nbCommittedGlobalTransactions.release();
			c.nbCommittedLocalTransactions.release();
			c.nbReadOnlyTransactions.release();
			
		}
		
		SynchroProcess.synchronise();
	}

	public void stop() throws DatabaseException, FileNotFoundException, InterruptedException {
		Thread.sleep(5000); // that the synchronization successfully terminates on every processes.
		pstore.close();
		fl.stop();
	}

}

