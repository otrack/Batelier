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

package net.sourceforge.fractal.replication.database.certificationprotocol.genuine;

import static net.sourceforge.fractal.replication.database.PSTORE.MAX_TRANSACTION_RETRIES;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.UMessage;
import net.sourceforge.fractal.ftwanamcast.FTWanAMCastStream;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.replication.database.CertificationProtocol;
import net.sourceforge.fractal.replication.database.Defrec;
import net.sourceforge.fractal.replication.database.PSTORE;
import net.sourceforge.fractal.replication.database.TpcbTransaction;
import net.sourceforge.fractal.replication.database.PSTORE.RESULT;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.ThreadPool;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;
import net.sourceforge.fractal.wanabcast.WanABCastStream;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;

/**
 * 
 * @author Nicolas Schiper
 * @author Pierre Sutra
 *
 */
public class GenuineCertificationProtocol extends CertificationProtocol implements Learner {    

	
	private static ValueRecorder certifyQSize;
	static{
		if(ConstantPool.TEST_DL>0){
			certifyQSize = new ValueRecorder("GenuineTerminationProtocol#certifyQSize");
		}
	}
	
	
	
	//
	// AMCast and RMCast variables
	//
	
	private int algType;
	private WanABCastStream wanab;
	private WanAMCastStream wanam;
	private FTWanAMCastStream ftwanam;
	private MulticastStream rmcast; // for testing purposes
	
	private MulticastStream voteStream;
	private VoteMessageLearner learner;
	
	//
	// Certification variables
	//

	// Queue storing A-Delivered TPCB transactions
	private Queue<TpcbTransaction> certifyQ;
		
	private final int NB_CERTIFERS=100;
	private ThreadPool certifierThreads;
	private Map<TpcbTransaction,RESULT> results;
	
	//
	// Other variables
	//
	
	private PSTORE pstore;
	private Database adb, tdb, bdb;
	
	
	@SuppressWarnings("unchecked")		
	public GenuineCertificationProtocol( PSTORE db, int delayInterGroup, int typeAlgo)  {
		super(db);
		
		assert typeAlgo==0 || typeAlgo==1 || typeAlgo==2 || typeAlgo==3;
		
		pstore=db;
		
		//
		// AMCast and RMCast (vote) streams
		//
		
		voteStream = FractalManager.getInstance().getOrCreateMulticastStream("PSTORE", pstore.group.name());
		learner = new VoteMessageLearner();
		voteStream.registerLearner("VoteMessage", learner);
		
		algType=typeAlgo;
		switch(algType) {// FIXME Tweak the latency of consensus, it currently equals 1
		case 0 :
			wanam = FractalManager.getInstance().getOrCreateWanAMCastStream("PSTORE", pstore.group.name());
			wanam.registerLearner("WanAMCastMessage", this);
			break;
		case 1 : 
			wanab = FractalManager.getInstance().getOrCreateWanABCastStream("PSTORE", pstore.allGroupsNames,pstore.group.name(),delayInterGroup,1);
			wanab.registerLearner("WanABCastIntraGroupMessage", this);
			break;
		case 2 : 
			ftwanam = FractalManager.getInstance().getOrCreateFTWanAMCastStream("PSTORE", pstore.allGroupsNames,pstore.group.name(),delayInterGroup,1);
			ftwanam.registerLearner("FTWanAMCastIntraGroupMessage", this);
			break;
		case 3 : 
			rmcast = FractalManager.getInstance().getOrCreateMulticastStream("PSTORE",pstore.group.name());
			rmcast.registerLearner("WanMessage", this);
			break;	
		default : throw new IllegalArgumentException("Invalid algorithm's type");
		}

		// 
		// Certification
		// 
		certifyQ = new LinkedList<TpcbTransaction>();
		certifierThreads = new ThreadPool(NB_CERTIFERS);
		results = CollectionUtils.newMap();
	}

	@Override
	public void start() {
		certifierThreads.startThreads();
		voteStream.start();
		switch(algType){
		case 0 : wanam.start(); break;
		case 1 : wanab.start(); break;
		case 2 : ftwanam.start(); break;
		case 3 : rmcast.start(); break;
		default : throw new IllegalArgumentException("Invalid algorithm's type");
		}
		
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(true);
		
		try {
			adb = pstore.dbenv.openDatabase(null, "account", null, dbConfig);
			tdb = pstore.dbenv.openDatabase(null, "teller", null, dbConfig);
			bdb = pstore.dbenv.openDatabase(null, "branch", null, dbConfig);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}

	}

	@Override
	public void stop() {
		
		switch(algType){
		case 0 :wanam.unregisterLearner("WanAMCastMessage", this);  wanam.stop(); break;
		case 1 : wanab.unregisterLearner("WanABCastIntraGroupMessage", this); wanab.stop(); break;
		case 2 : ftwanam.unregisterLearner("FTWanAMCastIntraGroupMessage", this);  ftwanam.stop(); break;
		case 3 : rmcast.unregisterLearner("WanMessage", this); rmcast.stop(); break; 
		default : throw new IllegalArgumentException("Invalid algorithm's type");
		}
		
		voteStream.registerLearner("VoteMessage", learner);
		voteStream.stop();
		certifierThreads.stopThreads();
		
		try {
			adb.close();
			tdb.close();		
			bdb.close();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public void learn(Stream s, Serializable value) {
		
		TpcbTransaction t = (TpcbTransaction)((UMessage) value).serializable; // FIXME ugly cast change the behaviour of AMCast protocols

		// Add the transaction to certifyQ, then launch a task on it.
		synchronized(certifyQ){
			if(ConstantPool.TEST_DL>1) System.out.println("Adding transaction "+t+" to certifyQ"+" ("+certifyQ.size()+")");
			certifyQ.offer(t);
		}
		certifierThreads.doTask(new certificationTask(t));
	}
	
	@Override
	public void submit(TpcbTransaction t) {
		assert !t.destGroups().isEmpty();
		if(ConstantPool.TEST_DL>1) System.out.println("Sending transaction "+t+" to "+t.destGroups());
		switch(algType){
			case 0 : wanam.atomicMulticast(t,t.destGroups()); break;
			case 1 : wanab.atomicMulticastNG(t, t.destGroups()); break;
			case 2 : ftwanam.atomicMulticastNG(t, t.destGroups()); break;			
			case 3 : rmcast.multicast(t,t.destGroups()); break;
			default : throw new IllegalArgumentException("Invalid algorithm's type");
		}
	}
	
	//
	// INNER CLASSES AND METHODS
	//
	
	private class certificationTask implements Runnable{
		
		private TpcbTransaction t;
		private long certifyingTime;
		
		public certificationTask(TpcbTransaction transaction){
			t=transaction;
		}
		
		public void run() {
			
			RESULT result = RESULT.UNKNOWN;
			
			// 1 - To start certifying t, wait that no conflicting transaction is being certified
			if(ConstantPool.TEST_DL>1){
				System.out.println("Waiting for "+t);
				certifyingTime=System.currentTimeMillis();
			}
			try{
				while(true){
					synchronized(certifyQ){
						boolean conflictExist=true;
						for(TpcbTransaction t0 : certifyQ){
							if(t0.equals(t)){
								conflictExist=false;
								break;
							}
							if(t0.conflictWith(t)) { // FIXME it should be a read-write conflict only
								conflictExist=true;
								break;
							}
						}
						if(conflictExist){
								certifyQ.wait();
						}else{
							break;
						}
					}
				}
			}catch(InterruptedException e){
				throw new RuntimeException("Interrupted while waiting for transaction "+t);
			}
			
			// 2 - Certification test
			if(ConstantPool.TEST_DL>1) System.out.println("Certifying "+t+" isLocal? "+pstore.partitionner.isLocal(t));
			// 2.1 - Local certification test
			for(int i=0; i<MAX_TRANSACTION_RETRIES; i++){
				result = certify(t);
				if(result!=RESULT.UNKNOWN) break;
			}
			if (result == RESULT.UNKNOWN) 
				throw new RuntimeException("Unable to certify transaction: " + t.getID() + " exiting...");
			// .2.2 - Global certification test
			try{
				if(!pstore.partitionner.isLocal(t)) result = vote(t,result);
			}catch(InterruptedException e){
				throw new RuntimeException("Interrupted while waiting votes for transaction "+t);
			}
			assert result!=RESULT.UNKNOWN;
			
			// 3 - If t commits, update the timestamps, and wait that t is the first in certifyQ before applying updates.
			//     That t commits or not, remove t from certifyQ.
			if(ConstantPool.TEST_DL>1)  System.out.println("Certification done for transaction "+t+" ; result is "+result);
			if(result==RESULT.COMMIT){
				try{
					boolean isFirst=false;
					while(!isFirst){
						synchronized (certifyQ) {
							isFirst = certifyQ.peek().equals(t);
							if(!isFirst) certifyQ.wait();
						}
					}
				}catch(InterruptedException e){
						throw new RuntimeException("Interrupted while waiting for transaction "+t);
				}
				commit(t);
			}else{	
				abort(t);
			}
			if(ConstantPool.TEST_DL>1){
				System.out.println("Time to certify transaction "+t+" is "+(System.currentTimeMillis()-certifyingTime)+" ms");
			}
			synchronized (certifyQ) {
				assert certifyQ.contains(t) && (certifyQ.peek().equals(t) || result==RESULT.ABORT): t+"\t"+certifyQ;
				certifyQ.remove(t);
				if(!certifyQ.isEmpty()){
					certifyQ.notifyAll();
				}
				if(ConstantPool.TEST_DL>0){
					 certifyQSize.add(certifyQ.size());
						if(ConstantPool.TEST_DL>1){
							System.out.println("Transaction "+t+"  removed from certifyQ ; notifying "
												+ (certifyQ.peek() == null ? "nobody ": certifyQ.peek())
												+" ("+certifyQ.size()+")");
						}
				}
			}
		}
		
		/**
		 * Send vote <i>result</i> to remote sites, and wait a voting quorum.
		 * This function return the result of the vote on <i>t</i>.
		 * 
		 * @param t
		 * @param result 
		 * @return the result of the vote.
		 * @throws InterruptedException 
		 */
		private RESULT vote(TpcbTransaction t, RESULT result) throws InterruptedException{
				
			if(ConstantPool.TEST_DL>1) System.out.println("Setting local votes");
			Vote v = new Vote(t.getID());
			
			if (pstore.partitionner.isLocalAccount(t.getAccID()))
				v.setAccountVote(result);

			if (pstore.partitionner.isLocalTeller(t.getTellerID()))
				v.setTellerVote(result);

			if (pstore.partitionner.isLocalBranch(t.getBranchID()))
				v.setBranchVote(result);

			if(ConstantPool.TEST_DL>1) System.out.println("Sending vote "+v+ " on transaction "+t);
			VoteMessage msg = new VoteMessage(v, pstore.partitionner.groupsReplicating(t.getAccID(), t.getTellerID(), t.getBranchID()), 
											  String.valueOf(pstore.groupID), pstore.swID); 
			voteStream.multicast(msg);
			return  Vote.waitCompleteVoteFor(t.getID());
		}
		
		/**
		 * Return the certification test of transaction <i>t</i> .
		 * 
		 * @param deliveredTransaction
		 * @return the result of the certification test.
		 */
		private RESULT certify(TpcbTransaction t){

			int a_ts = -1, t_ts = -1, b_ts = -1;
			
			Transaction bdbt= null;
			Cursor cursor = null;
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry data = new DatabaseEntry();
			Defrec rec = new Defrec();
			data.setData(rec.data);
			
			try{
				bdbt = pstore.dbenv.beginTransaction(null, null);
			}catch(DatabaseException e){
				return RESULT.UNKNOWN;
			}

			// Account record
			if (pstore.partitionner.isLocalAccount(t.getAccID())) {
				try{
					cursor = adb.openCursor(bdbt, null);
					key.setRecordNumber(t.getAccID());
					OperationStatus result = cursor.getSearchKey(key, data, LockMode.RMW);
					if ( result != OperationStatus.SUCCESS)  throw new DatabaseException(result.toString());
					a_ts = rec.get_ts();
					cursor.close();
				}catch(DatabaseException e){
					if(ConstantPool.TEST_DL>1) System.out.println("Getting account failed while certifying transaction "+t+"; reason "+e.getMessage());
					try{
						cursor.close();
						bdbt.abort();
					}catch(Exception f){
						f.printStackTrace();
					}
					return RESULT.UNKNOWN;
				}
			}

			// teller record
			if (pstore.partitionner.isLocalTeller(t.getTellerID())) {
				try{
					cursor = tdb.openCursor(bdbt, null);
					key.setRecordNumber(t.getTellerID());
					OperationStatus result = cursor.getSearchKey(key, data, LockMode.RMW); 
					if ( result != OperationStatus.SUCCESS) throw new DatabaseException(result.toString());
					t_ts = rec.get_ts();
					cursor.close();
				}catch(DatabaseException e){
					if(ConstantPool.TEST_DL>1) System.out.println("Getting teller failed while certifying transaction "+t.getID()+"; reason "+e.getMessage());
					try{
						cursor.close();
						bdbt.abort();
					}catch(Exception f){
						f.printStackTrace();
					}
					return RESULT.UNKNOWN;
				}
			}

			// Branch record
			if (pstore.partitionner.isLocalBranch(t.getBranchID())) {
				try{
					cursor = bdb.openCursor(bdbt, null);
					key.setRecordNumber(t.getBranchID());
					OperationStatus result = cursor.getSearchKey(key, data, LockMode.RMW); 
					if ( result != OperationStatus.SUCCESS) throw new DatabaseException(result.toString());
					b_ts = rec.get_ts();
					cursor.close();
				}catch(DatabaseException e){
					if(ConstantPool.TEST_DL>1) System.out.println("Getting branch failed while certifying transaction "+t.getID()+"; reason "+e.getMessage());
					try{
						cursor.close();
						bdbt.abort();
					}catch(Exception f){
						f.printStackTrace();
					}
					return RESULT.UNKNOWN;
				}
			}
			
			try{
				bdbt.commit();
			}catch(DatabaseException e){
				if(ConstantPool.TEST_DL>1) System.out.println("Commmitting transaction "+t.getID()+" failed during certification");
				return RESULT.UNKNOWN;
			}

		
			boolean passedCertification = true;
			
			if (a_ts != -1)
				passedCertification &= (a_ts == t.getAccTS());
			if (t_ts != -1)
				passedCertification &= (t_ts == t.getTellerTS());
			if (b_ts != -1)
				passedCertification &= (b_ts == t.getBranchTS());
			
			if(ConstantPool.TEST_DL>1){
				System.out.println("account:"+t.getAccID()+",  "
												+"account_ts:"+a_ts+", "
												+"teller:"+t.getTellerID()+",  "
												+"teller_ts:"+t_ts+" , "
												+"branch:"+t.getBranchID()+",  "
												+"branch_ts:"+b_ts);	
						
			}
			
			if (passedCertification)
				return RESULT.COMMIT;
			else
				return RESULT.ABORT;
			
		}
			
	}
	
}

