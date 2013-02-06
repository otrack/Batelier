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

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.replication.database.PSTORE.RESULT;
import net.sourceforge.fractal.utils.PerformanceProbe.FloatValueRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.SimpleCounter;

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
public class TpcbClient implements Runnable{

	private final float percentageUpdate = 1f;
	private final boolean DO_LOCAL_WRITE=false; // true = simulate a single BDB node

	//
	// OBJECT FIELDS
	//

	public FloatValueRecorder commitTime;
	public FloatValueRecorder commitTimeLocal;
	public FloatValueRecorder commitTimeGlobal;

	public SimpleCounter nbGlobalTransactions;
	public SimpleCounter nbLocalTransactions;
	public SimpleCounter nbCommittedTransactions;
	public SimpleCounter nbAbortedTransactions;
	public SimpleCounter nbAbortedGlobalTransactions;
	public SimpleCounter nbAbortedLocalTransactions;
	public SimpleCounter nbCommittedGlobalTransactions;
	public SimpleCounter nbCommittedLocalTransactions;
	public SimpleCounter nbReadOnlyTransactions;

	private PSTORE pstore;
	private Random rand;	
	private Database adb, bdb, tdb;
	private int txns,ntxns;
	private Thread clientThread;
	private int clientID;
	private long seq;


	public TpcbClient(int cID, int nbTransactions, PSTORE store) throws FileNotFoundException, DatabaseException{

		clientID=cID;
		pstore = store;

		//each client on each machine can generate up to 10^5 unique transactions
		seq = (pstore.swID * 10000000) + (clientID * 100000);

		txns = 0;
		ntxns = nbTransactions;

		rand = new Random(System.nanoTime());
		clientThread = new Thread(this,"Client"+clientID);

		commitTime = new FloatValueRecorder("Client"+clientID+"#commitTime");
		commitTimeLocal = new FloatValueRecorder("Client"+clientID+"#commitTimeLocal");
		commitTimeGlobal = new FloatValueRecorder("Client"+clientID+"#commitTimeGlobal");
		nbGlobalTransactions = new SimpleCounter("Client"+clientID+"#nbGlobalTransactions");
		nbLocalTransactions = new SimpleCounter("Client"+clientID+"#nbLocalTransactions");
		nbCommittedTransactions = new SimpleCounter("Client"+clientID+"#nbCommittedTransactions");
		nbAbortedTransactions = new SimpleCounter("Client"+clientID+"#nbAbortedTransactions");
		nbAbortedGlobalTransactions = new SimpleCounter("Client"+clientID+"#nbAbortedGlobalTransactions");
		nbAbortedLocalTransactions = new SimpleCounter("Client"+clientID+"#nbAbortedLocalTransactions");
		nbCommittedGlobalTransactions = new SimpleCounter("Client"+clientID+"#nbCommittedGlobalTransactions");
		nbCommittedLocalTransactions = new SimpleCounter("Client"+clientID+"#nbCommittedLocalTransactions");
		nbReadOnlyTransactions = new SimpleCounter("Client"+clientID+"#nbReadOnlyTransactions");

	}

	public void start() {
		clientThread.start();
	}

	public void join() {
		try {
			clientThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.err.println("Interrupted while waiting for client "+clientID);
		}

	}

	public void run() {

		try {

			DatabaseConfig config = new DatabaseConfig();
			config.setTransactional(true);
			adb = pstore.dbenv.openDatabase(null, "account", null, config);
			bdb = pstore.dbenv.openDatabase(null, "branch", null, config);
			tdb = pstore.dbenv.openDatabase(null, "teller", null, config);
			
			if(ConstantPool.TEST_DL>1) System.out.println("Client "+clientID+" starts!");

			while (txns < ntxns) {

				txns++;

				long start = System.nanoTime();
				boolean isWrite = rand.nextFloat()<= percentageUpdate;
				int account =0; // set bellow
				int branch = 0; // set bellow
				int teller = pstore.partitionner.randomLocalTeller(); //teller is always a random teller of the group
				int delta = rand.nextInt(1999998) - 999999;
				
				RESULT result=RESULT.ABORT;
				
				// Re-execute the same transaction until it commits.
				// (this is not the same id because it has to be unique, but account, branch and teller keys are the same)
				for(int j=0;  (j<PSTORE.MAX_TRANSACTION_RETRIES && result == RESULT.ABORT) ; j++){
					
					seq++; 
					
					if(isWrite){
						if(DO_LOCAL_WRITE){ // To simulate a single BDB node.
							account = pstore.partitionner.randomLocalAccount();
							branch = pstore.partitionner.branchOfAccount(account);
							result = newLocalUpdate(seq, account,  branch,  teller, delta);
						}else{
							account = pstore.partitionner.randomAccount();
							branch = pstore.partitionner.branchOfAccount(account);
							result=newUpdate(seq,account, branch, teller,delta);
						}
					}else{ // Query on some random local account.
						account = pstore.partitionner.randomLocalAccount();
						branch = pstore.partitionner.branchOfAccount(account);
						result = newReadOnly(seq,account);
					}
					
					if(result==RESULT.COMMIT){
						nbCommittedTransactions.incr();
						if (pstore.partitionner.isLocalAccount(account)){
							nbCommittedLocalTransactions.incr();
						}else{
							nbCommittedGlobalTransactions.incr();
						}
					}else{
						nbAbortedTransactions.incr();
						if (pstore.partitionner.isLocalAccount(account)){
							nbAbortedLocalTransactions.incr();
						}else{
							nbAbortedGlobalTransactions.incr();
						}

					}
					
				}

				if(result==RESULT.ABORT)
						throw new RuntimeException("Unable to execute a transaction; stopping"); 
				
				long now = System.nanoTime();
				if( txns>ntxns*1/5 )	commitTime.add((now-start)/(double)1000000);
				
				if (pstore.partitionner.isLocalAccount(account)){
					if( txns>ntxns*1/5 ) commitTimeLocal.add((now-start)/(double)1000000);
					nbLocalTransactions.incr();
				}else{
					if( txns>ntxns*1/5) commitTimeGlobal.add((now-start)/(double)1000000);
					nbGlobalTransactions.incr();
				}
				
				if(!isWrite) nbReadOnlyTransactions.incr();
				
				if(ConstantPool.TEST_DL>0){	
					System.out.println("Transaction T"+seq+(result==RESULT.COMMIT ? " commits in " : " aborts in ")
																		+((now-start)/(double)1000000)+" ms ("+pstore.partitionner.isLocalAccount(account)+")");
				}
				
			}

			if(ConstantPool.TEST_DL>0) System.out.println("Client "+clientID+" has terminated");

			// Close the database files.

			adb.close();
			bdb.close();
			tdb.close();
			
		}catch (DatabaseException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
			
	}

	@Override
	public String toString(){
		return "Client"+clientID;
	}


	//
	// INNER METHODS
	//
	
	private RESULT newUpdate(long tid, int account, int branch, int teller, int delta) {
		
		int a_bal = -1, a_ts = -1;    // acount balance and timestamp
		int b_bal = -1, b_ts = -1;    // branch balance and timestamp
		int t_bal = -1, t_ts = -1;     // teller balance and timestamp
		
		TpcbTransaction t = null;
		HashSet<String> destGroups = new HashSet<String>();  //groups to which transaction is multicast

		//
		// 1 - Optimistic execution
		//
		
		HashMap<Integer, Tuple>[] remoteReads = null;
		Transaction bdbt = null;
		Cursor cursor = null;
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		Defrec rec = new Defrec();
		data.setData(rec.data);
		
		try{
			
			bdbt = pstore.dbenv.beginTransaction(null, null);

			// Teller record (always local )
			key.setRecordNumber(teller);
			cursor= tdb.openCursor(bdbt, null);
			if (cursor.getSearchKey(key, data, LockMode.DEFAULT) != OperationStatus.SUCCESS)  throw new DatabaseException("invalid key");
			t_bal = rec.get_balance() + delta;
			t_ts = rec.get_ts();
			cursor.close();
			
			// In TPCB account and branch group are the same
			// so if account is remote, so is the branch
			if(pstore.partitionner.isLocalAccount(account)) {

				// Account record
				key.setRecordNumber(account);
				cursor= adb.openCursor(bdbt, null);
				if (cursor.getSearchKey(key, data, LockMode.DEFAULT) != OperationStatus.SUCCESS) throw new DatabaseException("invalid key");
				a_bal = rec.get_balance() + delta;
				a_ts = rec.get_ts();
				cursor.close();

				// Branch record
				key.setRecordNumber(branch);
				cursor= bdb.openCursor(bdbt, null);
				if (cursor.getSearchKey(key, data, LockMode.DEFAULT) != OperationStatus.SUCCESS) throw new DatabaseException("invalid key");
				b_bal = rec.get_balance() + delta;
				b_ts = rec.get_ts();
				cursor.close();

			}
			
			bdbt.commit();
				
		}catch(DatabaseException e){
			if(ConstantPool.TEST_DL>2) e.printStackTrace();
			try{
				cursor.close();
				bdbt.abort();
			}catch(Exception f){
				f.printStackTrace();
			}
			return RESULT.ABORT;
		}
			
		if(!pstore.partitionner.isLocalAccount(account)) { // remote reads are necessary ?
			int[][] ids = {{account}, {branch}};
			if(ConstantPool.TEST_DL>1) System.out.println( "Requesting remote read for transaction "	+tid+" , account "+account+" and branch "+branch);
			try{
				remoteReads = pstore.remoteRead(tid, ids);
			}catch (Exception e) {
				if(ConstantPool.TEST_DL>2) e.printStackTrace();
				return RESULT.ABORT;
			}
			a_bal = remoteReads[RemoteReader.ACCOUNT_TABLE].get(account).getValue() + delta;
			a_ts = remoteReads[RemoteReader.ACCOUNT_TABLE].get(account).getTS();
			b_bal = remoteReads[RemoteReader.BRANCH_TABLE].get(branch).getValue() + delta;
			b_ts = remoteReads[RemoteReader.BRANCH_TABLE].get(branch).getTS();			
		}

		destGroups = pstore.partitionner.groupsReplicating(account, teller, branch);
		t= new TpcbTransaction(tid, clientID, pstore.swID, account, teller, branch, a_ts, t_ts, b_ts, 
				a_bal, t_bal, b_bal, delta, System.currentTimeMillis(), destGroups, destGroups,destGroups);

		if(ConstantPool.TEST_DL>1){
			System.out.println("Transaction "+t+" optimisitically executed");
			// System.out.println(t.toStringDetailed());
		}

		//
		// 2- Durable execution
		//
		
		try{
			return pstore.execute(t, clientID);
		}catch (InterruptedException e) {
			if(ConstantPool.TEST_DL>2) e.printStackTrace();
			return RESULT.ABORT;
		}
			
	}

	private RESULT newReadOnly(long tid, int account){

		Transaction t = null;

		Defrec rec = new Defrec();

		DatabaseEntry d_dbt = new DatabaseEntry();
		DatabaseEntry k_dbt = new DatabaseEntry();

		byte[] key_bytes = new byte[4];
		k_dbt.setData(key_bytes);
		k_dbt.setSize(4 /* == sizeof(int)*/);

		d_dbt.setData(rec.data);
		d_dbt.setUserBuffer(rec.length(), true);

		try{
			t = pstore.dbenv.beginTransaction(null, null);

		}catch(DatabaseException e){
			System.err .println("Unable to create bdb transaction for transaction "+ tid+"; reason"+e.getMessage()); 
			return RESULT.ABORT;
		}

		k_dbt.setRecordNumber(account);
		Cursor acurs=null;
		try{
			acurs = adb.openCursor(t, null);
			if (acurs.getSearchKey(k_dbt, d_dbt, LockMode.DEFAULT) != OperationStatus.SUCCESS) throw new DatabaseException("invalid key");
			acurs.close();
		}catch(DatabaseException e){
			if(ConstantPool.TEST_DL>1) System.out.println("Getting account failed while generating transaction "+tid+"; reason"+e.getMessage());
			try{
				acurs.close();
				t.abort();
			}catch(Exception f){
				f.printStackTrace();
			}
		}

		try{
			t.commit();
		}catch(DatabaseException e){
			if(ConstantPool.TEST_DL>1) System.out.println("Commmitting bdb transaction for transaction "+tid+" failed ");
			return RESULT.ABORT;
		}

		return RESULT.COMMIT;
	}

	
	private RESULT newLocalUpdate(long tid, int account, int branch, int teller, int delta){

		Transaction t = null;
		
		Defrec rec = new Defrec();
		DatabaseEntry d_dbt = new DatabaseEntry();
		DatabaseEntry k_dbt = new DatabaseEntry();


		byte[] key_bytes = new byte[4];
		k_dbt.setData(key_bytes);
		k_dbt.setSize(4 /* == sizeof(int)*/);

		d_dbt.setData(rec.data);
		d_dbt.setUserBuffer(rec.length(), true);

		try{
			t = pstore.dbenv.beginTransaction(null, null);

		}catch(DatabaseException e){
			System.err .println("Unable to create bdb transaction for transaction "+ tid+"; reason"+e.getMessage()); 
			return null;
		}

		// In TPCB account and branch group are the same
		// so if account is remote, so is the branch			

		// Account record
		k_dbt.setRecordNumber(account);
		Cursor acurs=null;
		try{
			acurs = adb.openCursor(t, null);
			acurs.getSearchKey(k_dbt, d_dbt, null);
			rec.data = d_dbt.getData();
			rec.set_balance(rec.get_balance() + delta);
			d_dbt.setData(rec.data);
			 acurs.put(k_dbt,d_dbt);
			acurs.close();
		}catch(DatabaseException e){
			if(ConstantPool.TEST_DL>1) System.out.println("Getting account failed while generating transaction "+tid+"; reason"+e.getMessage());
			try{
				acurs.close();
				t.abort();
			}catch(Exception f){
				f.printStackTrace();
			}
			return RESULT.ABORT;
		}

		// Branch record
		k_dbt.setRecordNumber(branch);
		Cursor bcurs=null;
		try{
			bcurs = bdb.openCursor(t, null);
			bcurs.getSearchKey(k_dbt, d_dbt, null);
			rec.data = d_dbt.getData();
			rec.set_balance(rec.get_balance() + delta);
			d_dbt.setData(rec.data);
			bcurs.put(k_dbt, d_dbt);
			bcurs.close();
		}catch(DatabaseException e){
			if(ConstantPool.TEST_DL>1) System.out.println("Getting branch failed while generating transaction "+tid+"; reason"+e.getMessage());
			try{
				bcurs.close();
				t.abort();
			}catch(Exception f){
				f.printStackTrace();
			}
			return RESULT.ABORT;
		}

	// Teller record (always local )
	k_dbt.setRecordNumber(teller);
	Cursor tcurs=null;
	try{
		tcurs = tdb.openCursor(t, null);
		tcurs.getSearchKey(k_dbt, d_dbt, null);
		rec.data = d_dbt.getData();
		rec.set_balance(rec.get_balance() + delta);
		d_dbt.setData(rec.data);
		tcurs.put(k_dbt, d_dbt);
		tcurs.close();
	}catch(DatabaseException e){
		if(ConstantPool.TEST_DL>1) System.out.println("Getting teller failed while generating transaction "+tid+"; reason"+e.getMessage());
		try{
			tcurs.close();
			t.abort();
		}catch(Exception f){
			f.printStackTrace();
		}
		return RESULT.ABORT;
	}

	try{
		t.commit();
	}catch(DatabaseException e){
		if(ConstantPool.TEST_DL>1) System.out.println("Commmitting bdb transaction for transaction "+tid+" failed ");
		return RESULT.ABORT;
	}

	return RESULT.COMMIT;
	
}

}