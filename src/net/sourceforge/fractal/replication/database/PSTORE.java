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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.replication.database.certificationprotocol.genuine.GenuineCertificationProtocol;
import net.sourceforge.fractal.replication.database.certificationprotocol.trivial.TrivialCertificationProtocol;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.LockDetectMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;

/**
 * 
 * @author Nicolas Schiper
 * @author Pierre Sutra
 * 
 */

public class PSTORE {
	
	//
	// CLASS FIELDS
	//
	
	public static enum RESULT{
		UNKNOWN,
		COMMIT,
		ABORT
	}
	
	public static enum TERMINATION_PROTOCOL_TYPE{
		TRIVIAL,
		GENUINE
	}
	
	private static ValueRecorder checksum;
	static{
		if(ConstantPool.TEST_DL>0){
			checksum = new ValueRecorder("PSTORE#checksum");
			checksum.setFormat("%t");
			checksum.add(1);
		}
	}
	
	//
	// OBJECT FIELDS
	//
	
	//
	// Database fields
	//
	
	public Environment dbenv;
	private EnvironmentConfig envConfig;
	private String dbPath; 
	private Database adb, bdb, tdb;
	private final static int REGION_SIZE = 1000000;
	private static final int MAX_MUTEXES = 100000;
	private static final int MAX_TRANSACTIONS=  100000;
	
	//
	// TPCB specifics
	//
	
	public static final int RECLEN = 100;
	public static final int HISTORY_LEN = 100;

	//
	// Membership fields
	//
	
	public Collection<Group> allGroups;
	public Collection<String> allGroupsNames;
	public int nodesPerGroups;
	public int swID, groupID;
	public Group group;
	public Partitionner partitionner;

	//
	// Other fields
	//
	
	public static final int MAX_TRANSACTION_RETRIES = 1000;
	private CertificationProtocol terminationProtocol;
	private Map<Integer,Semaphore> clientSemaphores;
	private Map<TpcbTransaction,RESULT> terminatedTransactions;
	private RemoteReader remoteReader;
	
	
	public PSTORE(
			String path, 
			int nbBranches, float percentageGlobal, 
			int ngroups, int replicationFactor, int delayInterGroup, 
			int typeOfAlgo, TERMINATION_PROTOCOL_TYPE tptype)
		    throws FileNotFoundException, DatabaseException{
		
		//
		// Membership initialization
		//
		
		Collection<Group> allGroups = new ArrayList<Group>(FractalManager.getInstance().membership.dispatchPeers(ngroups));
		swID=FractalManager.getInstance().membership.myId();
		nodesPerGroups = allGroups.iterator().next().size(); 
		allGroupsNames = new ArrayList<String>();
		for(Group g : allGroups){
			allGroupsNames.add(g.name());
			if(g.contains(swID)){
				group = g;
				groupID = Integer.valueOf(g.name());
			}
		}
		System.out.println("AllGroups: "+allGroupsNames);
		System.out.println("MyGroup: "+group.name());
		
		partitionner = new Partitionner(groupID, nbBranches, ngroups, replicationFactor,percentageGlobal);

				
		//
		// Initialize Berckeley DB
		//
		
		dbPath = path;
		File home = new File(dbPath);
		envConfig = new EnvironmentConfig();
		envConfig.setErrorStream(System.err);
		envConfig.setErrorPrefix("PSTORE");
		envConfig.setTxnNoSync(true); // true = asynchronous logging 
		envConfig.setLockDetectMode(LockDetectMode.YOUNGEST);
		envConfig.setAllowCreate(true);
		envConfig.setInitializeCache(true);
		envConfig.setTransactional(true);
		envConfig.setInitializeLocking(true);
		envConfig.setInitializeLogging(true);
		envConfig.setMaxLockers(MAX_MUTEXES);
		envConfig.setMaxLocks(MAX_MUTEXES);
		envConfig.setMaxLockObjects( MAX_MUTEXES);
		envConfig.setTxnMaxActive(MAX_TRANSACTIONS);
		// We set the cache at around 1/3 of the size of the database.
		// This models situations were the whole database does not hold entirely in memory.
		int branchSize = Partitionner.TELLERS_PER_BRANCH*100+Partitionner.HISTORY_PER_BRANCH*100+Partitionner.ACCOUNTS_PER_TELLER*Partitionner.TELLERS_PER_BRANCH*100;
		int cacheSize = (branchSize*partitionner.nbLocalBranches())/3;
		envConfig.setCacheSize(cacheSize); 
		System.out.println("SIze of cache: "+(cacheSize/1000)+" KBytes");
		envConfig.setLogRegionSize(REGION_SIZE);
		envConfig.setLogInMemory(false);   // true =  in-memory database
		envConfig.setLogBufferSize(100 * 1024 * 1024);
		
		dbenv = new Environment(home, envConfig);			
		
		//
		// Other initializations
		//
		
		remoteReader = new RemoteReader(this);
		clientSemaphores = CollectionUtils.newConcurrentMap();
		terminatedTransactions = CollectionUtils.newConcurrentMap();
		
		switch(tptype){
		
		case TRIVIAL:
			terminationProtocol = new TrivialCertificationProtocol(this);
			break;
			
		case GENUINE:
			terminationProtocol = new GenuineCertificationProtocol(this, delayInterGroup, typeOfAlgo);
			break;
			
		default:
			throw new IllegalArgumentException("Invalid type of termination protocol");
		
		}
	
	}
	
	public void open() throws FileNotFoundException, DatabaseException {
		
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setType(DatabaseType.HASH);
		dbConfig.setHashNumElements(1000000);
		dbConfig.setAllowCreate(true);
		dbConfig.setTransactional(true);
		adb = dbenv.openDatabase(null, "account", null, dbConfig);
		bdb = dbenv.openDatabase(null, "branch", null, dbConfig);
		tdb = dbenv.openDatabase(null, "teller", null, dbConfig);
		
		terminationProtocol.start();
		remoteReader.start();
	}

	public void close() throws DatabaseException, FileNotFoundException {
		
		remoteReader.stop();
		terminationProtocol.stop();

		adb.close();
		bdb.close();
		tdb.close();
		dbenv.close();
		Environment.remove(new File(dbPath), true, envConfig);

	}

	
	void commit(TpcbTransaction t){
	
			if(ConstantPool.TEST_DL>1) System.out.println("Transaction "+t+" committed; applying updates");
				
			boolean isApplied=false;
						
			for(int i=0; i<MAX_TRANSACTION_RETRIES; i++){
				if(isApplied=applyUpdates(t)) break;
			}
				
			if(!isApplied)
				throw new RuntimeException("Unable to apply a transaction; stopping");
						
			if(ConstantPool.TEST_DL>1){
				System.out.println("Transaction "+t+" updates applied");
			}
			
			if(ConstantPool.TEST_DL>0){
				checksum.add(t.getID()*Long.valueOf(checksum.toString()));
			}
									
			if(t.getNodeID()==swID){
				assert clientSemaphores.containsKey(t.getClientID());
				terminatedTransactions.put(t, RESULT.COMMIT);
				clientSemaphores.get(t.getClientID()).release(1);
			}		
	}
	
	void abort(TpcbTransaction t){
		
		if(ConstantPool.TEST_DL>1) System.out.println("Transaction "+t+" aborted");

		if(t.getNodeID()==swID){
			assert clientSemaphores.containsKey(t.getClientID());
			terminatedTransactions.put(t, RESULT.ABORT);
			clientSemaphores.get(t.getClientID()).release(1);
		}		

	}
	
	
	public RESULT execute(TpcbTransaction t, int clientID) throws  InterruptedException{
		
		if(ConstantPool.TEST_DL>1) System.out.println("Transaction "+t+" executing");
		
		if(!clientSemaphores.containsKey(clientID)) clientSemaphores.put(clientID, new Semaphore(0));
		terminationProtocol.submit(t);
		clientSemaphores.get(clientID).acquire(1);
		assert terminatedTransactions.containsKey(t) && terminatedTransactions.get(t)!= RESULT.UNKNOWN;
		RESULT result = terminatedTransactions.get(t);
		terminatedTransactions.remove(t);
		
		if(ConstantPool.TEST_DL>1) System.out.println("Transaction "+t+" executed");
		
		return result;
		
	}
	
	public HashMap<Integer, Tuple>[] remoteRead(long tid, int[][] ids) throws InterruptedException{	
		return remoteReader.remoteRead(tid, ids);
	}	
    
    /**
	 * Initialize the records of accounts, tellers and branches.
	 * 
     * @throws DatabaseException 
     * @throws FileNotFoundException 
	 */
    public void populate() throws FileNotFoundException, DatabaseException {

		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setType(DatabaseType.HASH);
		dbConfig.setHashNumElements(1000000);
		dbConfig.setAllowCreate(true);
		dbConfig.setTransactional(false);
		Database db;
		
        DatabaseEntry key, data;
        int balance = 0; // default balance for every record
        
        // 1 - Checking if the database is already populated
        
        key= new DatabaseEntry();
        key.setRecordNumber(partitionner.randomLocalAccount());
        OperationStatus status = adb.exists(null,key);
        if(status==OperationStatus.SUCCESS){
        	System.out.println("Database already populated, skipping");
        	return;
        }
    	
		// 2 - Populate accounts
		System.out.println("Populating accounts ...");
		db =  dbenv.openDatabase(null, "account", null, dbConfig);
		for(int i: partitionner.localAccounts()){	        
			key = new DatabaseEntry();
	        key.setRecordNumber(i);

	        Defrec drec = new Defrec();
	        drec.set_balance(balance);
        	drec.set_ts(0); //timestamp added to TPCB tables for the certification protocol
	        data = new DatabaseEntry();
	        data.setData(drec.data);
			
        	db.put(null,key,data);
		}
		db.close();

        // 3 - Populate branches
		System.out.println("Populating branches ...");
		db =  dbenv.openDatabase(null, "branch", null, dbConfig);
		for(int i: partitionner.localBranches()){
			key = new DatabaseEntry();
	        key.setRecordNumber(i);

	        Defrec drec = new Defrec();
	        drec.set_balance(balance);
        	drec.set_ts(0);
	        data = new DatabaseEntry();
	        data.setData(drec.data);
	        
	        db.put(null,key,data);
		}
		db.close();
           
        // 4 - Populate tellers
		System.out.println("Populating tellers ...");
		db =  dbenv.openDatabase(null, "teller", null, dbConfig);
		for(int i: partitionner.localTellers()){
			key = new DatabaseEntry();
	        key.setRecordNumber(i);

	        Defrec drec = new Defrec();
	        drec.set_balance(balance);
        	drec.set_ts(0);
	        data = new DatabaseEntry();
	        data.setData(drec.data);
			
	        db.put(null,key,data);
		}
		db.close();
        
    }
    
    //
    // INNER METHODS
    //
    
    private boolean applyUpdates(TpcbTransaction t) {
		
    	// FIXME append history 
    	
    	Transaction bdbTransaction;
    	Cursor cursor = null;
    	
    	try{
    		bdbTransaction = dbenv.beginTransaction(null, null);    		
    	}catch(DatabaseException e){
    		return false;
    	}
		
		// Increment timestamps and appply updates only if account is local

        DatabaseEntry key= new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
		
		if (partitionner.isLocalAccount(t.getAccID())) {
			key.setRecordNumber(t.getAccID());
			Defrec arec = new Defrec();	
			arec.set_balance(t.getAccBal());
			arec.set_ts(t.getAccTS()+1);
			data.setData(arec.data);
			try{
				cursor= adb.openCursor(bdbTransaction, null);
				cursor.put(key,data);
				cursor.close();
			}catch(DatabaseException e){
				if(ConstantPool.TEST_DL>1) System.err.println("moving cursor acurs failed when applying transaction "+t.getID()+" node: " + swID + " account ID: " + t.getAccID() + " exception: " + e);
				try{
					cursor.close();
					bdbTransaction.abort();
				}catch(Exception f){
					f.printStackTrace();
				}
				return false;
			}
		}

		//only if branch is local
		if (partitionner.isLocalBranch(t.getBranchID())) {
			key.setRecordNumber(t.getBranchID());
			Defrec brec = new Defrec();	
			brec.set_balance(t.getBranchBal());
			brec.set_ts(t.getBranchTS()+1);
			data.setData(brec.data);
			try{
	    		cursor= bdb.openCursor(bdbTransaction, null);
				cursor.put(key,data);
				cursor.close();
			}catch(DatabaseException e){
				if(ConstantPool.TEST_DL>1) System.err.println("moving cursor bcurs failed when applying transaction "+t.getID()+" node: " + swID + " branch ID: " + t.getBranchID() + " exception: " + e);
				try{
					cursor.close();
					bdbTransaction.abort();
				}catch(Exception f){
					f.printStackTrace();
				}
				return false;
			}
		}

		//only if teller is local
		if (partitionner.isLocalTeller(t.getTellerID())) {
			key.setRecordNumber(t.getTellerID());
			Defrec trec = new Defrec();	
			trec.set_balance(t.getTellerBal());
			trec.set_ts(t.getTellerTS()+1);		
			data.setData(trec.data);
			try{
	    		cursor = tdb.openCursor(bdbTransaction, null);
				cursor.put(key,data);
				cursor.close();
			}catch(DatabaseException e){
				if(ConstantPool.TEST_DL>1) System.err.println("moving cursor tcurs failed when applying transaction "+t.getID() +" node: " + swID + " teller ID: " + t.getTellerID() + " exception: " + e);
				try{
					cursor.close();
					bdbTransaction.abort();
				}catch(Exception f){
					f.printStackTrace();
				}
				return false;
			}
		}

		
		try{
			bdbTransaction.commit();
		}catch(DatabaseException e){
			if(ConstantPool.TEST_DL>1) System.out.println("Applying transaction "+t.getTellerID()+" failed ");
			return false;
		}
		
		return true;
	
	}


    
}
