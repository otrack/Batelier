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
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.multicast.MulticastMessage;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.PerformanceProbe.SimpleCounter;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;

/**   
 * @author N.Schiper
* @author P. Sutra
* 
*/

public class RemoteReader implements Runnable, Learner{

	public final static int ACCOUNT_TABLE = 0;
	public final static int BRANCH_TABLE = ACCOUNT_TABLE + 1;
	
	private static SimpleCounter remoteReadCounter;
	static{
		if(ConstantPool.TEST_DL>0){
			remoteReadCounter = new SimpleCounter("RemoteReader#remoteRead");
		}
	}

	private static MulticastStream rms;

	private PSTORE pstore;
	private Database adb, bdb;
	
	// For each transaction id, the hashmap contains an array of size two of
	// hasmaps, the first one for the account table, and the second one for the branch table.
	// Each one of these hashmaps contain for each id the corresponding tuple of tuples
	// this matrix's rows represent the table type and the columns
	private Map<Long, HashMap<Integer, Tuple>[]> remoteTuples;
	
	private BlockingQueue<MulticastMessage> msgQ;
	private Thread remoteReaderThread;
	private boolean stop;


	public RemoteReader(PSTORE db) throws FileNotFoundException, DatabaseException {

		pstore = db;		
		rms = FractalManager.getInstance().getOrCreateMulticastStream("PSTORE", pstore.group.name());		
		rms.registerLearner("ReadRequestMessage", this);
		rms.registerLearner("ReadReplyMessage", this);
		remoteTuples = new LinkedHashMap<Long, HashMap<Integer, Tuple>[]>(1000,0.75f,true){ 
			private static final long serialVersionUID = 1L;
			@SuppressWarnings("unchecked")
			protected boolean removeEldestEntry(Map.Entry eldest) {
			return size() > 1000.;
			}
		};
		msgQ = CollectionUtils.newBlockingQueue();
		remoteReaderThread = new Thread(this,"RemoteReaderThread");
		stop=true;
	}

	/**
	 * @return two hasmaps, the first one is for ids of the account table
	 *         the second one is for the branch table
	 *         the rwo of the id parameter determines the table type and the columns are the ids requested
	 *	       for that table type.
	 *		   Note that the ids appear in increasing order to avoid deadlocks in the database
	 * @throws InterruptedException 
	 *  
	 */
	@SuppressWarnings("unchecked")
	public HashMap<Integer, Tuple>[] remoteRead(long tid, int[][] ids) throws InterruptedException {

		HashMap<Integer, LinkedList<Integer>[]> group2IDs = new HashMap<Integer, LinkedList<Integer>[]>();
		int groupID;
		LinkedList<Integer>[] groupTupleID;
		
		if(ConstantPool.TEST_DL>0) remoteReadCounter.incr();
		
		//for each tuple requested figure out which group replicates that tuple
		for (int i=0; i<ids.length; i++) {
			for (int j=0; j<ids[i].length; j++) {
				if (i == ACCOUNT_TABLE) {
					groupID = pstore.partitionner.randomGroupReplicatingAccount(ids[ACCOUNT_TABLE][j]);
					assert pstore.allGroupsNames.contains(Integer.toString(groupID)) : ids[BRANCH_TABLE][j] + " => " +groupID;
					groupTupleID = group2IDs.get(groupID);
					if (groupTupleID == null) {
						groupTupleID = (LinkedList<Integer>[]) Array.newInstance(LinkedList.class, 2);
						groupTupleID[ACCOUNT_TABLE] = new LinkedList<Integer>();
						groupTupleID[BRANCH_TABLE] = new LinkedList<Integer>();
					}
					groupTupleID[ACCOUNT_TABLE].add(ids[ACCOUNT_TABLE][j]);
					group2IDs.put(groupID, groupTupleID);
				}
				else if (i == BRANCH_TABLE) {
					groupID = pstore.partitionner.randomGroupReplicatingBranch(ids[BRANCH_TABLE][j]);
					assert pstore.allGroupsNames.contains(Integer.toString(groupID)) : + ids[BRANCH_TABLE][j] + " => " +groupID;
					groupTupleID = group2IDs.get(groupID);
					if (groupTupleID == null) {
						groupTupleID = (LinkedList<Integer>[]) Array.newInstance(LinkedList.class, 2);
						groupTupleID[ACCOUNT_TABLE] = new LinkedList<Integer>();
						groupTupleID[BRANCH_TABLE] = new LinkedList<Integer>();
					}
					groupTupleID[BRANCH_TABLE].add(ids[BRANCH_TABLE][j]);
					group2IDs.put(groupID, groupTupleID);				
				}

			}
		}

		assert !group2IDs.isEmpty();
		
		ArrayList<Object> msg = new ArrayList<Object>();
		
		//send requests to groups
		for (int gid : group2IDs.keySet()) {
			groupTupleID = group2IDs.get(gid);
			Integer[][] ids2Send = {groupTupleID[ACCOUNT_TABLE].toArray(new Integer[0]), 
					groupTupleID[BRANCH_TABLE].toArray(new Integer[0])};
				
			msg.add(0, pstore.swID);
			msg.add(1, tid);
			msg.add(2, ids2Send);
			HashSet<String> dest = new HashSet<String>();
			dest.add(Integer.toString(gid));
			if(ConstantPool.TEST_DL>1) System.out.println("Send read request to "+dest+" for transaction "+tid);
			rms.multicast(new ReadRequestMessage(msg, dest, pstore.group.name(),pstore.swID));
		}

		HashMap<Integer, Tuple>[] result=null;
		
		synchronized(remoteTuples) {
			while(!remoteTuples.containsKey(tid)){
				remoteTuples.wait();
			}
			result = remoteTuples.remove(tid);
		}
		
		if(ConstantPool.TEST_DL>1) System.out.println("Rad request for transaction "+tid+" succeed ");

		return result;
	}

	public void learn(Stream s, Serializable value) {
		msgQ.offer((MulticastMessage)value);
	}
	
	public void run() {
		
		try{
			
			while(!stop){
				
				MulticastMessage msg = msgQ.take();
				
				if(msg instanceof ReadRequestMessage){
			
					if(pstore.group.isLeading(pstore.swID)){ // FIXME not fault-tolerant !
						if(ConstantPool.TEST_DL>1) System.out.println("Got read request from "+((ReadRequestMessage)msg).gSource+" for transaction "+((ReadRequestMessage)msg).getTid());
						boolean isApplied=false;
						for(int i=0; i<PSTORE.MAX_TRANSACTION_RETRIES; i++){
							if(isApplied=reply((ReadRequestMessage)msg)) break;
						}
						if(!isApplied)
							throw new RuntimeException("Unable to send a reply; stopping");
					}
					
				}else{
					
					assert msg instanceof ReadReplyMessage;
					ReadReplyMessage reply = (ReadReplyMessage)msg;
					
					// FIXME in fact we do not want a real AMCast  ... change this!
					if(reply.getNid()==pstore.swID){
						if(ConstantPool.TEST_DL>1) System.out.println("Got read repply from "+msg.gSource+" for transaction "+((ReadReplyMessage)msg).getTid());
						gotReply((ReadReplyMessage)msg);
					}else{
						// this reply is not for me ....
					}
					
				}
				
			}
			
		}catch(InterruptedException e){
			if(!stop){
				System.err.println("Interrupted while waiting for messages");
				e.printStackTrace();
			}
		}
			
	}

	public void start() throws FileNotFoundException, DatabaseException {
		stop=false;
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(true);			
		adb = pstore.dbenv.openDatabase(null, "account", null, dbConfig);
		bdb = pstore.dbenv.openDatabase(null, "branch", null, dbConfig);
		remoteReaderThread.start();
		rms.start();
	}

	public void stop() throws DatabaseException {
		stop=true;
		rms.unregisterLearner("ReadRequestMessage", this);
		rms.unregisterLearner("ReadReplyMessage", this);
		adb.close();
		bdb.close();
		remoteReaderThread.interrupt();
	}
	
	
	//
	// INNER METHODS AND CLASSES
	//
	
	@SuppressWarnings("unchecked")
	private boolean reply(ReadRequestMessage request) {
		
		Integer[] accountIDs = request.getIDs()[RemoteReader.ACCOUNT_TABLE];
		Integer[] branchIDs = request.getIDs()[RemoteReader.BRANCH_TABLE];
	
		HashMap<Integer, Tuple>[] reply = (HashMap<Integer, Tuple>[]) Array.newInstance(HashMap.class, 2);		
		Tuple tuple;

		reply[RemoteReader.ACCOUNT_TABLE] = new HashMap<Integer, Tuple>();
		reply[RemoteReader.BRANCH_TABLE] = new HashMap<Integer, Tuple>();

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
			return false;
		}

		// Account records
		Cursor acurs = null;
		try{
			acurs = adb.openCursor(t, null);
			for (int i=0; i<accountIDs.length; i++) {
				k_dbt.setRecordNumber(accountIDs[i]);
				OperationStatus result = acurs.getSearchKey(k_dbt, d_dbt, null);
				if ( result != OperationStatus.SUCCESS)  throw new DatabaseException(result.toString());
				tuple = new Tuple(rec.get_balance(), rec.get_ts());						
				reply[RemoteReader.ACCOUNT_TABLE].put(accountIDs[i], tuple);
			}
			acurs.close();
		}catch(DatabaseException e){
			if(ConstantPool.TEST_DL>1) System.out.println("Getting accounts: " + accountIDs + " failed while crafting reply for transaction "+request.getTid()+"; reason "+e.getMessage());
			try{
				acurs.close();
				t.abort();
			}catch(Exception f){
				f.printStackTrace();
			}
			return false;
		}


		// Branch records
		Cursor bcurs = null;
		try{
			bcurs = bdb.openCursor(t, null);
			for (int i=0; i<branchIDs.length; i++) {
				k_dbt.setRecordNumber(branchIDs[i]);
				OperationStatus result = bcurs.getSearchKey(k_dbt, d_dbt, null); 
				if ( result != OperationStatus.SUCCESS) throw new DatabaseException(result.toString());
				tuple = new Tuple(rec.get_balance(), rec.get_ts());
				reply[RemoteReader.BRANCH_TABLE].put(branchIDs[i], tuple);
			}
			bcurs.close();
		}catch(DatabaseException e){
			if(ConstantPool.TEST_DL>1) System.out.println("Getting branches: " + branchIDs + " failed while crafting reply for transaction "+request.getTid()+"; reason "+e.getMessage());
			try{
				bcurs.close();
				t.abort();
			}catch(Exception f){
				f.printStackTrace();
			}
			return false;
		}

		
		
		try{
			t.abort();
		}catch(DatabaseException e){
			if(ConstantPool.TEST_DL>1) System.out.println("Commmitting transaction "+request.getTid()+" failed while crafting reply");
			return false;
		}

		ArrayList<Object> serializable = new ArrayList<Object>();
		serializable.add(0, request.getNid());
		serializable.add(1, request.getTid());
		serializable.add(2, reply);
		
		
		if(ConstantPool.TEST_DL>1) System.out.println("Send read repply to "+request.getNid()+" for transaction "+ request.getTid());
		rms.multicast(new ReadReplyMessage(serializable, request.getGid(),pstore.group.name(), pstore.swID));
		
		return true;
		
	}

	private void gotReply(ReadReplyMessage reply){

		assert reply.getNid() == pstore.swID; 
			
		synchronized(remoteTuples) {
			assert !remoteTuples.containsKey(reply.getTid()); // true because a single guy may answer
			remoteTuples.put(reply.getTid(), reply.getValues());
			remoteTuples.notifyAll();
		}

	}
	
}
