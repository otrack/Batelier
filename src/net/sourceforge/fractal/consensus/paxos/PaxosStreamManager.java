

package net.sourceforge.fractal.consensus.paxos;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Vector;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.consensus.paxos.PaxosStream.InstanceKeeper;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.FractalUtils;
import net.sourceforge.fractal.utils.Log;
import net.sourceforge.fractal.utils.XMLUtils;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**   
* @author L. Camargos
* 
*/ 


public class PaxosStreamManager {
    private Map<String,PaxosStream> streams;
    
    //Log
    Log paxosLog;
    private File envHomeDirectory;
    private Environment logDBEnv;
    private Database logDB;
    private PaxosDatabaseEntry dbEntry;
    private PaxosDatabaseKey dbKey;
    
    public PaxosStreamManager(String logName) {
    	
    	streams = CollectionUtils.newMap();
    	paxosLog = null;
    	envHomeDirectory = null;
    	dbEntry = new PaxosDatabaseEntry();
    	dbKey = new PaxosDatabaseKey();
    	
        if(ConstantPool.PAXOS_USE_STABLE_STORAGE){
            //Use stable storage for logs.
            if(ConstantPool.PAXOS_USE_BDB){
                //Use BDB
                envHomeDirectory = new File(logName+"BDB");
                if(!ConstantPool.PAXOS_STABLE_STORAGE_RECOVERY){
                    Log.recursiveDelete(envHomeDirectory);
                    envHomeDirectory.mkdir();
                }
                
                try {
                    /* Create an environment */
                    EnvironmentConfig envConfig = new EnvironmentConfig();
                    envConfig.setAllowCreate(true);    
                    envConfig.setTransactional(true);
                    envConfig.setTxnTimeout(0);
                    envConfig.setLockTimeout(0);
                    logDBEnv = new Environment(envHomeDirectory, envConfig);
                    
                    /* Make a database within that environment */
                    Transaction txn = logDBEnv.beginTransaction(null, null);
                    DatabaseConfig dbConfig = new DatabaseConfig();
                    dbConfig.setTransactional(true); 
                    dbConfig.setAllowCreate(true);
                    dbConfig.setSortedDuplicates(false);
                    logDB = logDBEnv.openDatabase(txn,"paxosLog",dbConfig);
                    txn.commitSync();
                    
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException("Error starting BDB!");
                }
            }else{
                throw new RuntimeException("Not implemented yet !");
            }
        }
    }

    public void load(Node config){
        
        String range = XMLUtils.getAttribByName((Element) config, "instantiate");

        String streamName, coordPolicyName, ACC_GRP_name, PRP_GRP_name, LRN_GRP_name;
        
        streamName = XMLUtils.getAttribByName((Element) config, "name");
        
        Element el = (Element) XMLUtils.getChildByName(config, "CoordinatorPolicy");
        coordPolicyName = el.getAttribute("type");
        
        el = (Element) XMLUtils.getChildByName(config, "ACCEPTOR_GROUP");
        ACC_GRP_name = el.getAttribute("name");
        
        el = (Element) XMLUtils.getChildByName(config, "PROPOSER_GROUP");
        PRP_GRP_name = el.getAttribute("name");
        
        el = (Element) XMLUtils.getChildByName(config, "LEARNER_GROUP");
        LRN_GRP_name = el.getAttribute("name");

        if(FractalUtils.inRange(range, FractalManager.getInstance().membership.myId())){
        	streams.put(
        			streamName,
        			new PaxosStream(
        					streamName,
        					FractalManager.getInstance().membership.myId(),
        					coordPolicyName,
        					ACC_GRP_name,
        					PRP_GRP_name,
        					LRN_GRP_name,
        					FractalManager.getInstance().membership));
        	if(ConstantPool.PAXOS_DL > 0) System.out.println("Started Paxos stream " + streamName + " on id " + FractalManager.getInstance().membership.myId());
        }
    }    

    public PaxosStream getOrCreatePaxosStream(
    		String streamName,
			String accGrpName,
			String prpGrpName,
			String lrnGrpName){
    	
    	if(streams.get(streamName)!=null){
    		return streams.get(streamName);
    	}
    	
    	PaxosStream stream = new PaxosStream(
    							streamName, 
    							FractalManager.getInstance().membership.myId(), 
    							"LEADER_CONSTANT", 
    							accGrpName, 
    							prpGrpName,
    							lrnGrpName,
    							FractalManager.getInstance().membership);
    	
    	streams.put(streamName,stream);
		
    	return stream;
    }
    
    public PaxosStream stream(String streamName){
        return streams.get(streamName);
    }

    public void paxosLogThis(String streamName,int accId, InstanceKeeper keeper) throws IOException{//int h_ts_promissed, Integer h_ts_accepted, Serializable accepted,  boolean decided, Serializable decision) throws IOException {
    	throw new RuntimeException("not implemented yet");
    }

    public boolean paxosUnLogThis(String streamName, int s_id, InstanceKeeper keeper) throws IOException {
    	throw new RuntimeException("not implemented yet");
    }

    @SuppressWarnings("unchecked")
	public LogEntry[][] getLog(LogFilter filter) {
        Vector[] log = new Vector[filter.getNumberOfRm()];
        int firstInstance = filter.getFirstInstance();
        
        for(int rm = 0; rm < log.length; rm++ ){
            log[rm] = new Vector<LogEntry>();
        }
        
        //iterate over the log and check with filter.
        Cursor cursor = null;
        try {
            PaxosDatabaseKey foundKey = new PaxosDatabaseKey(filter.getStreamName().getBytes("UTF-8")); 
            PaxosDatabaseEntry foundData = new PaxosDatabaseEntry();

            cursor = logDB.openCursor(null, null);
            
            if(ConstantPool.PAXOS_RECOVERY_DL > 2) System.out.println(" indirect entry ");                            
            
            if( cursor.getSearchKeyRange(foundKey, foundData, LockMode.DEFAULT) != OperationStatus.NOTFOUND) {
                while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                    foundKey.getAllData();
                    foundData.getAllData();
                    
                    CollectionUtils.setElementAt(
                            log[filter.getIndex(foundKey.streamName)], 
                            foundKey.instance - firstInstance, 
                            filter.filterEntry(foundKey.streamName, new LogEntry(foundKey.instance, foundData.ts_promissed, foundData.ts_accepted,foundData.decided, foundData.decided?foundData.decision:foundData.accepted))
                        );
 
                    if(ConstantPool.PAXOS_COMMIT_RECOVERY_DL > 2) System.out.println("log #" + filter.getIndex(foundKey.streamName) + " size "+ log[filter.getIndex(foundKey.streamName)].size() + " had " + (foundKey.instance - firstInstance) + " th pos set to " + filter.filterEntry(foundKey.streamName, new LogEntry(foundKey.instance, foundData.ts_promissed, foundData.ts_accepted,foundData.decided, foundData.decided?foundData.decision:foundData.accepted)));
                }
            }else{
                if(ConstantPool.PAXOS_COMMIT_RECOVERY_DL > 2) System.out.println(" none entry found ");                                
            }
            // Cursors must be closed.
            cursor.close();
        } catch (DatabaseException de) {
            System.err.println("Error accessing database." + de);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        if(ConstantPool.PAXOS_COMMIT_RECOVERY_DL > 2) System.out.println(" copying " + log.length + "logs");
        LogEntry[][] log2 = new LogEntry[log.length][];
        for(int rm = 0; rm < log2.length; rm++ ){
            log2[rm] = new LogEntry[log[rm].size()];
            if(ConstantPool.PAXOS_COMMIT_RECOVERY_DL > 2) System.out.println(" copying log #"+rm + " size " + log[rm].size() + " to array");                                
            System.arraycopy(log[rm].toArray(),0, log2[rm],0,log[rm].size());
//            log2[rm] = (LogEntry[]) log[rm].toArray();
        }

        return log2;
    }
}


class PaxosDatabaseKey extends DatabaseEntry {
    int accId, 
        instance;
    String streamName;
        
    PaxosDatabaseKey(){}
    
    public PaxosDatabaseKey(byte[] bytes) {
        setData(bytes);
    }

    DatabaseEntry setData(String streamName, int accId, int instance) {
            
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream(4096);
        try {
            ObjectOutputStream os = new ObjectOutputStream(byteStream);
            os.flush();
            os.writeUTF(streamName);
            os.writeInt(accId);
            os.writeInt(instance);
            os.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        setData(byteStream.toByteArray());
        return this;
    }

    void getAllData() {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(getData());
        ObjectInputStream is;
        try {
            is = new ObjectInputStream(byteStream);
            streamName = is.readUTF();
            accId = is.readInt();
            instance = is.readInt();
            is.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class PaxosDatabaseEntry extends DatabaseEntry {
    int instance,
        ts_promissed;
    Integer ts_accepted;
    Serializable accepted, decision;
    boolean decided;
    
    PaxosDatabaseEntry(){}
    
//    PaxosDatabaseEntry(int instance, int h_ts_promissed, Integer h_ts_accepted, Serializable accepted) {
//        setData(instance, h_ts_promissed, h_ts_accepted, accepted);
//    }
    
    DatabaseEntry setData(int instance, int h_ts_promissed, Integer h_ts_accepted, Serializable accepted, boolean decided, Serializable decision) {
            
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream(ConstantPool.NETWORK_MAX_DGRAM_SIZE);
        try {
            ObjectOutputStream os = new ObjectOutputStream(byteStream);
            os.flush();
            os.writeInt(instance);
            os.writeBoolean(decided);
        	
        	if(!decided){
                os.writeInt(h_ts_promissed);
                os.writeInt(h_ts_accepted == null ? -1 : h_ts_accepted.intValue());
                os.writeObject(accepted);
            }else{
                os.writeObject(decision);
            }
            os.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        setData(byteStream.toByteArray());
        return this;
    }

    void getAllData() {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(getData());
        ObjectInputStream is;
        try {
            is = new ObjectInputStream(byteStream);
            instance = is.readInt();
            decided = is.readBoolean();
            if(!decided){
                ts_promissed = is.readInt();
                ts_accepted = is.readInt();
                if(ts_accepted.intValue() == -1)
                    ts_accepted = null;
                accepted = (Serializable) is.readObject();
            }else{
                decision = (Serializable) is.readObject();
            }
            
            is.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
