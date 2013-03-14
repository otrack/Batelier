

package net.sourceforge.fractal.consensus.paxos;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Vector;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.consensus.paxos.PaxosStream.InstanceKeeper;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.Log;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**   
* @author L. Camargos
* 
*/ 


public class PaxosStreamManager {
    private Map<String,PaxosStream> streams;
    
    //Log
    Log paxosLog;
    private Database logDB;
    
    public PaxosStreamManager() {    	
    	streams = CollectionUtils.newMap();
    	paxosLog = null;
    	new PaxosDatabaseEntry();
    	new PaxosDatabaseKey();
    	
    }
    public PaxosStream getOrCreatePaxosStream(
    		FractalManager manager,
    		String streamName,
			Group accp,
			Group prp,
			Group lrn){
    	
    	if(streams.get(streamName)!=null){
    		return streams.get(streamName);
    	}
    	
    	PaxosStream stream = new PaxosStream(
    							streamName, 
    							manager.membership.myId(),
    							"LEADER_CONSTANT", 
    							accp, 
    							prp,
    							lrn,
    							manager.membership);
    	
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
