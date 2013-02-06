package net.sourceforge.fractal.commit;

import java.io.Externalizable;
import java.util.Vector;

/**   
* @author L. Camargos
* 
*/ 



public interface CommitValue extends Externalizable{
    public enum DataAccessType {RO_TRANSACTION, RW_TRANSACTION};
    public enum DistributionType {LOCAL, GLOBAL};
    static public final int RMListSize = 64;
    
    // should create new CommitValue object, not overwrite the passed v;
    public CommitValue getAbort(CommitValue v);
    
    public byte[] serialize();
    public void deserialize(byte[] ba); //deserialization by overwriting this object
    
    public CommitValue getObject(byte[] ba); //deserialization by creating a new object.

    
    public void setTId(int tid);
    public int getTId(); // global transaction's ID
    
    /**
     * RMList is the list of ResourceManagers involved in a transaction.
     * It is stored as a bitmap, the least meaningful bit is RM No. 0
     * Up to RMListSize managers can be put in the bitmap.
     * ResourceManager's number is its group wide id in the PROPOSER's group in
     * consensus instance used to create the PaxosCommit instance.
     * consensusMainStream.getProposersGroupName();
     * 
     */
    public void setRMList(long dsList);
    public long getRMList();
    public boolean isRMInvolved(int RMid);

    public int getVotingRM();
    public void setVotingRM(int RMid);
    
    /**
     * Returns the ConsensusInstance to be used by resource manager RMid to terminate
     * this transaction.
     * 
     * @param RMid
     * @return isRMInvolved(RMid) == false? -1 : the instance number;
     */
    public int getInstance(int RMid); 
    public int[] getInstanceList();
    public void setInstance(int RMid,int instID);

    /**
     * CommitProposed is true if commit was proposed and false if abort was proposed.
     */
    public boolean isCommitProposed();
    public void setCommitProposed(boolean commit);
        
    /**
     * Returns a vector with updates contained in this commitValue. The actuall
     * meaning of the elements in this vector are application dependent.
     * 
     * @return null if there is no update.
     * 
     */
    public Vector getUpdates();
    public void setUpdates(Vector updates);
    
    public DataAccessType getDataAccessType();
    public void setDataAccessType(DataAccessType dataAccessType);

    public DistributionType getDistributionType();
    public void setDistributionType(DistributionType distributionType);
}