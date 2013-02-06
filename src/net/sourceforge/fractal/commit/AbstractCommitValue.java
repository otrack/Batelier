/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/


package net.sourceforge.fractal.commit;

//TODO: Optimize this class.

/**   
* @author L. Camargos
* 
*/ 


import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Vector;

public abstract class AbstractCommitValue implements Externalizable, CommitValue{
    protected DataAccessType TDataAccessType = DataAccessType.RW_TRANSACTION;
    protected DistributionType TDistributionType = DistributionType.LOCAL;
    
    protected int TId=-1;
    
    protected boolean commitProposed=false;
    
    protected long RMList=0;
    protected int votingRM = 0;

    protected int[] instances=null;
    
    protected Vector vUpdates;

    
    public AbstractCommitValue(){}
    
    public AbstractCommitValue(int tid, boolean commit, long rmList, int[] instancesL, DataAccessType accessType, DistributionType distType, Vector updates)
    {
        TId=tid;
        commitProposed = commit;
        RMList = rmList;
        instances = instancesL;
        TDataAccessType=accessType;
        TDistributionType = distType;
        vUpdates = updates;        
    }

    public AbstractCommitValue(int tid, boolean commit, long rmList, DataAccessType accessType, DistributionType distType, Vector updates)
    {
        TId=tid;
        commitProposed = commit;
        setRMList(rmList);
        TDataAccessType=accessType;
        TDistributionType = distType;
        vUpdates = updates;        
    }
                    
    public long getRMList() {
        return RMList;
    }
    
    public void setVotingRM(int rmID){
        votingRM = rmID;
    }
    
    public int getVotingRM(){
        return votingRM;
    }

    public void setRMList(long list)
    {
        RMList = list;
		int cntr=0;
		for(int i=0;i<RMListSize;i++) if((RMList&(1L<<i))>0) cntr++;
        instances=new int[cntr];
    }

    private int getMyPositionInInstances(int myRMid)
    {
    		if(!isRMInvolved(myRMid)) return -1;
    		int pos=0;
    		for(int i=0;i<myRMid;i++) if((RMList&(1L<<i))>0) pos++;
    		return pos;
    }
    
    public void addToRMList(int RmID)
    {
    		int instID=-1;
    		if(isRMInvolved(RmID))
    		{
    			//setInstance(RmID,instID);
    			return;
    		}
        RMList=RMList | (1L << RmID);
		int[] newInstances=null;
		if(instances!=null) newInstances=new int[instances.length+1];
		else newInstances=new int[1];
		int pos=getMyPositionInInstances(RmID);
    		int i=0;
    		while(i<pos)
    		{
    			newInstances[i]=instances[i];
    			i++;
    		}
    		newInstances[i]=instID;
    		i++;
    		while(i<newInstances.length)
    		{
    			newInstances[i]=instances[i-1];
    			i++;
    		}
    		instances=newInstances;
    }
    
    
    public boolean isRMInvolved(int myRMid)
    {
            return ((1L<<myRMid)&RMList) != 0;
    }

    public int getInstance(int myRMid) // -1 if isRMInvolved is false
    {
    		if(!isRMInvolved(myRMid)) return -1;
		return instances[getMyPositionInInstances(myRMid)];
    }
    
    public int[] getInstanceList()
    {
    		return instances;
    }
    
    public void setInstance(int myRMid,int instID)
    {
		if(!isRMInvolved(myRMid)) return;
		instances[getMyPositionInInstances(myRMid)]=instID;
    }
    
    public DataAccessType getDataAccessType() {
        return TDataAccessType;
    }

    public void setDataAccessType(DataAccessType dataAccessType) {
        TDataAccessType = dataAccessType;
    }

    public DistributionType getDistributionType() {
        return TDistributionType;
    }

    public void setDistributionType(DistributionType distributionType) {
        TDistributionType = distributionType;
    }

    public int getTId() {
        return TId;
    }

    public void setTId(int id) {
        TId = id;
    }

    public Vector getUpdates() {
        return vUpdates;
    }

    public void setUpdates(Vector updates) {
        vUpdates = updates;
    }

    public void setCommitProposed(boolean commitProposed) {
        this.commitProposed = commitProposed;
    }
    
    public boolean isCommitProposed() // true - commit was proposed; false - abort was proposed
    {
        return commitProposed;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//        out.writeObject(TDataAccessType);
        TDataAccessType = (DataAccessType) in.readObject();
//        out.writeObject(TDistributionType);
        TDistributionType = (DistributionType) in.readObject();
//        out.writeInt(TId);
        TId = in.readInt();
//        out.writeBoolean(commitProposed);
        commitProposed = in.readBoolean();
//        out.writeInt(votingRM);
        votingRM = in.readInt();
//        out.writeLong(RMList);
        RMList = in.readLong();
//        out.writeObject(instances);
        instances = (int[]) in.readObject();
//        out.writeObject(vUpdates);
        vUpdates = (Vector) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(TDataAccessType);
        out.writeObject(TDistributionType);
        out.writeInt(TId);
        out.writeBoolean(commitProposed);
        out.writeInt(votingRM);
        out.writeLong(RMList);
        out.writeObject(instances);
        out.writeObject(vUpdates);
    }

	public static boolean unifyRMvotes(CommitValue[] pcvals){
    		for(CommitValue pcval : pcvals)
    			if(pcval!=null && !pcval.isCommitProposed()) return false;
    		return true;
    }
}