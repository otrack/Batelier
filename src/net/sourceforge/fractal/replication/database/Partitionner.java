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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.MathUtils;


/**
 * 
 * @author Nicolas Schiper
 * @author Pierre Sutra
 * 
 */

public class Partitionner {

	//
	// TPCB PARAMETERS
	//
	
	// Valid scaling
	// public static final int TELLERS_PER_BRANCH = 10;
	// public static final int ACCOUNTS_PER_TELLER = 10000;
	// public static final int HISTORY_PER_BRANCH = 2592000;
    
	// Tiny scaling
	// The default configuration that adheres to TPCB scaling rules requires
	// nearly 3 GB of space.  To avoid requiring that much space for testing,
	// we set the parameters much lower.  If you want to run a valid 10 TPS
	// configuration, uncomment the valid scaling configuration, and comment the tiny scaling configuration.
	public static final int TELLERS_PER_BRANCH = 10;
	public static final int ACCOUNTS_PER_TELLER = 10;
	public static final int HISTORY_PER_BRANCH = 100;
	
	
	//
	// OBJECT FIELDS
	//
	
	private int groupID; // the ID of the group containing this replica
	private Set<Integer> allGroups;
	private Set<Integer> remoteGroups;
	
	private int branches;
	private int tellers;
	private int accounts;
	private float percentageGlobalTransaction;
		
	private int branchesPerPartition;
	private int tellersPerPartition;
	private int accountsPerPartition;

	private Map<Integer,Partition> partitions; // indexed by their id
	private Map<Integer,Set<Partition>> groupsToPartitions;
	
	private Random rand;
	
	public Partitionner(int groupID, int nbBranches, int ngroups, int replicationFactor, float percentageGlobal) throws IllegalArgumentException {
			
		if (nbBranches%ngroups!=0 
			|| ((nbBranches * TELLERS_PER_BRANCH)%ngroups!=0)
			|| ((nbBranches * TELLERS_PER_BRANCH * ACCOUNTS_PER_TELLER)%ngroups!=0))
			throw new IllegalArgumentException("Invalid number of groups");
		
		if ((replicationFactor < 1) || (replicationFactor > ngroups))
			throw new IllegalArgumentException("Invalid replication factor");
		
		if ( groupID<0 || groupID >= ngroups ){
			throw new IllegalArgumentException("Invalid groupID or invalid number of groups");
		}
				
		this.groupID = groupID;
		allGroups = CollectionUtils.newSet();
		remoteGroups = CollectionUtils.newSet();
		for(int i=0; i<ngroups; i++){
			allGroups.add(i);
			if(i!=groupID) remoteGroups.add(i);
		}
		
		branches = nbBranches;
		tellers = branches * TELLERS_PER_BRANCH;
		accounts = tellers * ACCOUNTS_PER_TELLER;
		percentageGlobalTransaction = percentageGlobal;
		
		rand = new Random(System.currentTimeMillis());


		// 1 - Construct partitions
		//
		//     We divide the whole database in nbGroups equal share.
		//     Each share is called a partition.
		//	   Partitions are numbere from 0 to nbGroups-1.

		branchesPerPartition = (int) (branches / ngroups);
		tellersPerPartition = branchesPerPartition * TELLERS_PER_BRANCH;		
		accountsPerPartition = branchesPerPartition * TELLERS_PER_BRANCH * ACCOUNTS_PER_TELLER;			
			
		partitions = CollectionUtils.newMap();
		
		for(int p=0; p<ngroups;p++){
			partitions.put(
				p,
				new Partition(p,
				  			  p*branchesPerPartition,(p+1)*branchesPerPartition-1,
							  p*branchesPerPartition*TELLERS_PER_BRANCH,(p+1)*branchesPerPartition*TELLERS_PER_BRANCH-1,
							  p*branchesPerPartition*TELLERS_PER_BRANCH*ACCOUNTS_PER_TELLER,(p+1)*branchesPerPartition*TELLERS_PER_BRANCH*ACCOUNTS_PER_TELLER-1));
		}		
		
		// 2 - Ventilate partitions in groups
		//
		//     We dispatch partitions according to the replication factor.
		//     Each group replicates exactly replicationFactor partitions.
		//     Partition p is replicated at group p to (p + replicationFactor - 1) mod nbGroups
		
		groupsToPartitions = CollectionUtils.newMap();
		for(int p=0; p<ngroups; p++){
			for(int g=p; g<(p+replicationFactor); g++){
				if(!groupsToPartitions.containsKey(g%ngroups)) groupsToPartitions.put(g%ngroups, new HashSet<Partition>());
				groupsToPartitions.get(g%ngroups).add(partitions.get(p));
			}
		}
		
		// 3 - Output some info
		
		if(ConstantPool.TEST_DL > 0){
			
			System.out.println("Partitionning information\n"
					+ "\tnumber of partitions : "+ ngroups+"\n"
					+ "\treplication factor: "+ replicationFactor+"\n"
					+ "\taccounts per partition: " + accountsPerPartition+"\n" 
					+ "\tbranches per partition: " + branchesPerPartition + "\n"
					+ "\ttellers per partition: " + tellersPerPartition
					+ "\tpercentage global transaction: "+percentageGlobalTransaction
			);
			
			for(int g: groupsToPartitions.keySet()){
				System.out.println("\tpartitions replicated at group "+g+":");
				for(Partition p : groupsToPartitions.get(g)){
					System.out.println("\t\t"+p);
				}
			}
			
		}
		
	}

	
	//
	// INTERFACES
	//
	
	// Generators
	
	/**
	 * 
	 * @return an "random" account as specified by TPC-B.
	 * 
	 * From TPC-B specifications:
	 * The Account_ID is generated as follows:
	 • A random number X is generated within [0,1]
	 • If X<0.85 or branches = 1, a random Account_ID is selected over all <Branch_ID> accounts.
	 • If X>=0.85 and branches > 1, a random Account_ID is selected over all non-<Branch_ID> accounts.
	 * Comment 1: This algorithm guarantees that, if there is more than one branch in the database, then an
	 * average of 15% of remote transactions is presented to the SUT. Due to statistical variations during a finite
	 * measurement period, the actual measured proportion ofremote transactions may vary around 15%. Actual
	 * measured values must be within 14% to 16% for the set of transactions processed during the measurement
	 * interval (see Clauses 6.1 and 7.2)
	 * 
	 * NB: this does _not_ mean that 15% of the transactions touched a branches outside what is locally replicated.
	 * 
	 */
	public int randomAccount(){
    	if (rand.nextFloat() < percentageGlobalTransaction) 
    		return MathUtils.random_in_set(groupsToPartitions.get(MathUtils.random_in_set(allGroups))).randomAccount();
    	else 
    		return randomLocalAccount();
	}
	
	public int randomLocalAccount(){
		return  MathUtils.random_in_set(groupsToPartitions.get(groupID)).randomAccount();
	}

	public int randomLocalTeller(){
		return  MathUtils.random_in_set(groupsToPartitions.get(groupID)).randomTeller();
	}
	
	public int randomLocalBranch(){
		return  MathUtils.random_in_set(groupsToPartitions.get(groupID)).randomBranch();
	}

	public int randomRemoteAccount(){
		return MathUtils.random_in_set(groupsToPartitions.get(MathUtils.random_in_set(remoteGroups))).randomAccount();
	}

	public int randomRemoteTeller(){
		return MathUtils.random_in_set(groupsToPartitions.get(MathUtils.random_in_set(remoteGroups))).randomTeller();
	}
	
	public int randomRemoteBranch(){
		return MathUtils.random_in_set(groupsToPartitions.get(MathUtils.random_in_set(remoteGroups))).randomBranch();
	}
	
	public int randomGroupReplicatingAccount(int account){
		if( account < 0 || account >= accounts) throw new IllegalArgumentException("invalid account");
		int g = -1;
		do{
			g = MathUtils.random_in_set(groupsToPartitions.keySet());
		}while(!isReplicatingAccount(g, account));
		return g;
	}

	public int randomGroupReplicatingBranch(int branch){
		if( branch < 0 || branch >= branches) throw new IllegalArgumentException("invalid branch");
		int g = -1;
		do{
			g = MathUtils.random_in_set(groupsToPartitions.keySet());
		}while(!isReplicatingBranch(g, branch));
		return g;
	}
	
	// Information 

	public Set<Partition> localPartitions(){
		return groupsToPartitions.get(groupID);
	}
	
	public boolean isLocalAccount(int account){
		return isReplicatingAccount(groupID, account);
	}
	
	public boolean isLocalBranch(int branch){
		return isReplicatingBranch(groupID, branch);
	}
	
	public boolean isLocalTeller(int teller){
		return isReplicatingTeller(groupID, teller);
	}
	
	public int branchOfAccount(int account){
		return account/(TELLERS_PER_BRANCH*ACCOUNTS_PER_TELLER);
	}
	
	public boolean isLocal(TpcbTransaction t){
		return isLocalAccount(t.getAccID()) && isLocalBranch(t.getBranchID()) && isLocalTeller(t.getTellerID());
	}
	
	public HashSet<String> groupsReplicating(int account, int teller, int branch){
		HashSet<String> ret = new HashSet<String>();
		for(int g : groupsToPartitions.keySet()){
			if( isReplicatingAccount(g, account) || isReplicatingBranch(g, branch) || isReplicatingTeller(g, teller) ) ret.add(String.valueOf(g));
		}
		return ret;
	}
	
	public int nbLocalBranches(){
		return groupsToPartitions.get(groupID).size()*branchesPerPartition;
	}

	public int nbLocalAccounts(){
		return groupsToPartitions.get(groupID).size()*accountsPerPartition;
	}	
	
	public int nbLocalTeller(){
		return groupsToPartitions.get(groupID).size()*tellersPerPartition;
	}
		
	public List<Integer> localBranches(){
		List<Integer> ret = CollectionUtils.newList();
		for(Partition p : groupsToPartitions.get(groupID)){ret.addAll(p.branches());}
		return ret;		
	}

	public List<Integer> localTellers(){
		List<Integer> ret = CollectionUtils.newList();
		for(Partition p : groupsToPartitions.get(groupID)){ret.addAll(p.tellers());}
		return ret;		
	}
	
	public List<Integer> localAccounts(){
		List<Integer> ret = CollectionUtils.newList();
		for(Partition p : groupsToPartitions.get(groupID)){ret.addAll(p.accounts());}
		return ret;
	}
	
	//
	// INNER METHODS
	//
	
	private boolean isReplicatingAccount(int g, int account){
		for(Partition p: groupsToPartitions.get(g)){
			if(p.containsAccount(account)) return true;
		}
		return false;
	}
	
	private boolean isReplicatingBranch(int g, int branch){
		for(Partition p: groupsToPartitions.get(g)){
			if(p.containsBranch(branch)) return true;
		}
		return false;
	} 
	
	private boolean isReplicatingTeller(int g, int teller){
		for(Partition p: groupsToPartitions.get(g)){
			if(p.containsTeller(teller)) return true;
		}
		return false;
	}
	
	
	//
	// INNER CLASSES
	//
	
	private class Partition{

		int id;
		int minAccount, maxAccount;
		int minBranch, maxBranch;
		int minTeller, maxTeller;

		Partition(int id, int minBranch, int maxBranch, int minTeller, int maxTeller, int minAccount, int maxAccount){
			this.id = id;
			this.minBranch = minBranch;
			this.maxBranch = maxBranch;
			this.minTeller = minTeller;
			this.maxTeller = maxTeller;
			this.minAccount = minAccount;
			this.maxAccount = maxAccount;
		}

		List<Integer> branches(){
			List<Integer> ret = CollectionUtils.newList();
			for(int i=minBranch; i<=maxBranch;i++){ ret.add(i);}
			return ret;
		}

		List<Integer> tellers(){
			List<Integer> ret = CollectionUtils.newList();
			for(int i=minTeller; i<=maxTeller;i++){ret.add(i);}
			return ret;
		}
		
		List<Integer> accounts(){
			List<Integer> ret = CollectionUtils.newList();
			for(int i=minAccount; i<=maxAccount;i++){ret.add(i);}
			return ret;
		}
		
		int randomAccount(){
			return MathUtils.random_int(minAccount, maxAccount+1);
		}
		
		int randomBranch(){
			return MathUtils.random_int(minBranch, maxBranch+1);
		}
		
		int randomTeller(){
			return MathUtils.random_int(minTeller, maxTeller+1);
		}
		
		boolean containsAccount(int account){
			return minAccount<=account && account<=maxAccount;
		}
		
		boolean containsBranch(int branch){
			return minBranch<=branch && branch<=maxBranch;
		}
		
		boolean containsTeller(int teller){
			return minTeller<=teller && teller<=maxTeller;
		}
		
		public String toString(){
			return "Partition "+id+": "
				   +"minAccount="+minAccount
				   +", "
				   +"maxAccount="+maxAccount
				   +", "
				   +"minBranch="+minBranch
				   +", "
				   +"maxBranch="+maxBranch
				   +", "
				   +"minTeller="+minTeller
				   +", "
				   +"maxTeller="+maxTeller;
		}
	}
	

}
