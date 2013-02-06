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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
/**   
 * @author N.Schiper
* @author P. Sutra
* 
*/

public class TpcbTransaction implements Serializable{    

	private static final long serialVersionUID = 1L;

	//transaction id
	private Long id;
	
	//id of client who submitted transaction
	private int clientID;
	
	//id of node who submitted transaction
	private int nodeID;
	
	// Readset/WriteSet (identical)
	private int acc_id, teller_id, branch_id;
	private int acc_ts, teller_ts, branch_ts;
	
	// Update Values
	private int acc_bal, teller_bal, branch_bal;
	private int delta;
	private long submit_ts;
	
	//group IDs to which transaction must be multicast as well as which groups must certify
	//the transaction
	private HashSet<String> destGroups, readSetGroups, writeSetGroups;
	
	private int commit;
	
	public TpcbTransaction(long id, int clientID, int nodeID, int acc_id, int teller_id, int branch_id, int acc_ts, int teller_ts, int branch_ts,
						   int acc_bal, int teller_bal, int branch_bal, int delta, long submit_ts, HashSet<String> destGroups, HashSet<String> readSetGroups,
						   HashSet<String> writeSetGroups) {
		
		this.id = id;
		this.clientID = clientID;
		this.nodeID = nodeID;
		this.acc_id = acc_id;
		this.teller_id = teller_id;
		this.branch_id = branch_id;
		this.acc_ts = acc_ts;
		this.teller_ts = teller_ts;
		this.branch_ts = branch_ts;
		this.acc_bal = acc_bal;
		this.teller_bal = teller_bal;
		this.branch_bal = branch_bal;
		this.delta = delta;
		this.submit_ts = submit_ts;
		this.destGroups = destGroups;
		this.readSetGroups = readSetGroups;
		this.writeSetGroups = writeSetGroups;
	}
	
	@Override
	public boolean equals(Object object){
		
		if (this == object) {
			return true;
		}
		
		if (!(object instanceof TpcbTransaction)) {
			return false;
		}
		
		return id.equals(((TpcbTransaction)object).id);
		
	}
	
	@Override
	public final int hashCode(){
		return id.hashCode();
	}
	
	public boolean conflictWith(TpcbTransaction t){
		return acc_id==t.acc_id || teller_id==t.teller_id || branch_id==t.branch_id;
	}
	
	public long getID() {
		return this.id;
	}
	
	public int getClientID() {
		return this.clientID;
	}
	
	public int getNodeID() {
		return this.nodeID;
	}
	
	public int getAccID() {
		return this.acc_id;
	}

	public int getTellerID() {
		return this.teller_id;
	}

	public int getBranchID() {
		return this.branch_id;
	}
	
	public int getAccTS() {
		return this.acc_ts;
	}

	public int getTellerTS() {
		return this.teller_ts;
	}

	public int getBranchTS() {
		return this.branch_ts;
	}

	public int getAccBal() {
		return this.acc_bal;
	}
	
	public int getTellerBal() {
		return this.teller_bal;
	}
	
	public int getBranchBal() {
		return this.branch_bal;
	}
	
	public int getDelta() {
		return this.delta;
	}
	
	public long getSubmitTS() {
		return this.submit_ts;
	}
	
	public int getCommit() {
		return this.commit;
	}
	
	public void setCommit(int commit) {
		this.commit = commit;
	}
	
	public String toString() {
		return String.valueOf("T"+id);
	}
	
	public String toStringDetailed(){
		return "T"+id + " (acc: " + acc_id + " ts: " + acc_ts + ", teller: " + teller_id + " ts: " + teller_ts + " branch: " + branch_id + " ts: " + branch_ts + ")" +
					"(acc_bal: " + acc_bal + " teller_bal: " + teller_bal + " branch_bal: " + branch_bal + " submit_ts: " + submit_ts + ")";
	}
	
	public HashSet<String> destGroups() {
		return this.destGroups;
	}
	
	public void setDestGroups(Collection<String> destGroups) {
		this.destGroups = new HashSet<String>(destGroups);
	}
	
	public HashSet<String> readSetGroups() {
		return this.readSetGroups;
	}
	
	public HashSet<String> writeSetGroups() {
		return this.writeSetGroups;
	}
}
