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

/**
 * @author Pierre Sutra
 */

package net.sourceforge.fractal.replication.database.certificationprotocol.genuine;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import net.sourceforge.fractal.ConstantPool;

import net.sourceforge.fractal.replication.database.PSTORE.RESULT;
/**   
 * @author N.Schiper
* @author P. Sutra
* 
*/

public class Vote implements Serializable {    

	// CLASS FIELDS AND METTHODS
	
	private static final long serialVersionUID = 1L;

	private static transient Map<Long,Vote> votes;
	static{
		votes = new LinkedHashMap<Long,Vote>(10000,0.75f,true){
					private static final long serialVersionUID = 1L;
					@SuppressWarnings("unchecked")
					protected boolean removeEldestEntry(Map.Entry eldest) {
					return size() > 10000.;
					}
		};
	}
		
	/**
	 * Merge <i>w</i> with the votes currently existing on <i>w.getTransactionId()</i>.
	 * @param w
	 */
	public static void mergeVote(Vote w){
		Vote v = voteFor(w.getTransactionID());
		synchronized(v){
			v.merge(w);
			v.notifyAll();
			if(ConstantPool.TEST_DL>1) System.out.println("Current result for transaction "+v.getTransactionID()+" is "+v.getVoteResult());
		}
	}
	
	/**
	 * Wait that the vote on <i>tid</i> completes. 
	 * @param tid
	 * @return
	 * @throws InterruptedException
	 */
	public static RESULT waitCompleteVoteFor(long tid) throws InterruptedException {
		Vote v = voteFor(tid);
		RESULT result = RESULT.UNKNOWN;
		synchronized(v){
			while(v.getVoteResult()==RESULT.UNKNOWN){
				 v.wait();
			}
			result = v.getVoteResult();
		}
		return result;
	}
	
	private static synchronized Vote voteFor(long tid){
		if(!votes.containsKey(tid)){
			Vote v = new Vote(tid);
			votes.put(tid,v);
		}
		return votes.get(tid);
	}
		
	// OBJECT FIELDS AND METTHODS 
	
	private long tid;
	private RESULT account, teller, branch;
	
	public Vote(long id){
		tid = id;
		account = RESULT.UNKNOWN;
		teller = RESULT.UNKNOWN;
		branch = RESULT.UNKNOWN;
	}
	
	public long getTransactionID(){
		return tid;
	}
	
	public void setAccountVote(RESULT vote){
		assert vote!=RESULT.UNKNOWN;
		account=vote;
	}
	
	public void setTellerVote(RESULT vote){
		assert vote!=RESULT.UNKNOWN;
		teller=vote;
	}
	
	public void setBranchVote(RESULT vote){
		assert vote!=RESULT.UNKNOWN;
		branch=vote;
	}
	
	public void merge(Vote v){
		
		if(v.account!=RESULT.UNKNOWN){
			assert account==RESULT.UNKNOWN || account==v.account : account +" vs " + v.account;
			account = v.account;
		}
		
		if(v.teller!=RESULT.UNKNOWN){
			assert teller==RESULT.UNKNOWN || teller==v.teller : teller+" vs "+v.teller;
			teller = v.teller;
		}
		
		if(v.branch!=RESULT.UNKNOWN){
			assert branch==RESULT.UNKNOWN || branch==v.branch : branch +" vs " + v.branch;
			branch = v.branch;
		}
		
	}
	
	public RESULT getVoteResult(){
		if( account==RESULT.COMMIT && teller==RESULT.COMMIT && branch==RESULT.COMMIT ) return RESULT.COMMIT;
		if( account==RESULT.ABORT || teller==RESULT.ABORT || branch==RESULT.ABORT ) return RESULT.ABORT;
		return RESULT.UNKNOWN;
	}
	
	public String toString(){
		return "[teller="+teller+",branch="+branch+",account="+account+"]";
	}
	
}