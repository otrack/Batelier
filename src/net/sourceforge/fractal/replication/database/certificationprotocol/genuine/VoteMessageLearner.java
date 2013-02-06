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

package net.sourceforge.fractal.replication.database.certificationprotocol.genuine;

import java.io.Serializable;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
/**   
 * @author N.Schiper
* @author P. Sutra
* 
*/


public class VoteMessageLearner implements Learner {

	public VoteMessageLearner(){
	}

	public void learn(Stream s, Serializable value) {
		VoteMessage m = (VoteMessage) value;
		Vote v = m.getVote();
		if(ConstantPool.TEST_DL>1) System.out.println("I receive "+ m.getVote()+ " for "+v.getTransactionID()+" from "+m.source);
		Vote.mergeVote(v);
	}
}
