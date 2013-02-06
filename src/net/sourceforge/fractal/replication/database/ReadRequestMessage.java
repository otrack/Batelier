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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.multicast.MulticastMessage;
/**   
 * @author N.Schiper
* @author P. Sutra
* 
*/


public class ReadRequestMessage extends MulticastMessage implements Cloneable, Serializable {    

	private static final long serialVersionUID = 1L;
	
	// FIXME change this !!!
	//serializable object stores an arraylist of object of size three
	//entry 0: node id that issued this request
	//entry 1:
	//transaction id that issued this request
    //entry 2:
	//matrix with 2 rows, the first one is the requested account ids
	//the second one is the requested branch ids
	
	public ReadRequestMessage() {
	
	}
	
	public ReadRequestMessage(Serializable s, Set<String> dest, String gSource, int swid) {
		super(s, dest, gSource, swid);
	}
	
	public long getTid() {
		return (Long) ((ArrayList<Object>) super.serializable).get(1);
	}
	
	public int getNid() {
		return (Integer) ((ArrayList<Object>) super.serializable).get(0);
	}
	
	public Set<String> getGid() {
		HashSet<String> groupSrc = new HashSet<String>();
		groupSrc.add(super.gSource);
		return groupSrc;
	}
	
	public Integer[][] getIDs() {
		return (Integer[][]) ((ArrayList<Object>) super.serializable).get(2);
	}
}
