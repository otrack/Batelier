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
/**   
 * @author N.Schiper
* 
*/

public class Tuple implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int value;
	private int ts;
	
	public Tuple() {
	}
	
	public Tuple(int value, int ts) {
		this.value = value;
		this.ts = ts;
	}
	
	public int getValue() {
		return this.value;
	}
	
	public int getTS() {
		return this.ts;
	}
	
	public void setValue(int value) {
		this.value = value;
	}
	
	public void setTS(int ts) {
		this.ts = ts;
	}
}
