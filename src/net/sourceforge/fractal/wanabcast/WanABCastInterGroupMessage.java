/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/


package net.sourceforge.fractal.wanabcast;
/**   
* @author P. Sutra
* 
*/

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.HashSet;

import net.sourceforge.fractal.multicast.MulticastMessage;

public class WanABCastInterGroupMessage extends MulticastMessage implements Cloneable {

	private static final long serialVersionUID = 1L;
	int round;
	
	public WanABCastInterGroupMessage(){
		  
	}
	
	public WanABCastInterGroupMessage(Serializable s, HashSet<String> dest, String gSource, int swidSource, 
			int round){
		super(s,dest,gSource,swidSource);
		this.round = round;
	}
		
	public Object clone(){
		WanABCastInterGroupMessage m = (WanABCastInterGroupMessage)super.clone();
		m.round = this.round;
		return m;
	}
	
	public void writeExternal(ObjectOutput out) throws IOException {
		 super.writeExternal(out);
		 out.writeObject(this.round);
	}
		 
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		this.round = (Integer) in.readObject();
	}
		
	public String toString(){
		return "<"+round+","+gSource+">";
	}

	public boolean equals(Object o){
		return super.equals(o);
	}
	
}
