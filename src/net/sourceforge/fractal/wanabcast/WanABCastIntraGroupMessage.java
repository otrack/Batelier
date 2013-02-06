/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/

/**   
* @author P. Sutra
* 
*/

package net.sourceforge.fractal.wanabcast;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.broadcast.BroadcastMessage;

public class WanABCastIntraGroupMessage extends BroadcastMessage implements Cloneable{

	private static final long serialVersionUID = 1L;

	public HashSet<String> dest;
	
	public WanABCastIntraGroupMessage(){
		
	}
	
	public WanABCastIntraGroupMessage(Serializable s, Set<String> dest, int swidSource){
		super(s,swidSource);
		this.dest = new HashSet<String>(dest);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		 super.writeExternal(out);
		 out.writeObject(dest);
	}
		 
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		dest=(HashSet<String>)in.readObject();
	}
		
	public Object clone(){
		return super.clone();
	}
	
}
