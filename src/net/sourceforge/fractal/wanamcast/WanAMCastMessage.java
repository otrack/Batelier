/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/


package net.sourceforge.fractal.wanamcast;

/**   
* @author P. Sutra
* 
*/

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collection;

import net.sourceforge.fractal.Messageable;
import net.sourceforge.fractal.UMessage;

public class WanAMCastMessage extends UMessage {

	private static final long serialVersionUID = Messageable.FRACTAL_MID;

	public Integer clock;
	public Collection<String> dest;
	public String gSource;
	public long start;
	
	@Deprecated
	public WanAMCastMessage(){	
	}
	
	public WanAMCastMessage(Serializable s, Collection<String> dest, String gSource, int swidSource){
		super(s,swidSource);
		this.clock = -1;
		this.dest=dest;
		this.gSource = gSource;
		this.start = System.currentTimeMillis();
	}
	
	public void writeExternal(ObjectOutput out) throws IOException {
		 super.writeExternal(out);
		 out.writeObject(clock);
		 out.writeObject(dest);
		 out.writeObject(gSource);
		 out.writeLong(start);
	}
		 
	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		clock = (Integer)in.readObject();
		dest = (Collection<String>)in.readObject();
		gSource = (String)in.readObject();
		start = in.readLong();
	}
		
	public String toString(){
		return "<<"+getUniqueId()+","+clock+","+dest+","+gSource+">>";
	}

	public boolean commute(WanAMCastMessage m){
		if(m==this) return true;
		return false;
	}
	
}
