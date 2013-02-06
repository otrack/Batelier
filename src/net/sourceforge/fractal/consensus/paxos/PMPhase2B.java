/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/


package net.sourceforge.fractal.consensus.paxos;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import net.sourceforge.fractal.Messageable;
import net.sourceforge.fractal.utils.SerializationUtils;
/**   
* @author L. Camargos
* 
*/ 

class PMPhase2B extends PMessage implements Cloneable{
  
	private static final long serialVersionUID = Messageable.FRACTAL_MID;

    int timestamp;
    int a_id;
    
    public PMPhase2B(){}
    
    public PMPhase2B(String streamName, int instance, int ts, Serializable accepted, int a_id, int swid) {
        super(PHASE2_B, streamName, instance, swid);
        this.timestamp = ts;
        this.a_id = a_id;
        this.serializable = accepted;
    }
    
    public int getAccId() {
        return a_id;
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        timestamp = in.readInt();
        a_id = in.readInt();
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(timestamp);
        out.writeInt(a_id);
    }
    
    public String toString(){
        return "("+type + ","+instance +","+serializable+")";
    }
    
    public Object clone(){
    	PMPhase2B m = (PMPhase2B)super.clone();
    	m.timestamp = this.timestamp;
        m.a_id= this.a_id;
        return m;
    }

}