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
/**   
* @author L. Camargos
* 
*/ 

class PMPhase1B extends PMessage implements Cloneable{
  
	private static final long serialVersionUID = Messageable.FRACTAL_MID;

    int timestamp;
    Integer highestA = null;

    public PMPhase1B(){}
    
    public PMPhase1B(String streamName, int instance, int ts,
    		Integer highestAccptd, Serializable accptdValue, int swid) {
        super(PHASE1_B,streamName, instance, swid);
        this.timestamp = ts;
        this.highestA = highestAccptd;
        this.serializable = accptdValue;
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        timestamp = in.readInt();
        boolean hasAccepted = in.readBoolean();
        if(hasAccepted){
            highestA = (Integer) in.readInt();
        }
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(timestamp);
        if(highestA == null)
            out.writeBoolean(false); //hasAccepted
        else{
            out.writeBoolean(true);
            out.writeInt(highestA);
        }
    }    
    
    public Object clone(){
    	PMPhase1B m = (PMPhase1B)super.clone();
    	m.timestamp= this.timestamp;
    	m.highestA= this.highestA;
    	return m;
    }
    
}
