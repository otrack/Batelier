/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/


package net.sourceforge.fractal.consensus.paxos;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
/**   
* @author L. Camargos
* 
*/ 

import net.sourceforge.fractal.Messageable;


public class PMPhase2A extends PMessage{

	private static final long serialVersionUID = Messageable.FRACTAL_MID;

    int timestamp;
    
    public PMPhase2A(){}
    
    public PMPhase2A(String streamName, int instance, int ts, Serializable proposal, int swid) {
        super(PHASE2_A,streamName, instance, swid);
        this.timestamp = ts;
        this.serializable = proposal;
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        timestamp = in.readInt();
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(timestamp);
    }
    
    public String toString(){
        return "("+type +","+instance +","+serializable+")";
    }
}
