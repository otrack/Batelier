/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/


package net.sourceforge.fractal.consensus.paxos;

import java.io.Serializable;

import net.sourceforge.fractal.Messageable;

/**   
* @author L. Camargos
* 
*/ 


class PMBroadcast extends PMessage implements Cloneable{
    //transient private int validity = 1; //If this proposal can be proposed in future instances, set it to how much instances.
    
    private static final long serialVersionUID = Messageable.FRACTAL_MID;
    
    public PMBroadcast(){}
    
    public PMBroadcast(String streamName, int inst, Serializable proposal, int swid) {
        super(BROADCAST, streamName, inst, swid);
        this.serializable = proposal;
    }
    
}
