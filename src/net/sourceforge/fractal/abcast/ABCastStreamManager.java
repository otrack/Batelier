/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.

    This library is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by the Free Software
	Foundation; either version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful, but WITHOUT 
    ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A 
    PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License along 
    with this library; if not, write to the 
    Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
*/


package net.sourceforge.fractal.abcast;

import java.util.Map;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.broadcast.BroadcastStream;
import net.sourceforge.fractal.consensus.paxos.PaxosStream;
import net.sourceforge.fractal.utils.CollectionUtils;


/**   
* @author L. Camargos
* @author P. Sutra
* 
*/ 


public class ABCastStreamManager {
	
    Map<String,ABCastStream> streams;

    public ABCastStreamManager(){
    	streams = CollectionUtils.newMap();
    }
  
    public ABCastStream getOrCreateABCastStream(FractalManager manager, String streamName, PaxosStream cs, BroadcastStream bs) {
 	    	
    	if(streams.get(streamName)!=null){
    		return streams.get(streamName);
    	}

    	ABCastStream stream = new ABCastStream(
    								streamName,
    								manager.membership.myId(),
    								cs,
    								bs);
    	streams.put(streamName,stream);
		return stream;
    }
    
    public ABCastStream stream(String streamName){
        return streams.get(streamName);
    }
    
}
