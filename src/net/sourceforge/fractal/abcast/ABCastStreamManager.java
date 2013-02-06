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

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.FractalUtils;
import net.sourceforge.fractal.utils.XMLUtils;

import org.w3c.dom.Element;
import org.w3c.dom.Node;


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

    public void load(Node config){
        
        String range = XMLUtils.getAttribByName((Element) config, "instantiate");

        String streamName, consensusStreamName, rbcastStreamName;
        
        if(FractalUtils.inRange(range,FractalManager.getInstance().membership.myId())){
        	streamName = XMLUtils.getAttribByName((Element) config, "name");
        	consensusStreamName = XMLUtils.getAttribByName((Element) config, "consensusStreamName");
        	rbcastStreamName = XMLUtils.getAttribByName((Element) config, "rbcastStreamName");

        	if(FractalManager.getInstance().broadcast.getOrCreateBroadcastStream(rbcastStreamName, rbcastStreamName)==null)
        		throw new RuntimeException("RBCastStream "+rbcastStreamName+" does not exist");

        	if(FractalManager.getInstance().paxos.stream(consensusStreamName)==null)
        		throw new RuntimeException("PaxosStream"+consensusStreamName+" does not exist");

        	streams.put(streamName, new ABCastStream(
        			streamName,
        			FractalManager.getInstance().membership.myId(),
        			consensusStreamName,
        			rbcastStreamName));

        	if(ConstantPool.ABCAST_DL > 0) 
        		System.out.println("Started ABCast stream " + streamName + " on id " + FractalManager.getInstance().membership.myId());
        }
    }
    
    public ABCastStream getOrCreateABCastStream(String streamName, String consensusStreamName, String rbcastStreamName){
    	
    	if(FractalManager.getInstance().broadcast.stream(rbcastStreamName)==null)
    		throw new RuntimeException("RBCastStream "+rbcastStreamName+" does not exist");
    	
    	if(streams.get(streamName)!=null){
    		return streams.get(streamName);
    	}
    	ABCastStream stream = new ABCastStream(
    								streamName,
    								FractalManager.getInstance().membership.myId(),
    								consensusStreamName,
    								rbcastStreamName);
    	streams.put(streamName,stream);
		return stream;
    }
    
    public ABCastStream stream(String streamName){
        return streams.get(streamName);
    }
    
}
