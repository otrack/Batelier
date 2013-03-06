/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.
This library is free software; you can redistribute it and/or modify it under     
the terms of the GNU Lesser General Public License as published by the Free Software 	
Foundation; either version 2.1 of the License, or (at your option) any later version.      
his library is distributed in the hope that it will be useful, but WITHOUT      
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      
PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      
You should have received a copy of the GNU Lesser General Public License along      
with this library; if not, write to the      
Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
 */


package net.sourceforge.fractal.wanamcast;

import java.util.Map;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.FractalUtils;
import net.sourceforge.fractal.utils.XMLUtils;

import org.w3c.dom.Element;
import org.w3c.dom.Node;


public class WanAMCastStreamManager {

	Map<String,WanAMCastStream> streams;

	public WanAMCastStreamManager(){
		streams = CollectionUtils.newMap();
	}

	public void load(Node config){	

		String range = XMLUtils.getAttribByName((Element) config, "instantiate");

		if(FractalUtils.inRange(range,FractalManager.getInstance().membership.myId())){
			String streamName,consensusName, rmcastName, groupName;

			streamName = XMLUtils.getAttribByName((Element) config, "name");		
			consensusName = String.valueOf(XMLUtils.getAttribByName((Element) config, "consensusName"));
			rmcastName = String.valueOf(XMLUtils.getAttribByName((Element) config, "rmcastName"));
			groupName = String.valueOf(XMLUtils.getAttribByName((Element) config, "group"));
			streams.put(streamName, new WanAMCastStream(
					FractalManager.getInstance().membership.myId(),
					FractalManager.getInstance().membership.group(groupName),
					streamName,
					FractalManager.getInstance().multicast.stream(rmcastName),
					FractalManager.getInstance().paxos.stream(consensusName)));
			if(ConstantPool.MULTICAST_DL > 0) System.out.println("Started AMCast stream " + streamName + " on id " + FractalManager.getInstance().membership.myId());
		}
	}

	public WanAMCastStream getOrCreateWanAMCastStream(
			String streamName,
			String groupName, 
			String rmcastName,
			String rbcastName,
			String consensusName){

		if(streams.get(streamName)!=null){
			return streams.get(streamName);
		}

		WanAMCastStream stream = new WanAMCastStream(
				FractalManager.getInstance().membership.myId(),
				FractalManager.getInstance().membership.group(groupName),
				streamName,
				FractalManager.getInstance().multicast.stream(rmcastName),
				FractalManager.getInstance().paxos.stream(consensusName));
		streams.put(streamName,stream);
		return stream;
	}

	public WanAMCastStream stream(String streamName){
		return streams.get(streamName);
	}	

}
