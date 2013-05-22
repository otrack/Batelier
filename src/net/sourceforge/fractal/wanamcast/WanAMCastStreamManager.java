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

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.utils.CollectionUtils;


public class WanAMCastStreamManager {

	Map<String,WanAMCastStream> streams;

	public WanAMCastStreamManager(){
		streams = CollectionUtils.newMap();
	}

	public WanAMCastStream getOrCreateWanAMCastStream(
			FractalManager manager,
			String streamName,
			String groupName, 
			String rmcastName,
			String rbcastName){

		if(streams.get(streamName)!=null){
			return streams.get(streamName);
		}

		WanAMCastStream stream = new WanAMCastStream(
				manager.membership.myId(),
				manager.membership.group(groupName),
				streamName,
				manager.multicast.stream(rmcastName),
				true);
		streams.put(streamName,stream);
		return stream;
	}

	public WanAMCastStream getOrCreateWanNonAcyclicAMCastStream(
			FractalManager manager,
			String streamName,
			String groupName, 
			String rmcastName,
			String rbcastName){

		if(streams.get(streamName)!=null){
			return streams.get(streamName);
		}

		WanAMCastStream stream = new WanAMCastStream(
				manager.membership.myId(),
				manager.membership.group(groupName),
				streamName,
				manager.multicast.stream(rmcastName),
				false);
		streams.put(streamName,stream);
		return stream;
	}
	
	
	public WanAMCastStream stream(String streamName){
		return streams.get(streamName);
	}	

}
