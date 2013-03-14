/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/


package net.sourceforge.fractal.multicast;

import java.util.Map;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.utils.CollectionUtils;

/**   
* @author P. Sutra
* 
*/

public class MulticastStreamManager{

	Map<String,MulticastStream> streams;

	public MulticastStreamManager(){
		 streams = CollectionUtils.newMap();
	}
		
	public MulticastStream stream(String streamName){
		return streams.get(streamName);
	}

	public MulticastStream getOrCreateRMCastStream(
			FractalManager manager,
			String streamName,
			String groupName) {
		
		if(streams.get(streamName)!=null){
			return streams.get(streamName);
		}		
		
		MulticastStream stream = new MulticastStream(
				streamName,
				manager.membership.group(groupName),
				manager.membership);
		
		streams.put(streamName,stream);
		return stream;
	}

}
