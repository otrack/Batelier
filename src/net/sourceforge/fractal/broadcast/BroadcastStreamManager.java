/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
 */


package net.sourceforge.fractal.broadcast;

import java.util.Map;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.FractalUtils;
import net.sourceforge.fractal.utils.XMLUtils;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**   
* @author P. Sutra
* 
*/
public class BroadcastStreamManager {
	
	Map<String,BroadcastStream> streams;

	public BroadcastStreamManager(){
		streams = CollectionUtils.newMap();
	}
	
	public BroadcastStream getOrCreateBroadcastStream(FractalManager manager, String streamName, String groupName){
		if(streams.get(streamName)!=null){
			return streams.get(streamName);
		}
		BroadcastStream stream =  new BroadcastStream(streamName,
				manager.membership.group(groupName),
				manager.membership.myId());
		streams.put(streamName,stream);
		return stream;
	}
	
	public BroadcastStream stream(String name){
		return streams.get(name);
	}

}
