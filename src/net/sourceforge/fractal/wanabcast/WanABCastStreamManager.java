/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
 */


package net.sourceforge.fractal.wanabcast;
/**   
* @author P. Sutra
* 
*/

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.FractalUtils;
import net.sourceforge.fractal.utils.XMLUtils;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

public class WanABCastStreamManager {

	Map<String,WanABCastStream> streams = CollectionUtils.newMap();

	public WanABCastStreamManager(){
		streams = CollectionUtils.newMap();
	}

	public void load(Node config){

		int nbLocalConsensusPerRound = 1, startRoundFrequency = 1;
		String range = XMLUtils.getAttribByName((Element) config, "instantiate");

		if(FractalUtils.inRange(range,FractalManager.getInstance().membership.myId())){
			String streamName,consensusName, rmcastName, rbcastName, myGroup;
			Collection<String> groups = new ArrayList<String>();
			streamName = XMLUtils.getAttribByName((Element) config, "name");				
			consensusName = String.valueOf(XMLUtils.getAttribByName((Element) config, "consensusName"));
			rmcastName = String.valueOf(XMLUtils.getAttribByName((Element) config, "rmcastName"));
			rbcastName = String.valueOf(XMLUtils.getAttribByName((Element) config, "rbcastName"));

			if( ! XMLUtils.hasAttribByName((Element) config, "nbLocalConsensusPerRound") )
				throw new RuntimeException("In WanABCastStream nbLocalConsensusPerRound not specified");
			nbLocalConsensusPerRound = Integer.valueOf(XMLUtils.getAttribByName((Element) config, "nbLocalConsensusPerRound"));


			if( ! XMLUtils.hasAttribByName((Element) config, "startRoundFrequency") )
				throw new RuntimeException("In WanABCastStream startRoundFrequency not specified");
			startRoundFrequency = Integer.valueOf(XMLUtils.getAttribByName((Element) config, "startRoundFrequency"));

			if(nbLocalConsensusPerRound<startRoundFrequency)
				throw new RuntimeException("Invalid parameters");

			myGroup = String.valueOf(XMLUtils.getAttribByName((Element) config, "group"));
			for(String g : String.valueOf(XMLUtils.getAttribByName((Element) config, "groups")).split(",")){
				groups.add(g);
			}
			
			streams.put(streamName, new WanABCastStream(FractalManager.getInstance().membership.myId(), myGroup, groups, streamName, rbcastName, rmcastName, 
					consensusName, nbLocalConsensusPerRound, startRoundFrequency));
			if(ConstantPool.WANABCAST_DL > 0)
				System.out.println("Started WanABCast stream " + streamName + " on " + FractalManager.getInstance().membership.myId());
		}

	}

	public WanABCastStream getOrCreateWanABCastStream(
			String streamName,
			String groupName,
			Collection<String> allGroupsNames,
			String rmcastName,
			String rbcastName,
			String consensusName, 
			int maxInterGroupMessageDelay,
			int consensusLatency){

		if(consensusLatency>maxInterGroupMessageDelay){
			System.err.println("Consensus'latency cannot be higher than inter-group message delay");
			System.err.println("Setting inter-group message delay to consensus'latency");
			maxInterGroupMessageDelay = consensusLatency;
		}
		
		if(streams.get(streamName)!=null){
			return streams.get(streamName);
		}
		
		int nbLocalConsensusPerRound = maxInterGroupMessageDelay/consensusLatency;
		int startRoundFrequency = 1;
		if(nbLocalConsensusPerRound>=2) startRoundFrequency = nbLocalConsensusPerRound/2; // FIXME tweak this parameter

		WanABCastStream stream = new WanABCastStream(
				FractalManager.getInstance().membership.myId(),
				groupName,
				allGroupsNames,
				streamName,
				rmcastName,
				rbcastName,
				consensusName,
				nbLocalConsensusPerRound,
				startRoundFrequency);
		
		streams.put(streamName,stream);
		return stream;
	}

	public WanABCastStream stream(String streamName){
		return streams.get(streamName);
	}	

}
