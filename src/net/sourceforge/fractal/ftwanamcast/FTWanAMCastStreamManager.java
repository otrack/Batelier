package net.sourceforge.fractal.ftwanamcast;

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
/**   
* @author P. Sutra
* 
*/ 

public class FTWanAMCastStreamManager {

	Map<String,FTWanAMCastStream> streams = CollectionUtils.newMap();

	public FTWanAMCastStreamManager(){}

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
				throw new RuntimeException("In FTWanAMCastStream nbLocalConsensusPerRound not specified");

			nbLocalConsensusPerRound = Integer.valueOf(XMLUtils.getAttribByName((Element) config, "nbLocalConsensusPerRound"));

			if( ! XMLUtils.hasAttribByName((Element) config, "startRoundFrequency") )
				throw new RuntimeException("In FTWanAMCastStream startRoundFrequency not specified");

			startRoundFrequency = Integer.valueOf(XMLUtils.getAttribByName((Element) config, "startRoundFrequency"));

			if(nbLocalConsensusPerRound<startRoundFrequency)
				throw new RuntimeException("Invalid parameters");

			myGroup = String.valueOf(XMLUtils.getAttribByName((Element) config, "group"));
			for(String g : String.valueOf(XMLUtils.getAttribByName((Element) config, "groups")).split(",")){
				groups.add(g);
			}
			streams.put(streamName, new FTWanAMCastStream(FractalManager.getInstance().membership.myId(), myGroup, groups, streamName, rbcastName, rmcastName, 
					consensusName, nbLocalConsensusPerRound, startRoundFrequency));
			if(ConstantPool.WANABCAST_DL > 0)
				System.out.println("Started WanABCast stream " + streamName + " on " + FractalManager.getInstance().membership.myId());
		}
	}

	public FTWanAMCastStream stream(String streamName){
		return streams.get(streamName);
	}

	public FTWanAMCastStream getOrCreateFTWanAMCastStream(
			String streamName,
			String groupName,
			Collection<String> allGroupsNames,
			String rmcastName,
			String rbcastName,
			String consensusName, 
			int maxInterGroupMessageDelay,
			int consensusLatency){

		if(streams.get(streamName)!=null){
			return streams.get(streamName);
		}
		
		if(consensusLatency>maxInterGroupMessageDelay)
			throw new IllegalArgumentException("consensus'latency cannot be higher than inter-group message delay");
		
		int nbLocalConsensusPerRound = (2*maxInterGroupMessageDelay)/consensusLatency;
		int startRoundFrequency = nbLocalConsensusPerRound/2; // FIXME tweak this parameter

		FTWanAMCastStream stream = new FTWanAMCastStream(
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

}
