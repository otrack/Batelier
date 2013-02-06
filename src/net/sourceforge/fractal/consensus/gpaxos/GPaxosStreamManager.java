package net.sourceforge.fractal.consensus.gpaxos;

import java.util.Map;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream.RECOVERY;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.XMLUtils;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * 
 * @author Pierre Sutra
 *
 */

public class GPaxosStreamManager {

	private Map<String,GPaxosStream> streams;

	public GPaxosStreamManager(){
		streams = CollectionUtils.newMap();
	}

	public void load(Node config){

		String streamName, cstructClassName;
		boolean useFastBallot;
		RECOVERY recovery;
		int ltimeout, checkpointSize;
		String pgroup, agroup, lgroup;

		streamName = XMLUtils.getAttribByName((Element) config, "name");
		pgroup= String.valueOf(XMLUtils.getAttribByName((Element) config, "pgroup"));
		agroup= String.valueOf(XMLUtils.getAttribByName((Element) config, "agroup"));
		lgroup= String.valueOf(XMLUtils.getAttribByName((Element) config, "lgroup"));

		cstructClassName = String.valueOf(XMLUtils.getAttribByName((Element) config, "cstruct"));

		if(XMLUtils.hasAttribByName((Element) config, "ltimeout"))
			ltimeout = Integer.valueOf(XMLUtils.getAttribByName((Element) config, "ltimeout"));
		else
			ltimeout = 10000; 

		if(XMLUtils.hasAttribByName((Element) config, "useFastBallot"))
			useFastBallot = Boolean.valueOf(XMLUtils.getAttribByName((Element) config, "useFastBallot"));
		else
			useFastBallot = false;

		if(XMLUtils.hasAttribByName((Element) config, "recovery")){

			switch(Integer.valueOf(XMLUtils.getAttribByName((Element) config, "recovery"))){

			default:
			case 0 : recovery = RECOVERY.DEFAULT; break;
			case 1 : recovery = RECOVERY.COORDINATED; break;
			case 2 : recovery = RECOVERY.FGGC; break;

			}

		}else{
			recovery = RECOVERY.DEFAULT;
		}

		if(XMLUtils.hasAttribByName((Element) config, "checkpointSize"))
			checkpointSize = Integer.valueOf(XMLUtils.getAttribByName((Element) config, "checkpointSize"));
		else
			checkpointSize = 400;		

		if( recovery == RECOVERY.FGGC
				&&
				! useFastBallot ){
			throw new RuntimeException("Invalid usage: FGGC => useFastBallots");	
		}

		if(streams.containsKey(streamName)){
			throw new RuntimeException("Stream "+streamName+" already exists.");
		}

		GPaxosStream s = new GPaxosStream(streamName,
				FractalManager.getInstance().membership.myId(),
				FractalManager.getInstance().membership.group(pgroup),
				FractalManager.getInstance().membership.group(agroup),
				FractalManager.getInstance().membership.group(lgroup),
				cstructClassName,
				useFastBallot,
				recovery,
				ltimeout,
				checkpointSize);

		streams.put(streamName,s);

	}

	public GPaxosStream getOrCreateGPaxosStream(
			String streamName,
			String proposerGroupName,
			String acceptorGroupName,
			String learnerGroupName,
			String cstructClassName,
			boolean useFastBallot,
			RECOVERY recovery,
			int ballotTimeOut,
			int checkpointSize){

		if( recovery == RECOVERY.FGGC
				&&
				! useFastBallot ){
			throw new RuntimeException("Invalid usage: FGGC => useFastBallots");	
		}

		GPaxosStream s = new GPaxosStream(streamName,
				FractalManager.getInstance().membership.myId(),
				FractalManager.getInstance().membership.group(proposerGroupName),
				FractalManager.getInstance().membership.group(acceptorGroupName),
				FractalManager.getInstance().membership.group(learnerGroupName),
				cstructClassName,
				useFastBallot,
				recovery,
				ballotTimeOut,
				checkpointSize);

		streams.put(streamName,s);
		return s;
	}


	public GPaxosStream stream(String streamName){
		return streams.get(streamName);
	}	


}
