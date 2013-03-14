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

	public GPaxosStream getOrCreateGPaxosStream(
			FractalManager manager,
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
				manager.membership.myId(),
				manager.membership.group(proposerGroupName),
				manager.membership.group(acceptorGroupName),
				manager.membership.group(learnerGroupName),
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
