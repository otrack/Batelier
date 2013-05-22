package net.sourceforge.fractal;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;

import net.sourceforge.fractal.abcast.ABCastStream;
import net.sourceforge.fractal.abcast.ABCastStreamManager;
import net.sourceforge.fractal.broadcast.BroadcastStream;
import net.sourceforge.fractal.broadcast.BroadcastStreamManager;
import net.sourceforge.fractal.commit.CommitStreamManager;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream.RECOVERY;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStreamManager;
import net.sourceforge.fractal.consensus.paxos.PaxosStream;
import net.sourceforge.fractal.consensus.paxos.PaxosStreamManager;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.multicast.MulticastStreamManager;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.XMLUtils;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;
import net.sourceforge.fractal.wanamcast.WanAMCastStreamManager;

import org.w3c.dom.Node;


/**   
* @author L. Camargos
* @author P. Sutra
* 
*/ 

public class FractalManager {
    

	public static final String PAXOS_COMMIT = "paxosCommit";
    public static final String ABCAST_COMMIT = "abcastCommit";

    public Membership membership;
    
    public PaxosStreamManager paxos;
    public GPaxosStreamManager gpaxos;
    public ABCastStreamManager abcast;
    public BroadcastStreamManager broadcast;
    public MulticastStreamManager multicast;
    public WanAMCastStreamManager wanamcast;
    public Map<String,CommitStreamManager>commit = CollectionUtils.newMap();;
    
    public FractalManager() {
    	membership = new Membership();
    	paxos = new PaxosStreamManager();
    	gpaxos = new GPaxosStreamManager();
    	abcast = new ABCastStreamManager();
    	broadcast = new BroadcastStreamManager();
    	multicast = new MulticastStreamManager();
    	wanamcast = new WanAMCastStreamManager();
    }
    
    
    //
    // INTERFACES
    //
   
    public void stop() {
    	// FIXME stop streams
    	membership.stop();
    }

    public void start() {
    	// FIXME start streams
	   membership.start();
    }
    
    public BroadcastStream getOrCreateBroadcastStream(String streamName, String groupName){
    	return broadcast.getOrCreateBroadcastStream(this, streamName, groupName);
    }
    
    public PaxosStream getOrCreatePaxosStream(String streamName, String proposerGroupName, String acceptorGroupName, String learnerGroupName) {
    	Group proposers = membership.group(proposerGroupName);
    	Group acceptors = membership.group(acceptorGroupName);
    	Group learners = membership.group(learnerGroupName);
    	return paxos.getOrCreatePaxosStream(this, streamName, proposers, acceptors, learners);
    }
    
    public PaxosStream getOrCreatePaxosStream(String streamName, String groupName) {
    	return getOrCreatePaxosStream(streamName, groupName, groupName, groupName);
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
    	return gpaxos.getOrCreateGPaxosStream(
    							this,
    							streamName,
    							proposerGroupName,
    							acceptorGroupName,
    							learnerGroupName,
    							cstructClassName,
    							useFastBallot,
    							recovery,
    							ballotTimeOut,
    							checkpointSize);
    	
    }
    
    
    public ABCastStream getOrCreateABCastStream(String streamName, String groupName) {
    	return abcast.getOrCreateABCastStream(
    			this,
    			streamName,
    			getOrCreatePaxosStream(streamName, groupName),
    			getOrCreateBroadcastStream(streamName, groupName));
    }
    
    /**
     * Return a reliable multicast stream having the name <p>streamName<p> for the group <p>groupName</p>.
     * The stream is created if necessary.
     */
    public MulticastStream getOrCreateMulticastStream(String streamName, String groupName){
    	getOrCreateBroadcastStream(streamName, groupName);
    	return multicast.getOrCreateRMCastStream(this, streamName,groupName);
    }
    
    /**
     * Return an atomic multicast stream having the name <p>streamName<p> for the group <p>groupName</p>.
     * The stream is created if necessary.
     */
    public WanAMCastStream getOrCreateWanAMCastStream(String streamName, String groupName) {
    	getOrCreatePaxosStream(streamName, groupName);
    	getOrCreateMulticastStream(streamName, groupName);
    	return wanamcast.getOrCreateWanAMCastStream(this, streamName,groupName,streamName,streamName);
    }
    
    /**
     * Return an atomic multicast stream having the name <p>streamName<p> for the group <p>groupName</p>.
     * The stream does not provide the acyclicity property; it is created if necessary.
     */
    public WanAMCastStream getOrCreateWanNonAcyclicAMCastStream(String streamName, String groupName) {
    	getOrCreatePaxosStream(streamName, groupName);
    	getOrCreateMulticastStream(streamName, groupName);
    	return wanamcast.getOrCreateWanNonAcyclicAMCastStream(this, streamName,groupName,streamName,streamName);
    }
    
    
    public void loadFile(String configFile) throws UnknownHostException{
    	
     	if(XMLUtils.openFile(configFile)==null){
    		throw new RuntimeException("Invalid XML file");
    	}
        
     	Node config = XMLUtils.getChildByName(XMLUtils.openFile(configFile), "FRACTAL");
    	
    	if(config==null){
    		throw new RuntimeException("Invalid XML file ");
    	}
        
        Node confNode;
        Set<Node> confNodes = null;
        
        confNode = XMLUtils.getChildByName(config, "BootstrapIdentity");
        if(null != confNode){
        	membership.loadNodes(confNode);
        }
                
        confNode = XMLUtils.getChildByName(config, "BootstrapMembership");
        if(null != confNode){
        	membership.loadGroups(confNode);
        }

    }


}
