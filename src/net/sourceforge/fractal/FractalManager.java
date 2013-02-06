package net.sourceforge.fractal;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import net.sourceforge.fractal.abcast.ABCastStream;
import net.sourceforge.fractal.abcast.ABCastStreamManager;
import net.sourceforge.fractal.broadcast.BroadcastStream;
import net.sourceforge.fractal.broadcast.BroadcastStreamManager;
import net.sourceforge.fractal.commit.CommitStreamManager;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStreamManager;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream.RECOVERY;
import net.sourceforge.fractal.consensus.paxos.PaxosStream;
import net.sourceforge.fractal.consensus.paxos.PaxosStreamManager;
import net.sourceforge.fractal.ftwanamcast.FTWanAMCastStream;
import net.sourceforge.fractal.ftwanamcast.FTWanAMCastStreamManager;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.multicast.MulticastStreamManager;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.XMLUtils;
import net.sourceforge.fractal.wanabcast.WanABCastStream;
import net.sourceforge.fractal.wanabcast.WanABCastStreamManager;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;
import net.sourceforge.fractal.wanamcast.WanAMCastStreamManager;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**   
* @author L. Camargos
* @author P. Sutra
* 
*/ 

public class FractalManager {
    
	public static final String PAXOS_COMMIT = "paxosCommit";
    public static final String ABCAST_COMMIT = "abcastCommit";

    private static FractalManager instance;
    public Membership membership;
    
    private Node config;
    
    private String logName;
    
    public PaxosStreamManager paxos;
    public GPaxosStreamManager gpaxos;
    public ABCastStreamManager abcast;
    public BroadcastStreamManager broadcast;
    public MulticastStreamManager multicast;
    public WanABCastStreamManager wanabcast;
    public WanAMCastStreamManager wanamcast;
    public FTWanAMCastStreamManager ftwanamcast;
    public Map<String,CommitStreamManager>commit = CollectionUtils.newMap();;
    
    private FractalManager() {
    	membership = new Membership();
    	logName = "/tmp/Fractal.log";
    }
    
    
    //
    // INTERFACES
    //

    /**
     * Start a new FractalManager without initializing it.
     * @return
     */
    public static FractalManager getInstance(){
    	if(instance==null)
    		instance = new FractalManager();
    	return instance;
    } 
   
    public void stop() {
    	// FIXME stop streams
    	membership.stop();
    }

    public void start() {
    	// FIXME start streams
	   membership.start();
    }
    
    public BroadcastStream getOrCreateBroadcastStream(String streamName, String groupName){
    	return broadcast.getOrCreateBroadcastStream(streamName, groupName);
    }
    
    public PaxosStream getOrCreatePaxosStream(String streamName, String proposerGroupName, String acceptorGroupName, String learnerGroupName) {
    	if(paxos==null){
    		paxos = new PaxosStreamManager(logName);
    	}
    	return paxos.getOrCreatePaxosStream(streamName, proposerGroupName, acceptorGroupName, learnerGroupName);
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
    	
    	 if(gpaxos==null){
    		 gpaxos = new GPaxosStreamManager();
    	 }
    	
    	return gpaxos.getOrCreateGPaxosStream(
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
    	getOrCreateBroadcastStream(streamName, groupName);
    	getOrCreatePaxosStream(streamName, groupName);
    	return abcast.getOrCreateABCastStream(streamName, streamName, streamName);
    }
    
    public ABCastStream getOrCreateABCastStream(String streamName, String rbStreamName, String paxosStreamName){
    	return abcast.getOrCreateABCastStream(streamName,rbStreamName, paxosStreamName);
    }
    
    /**
     * Return a reliable multicast stream having the name <p>streamName<p> for the group <p>groupName</p>.
     * The stream is created if necessary.
     */
    public MulticastStream getOrCreateMulticastStream(String streamName, String groupName){
    	getOrCreateBroadcastStream(streamName, groupName);
    	return multicast.getOrCreateRMCastStream(streamName,groupName);
    }
    
    /**
     * Return an atomic multicast stream having the name <p>streamName<p> for the group <p>groupName</p>.
     * The stream is created if necessary.
     * 
     * The stream makes use of boh a multicast stream and a paxos stream.
     * These streams are created using the same parameters if necessary.
     */
    public WanAMCastStream getOrCreateWanAMCastStream(String streamName, String groupName) {
    	getOrCreatePaxosStream(streamName, groupName);
    	getOrCreateMulticastStream(streamName, groupName);
    	return wanamcast.getOrCreateWanAMCastStream(streamName,groupName,streamName,streamName,streamName,true);
    }
    
    public WanABCastStream getOrCreateWanABCastStream(String streamName, Collection<String> allGroupsNames, String groupName, 
    												  int maxInterGroupMessageDelay, int consensusLatency) {
    	getOrCreateBroadcastStream(streamName, groupName);
    	getOrCreatePaxosStream(streamName, groupName);
    	getOrCreateMulticastStream(streamName, groupName);
    	return wanabcast.getOrCreateWanABCastStream(streamName, groupName, allGroupsNames, streamName, streamName, streamName,
    												maxInterGroupMessageDelay, consensusLatency);
    }
    
    public FTWanAMCastStream getOrCreateFTWanAMCastStream(String streamName, Collection<String> allGroupsNames, String groupName, 
    		int maxInterGroupMessageDelay, int consensusLatency) {
    	getOrCreateBroadcastStream(streamName, groupName);
    	getOrCreatePaxosStream(streamName, groupName);
    	getOrCreateMulticastStream(streamName, groupName);
    	return ftwanamcast.getOrCreateFTWanAMCastStream(streamName, groupName, allGroupsNames, streamName, streamName, streamName,
    			maxInterGroupMessageDelay, consensusLatency);
    }

    //
    // BootStrapping functions
    //
    
    /**
     * 
     * @param configFile facultative , initialize FractalManager with the content of the XML file <i>configFile</i>
     * @param myNetName Fractal assigns the ID corresponding to <i>myNetName</i>.
     * 		  If <i>ip</i> equals null, Fractal tries to assign an swid using the set of IP addresses of this host.
     * @return create a new FractalManger 
     */
    public static FractalManager init(String configFile, String myNetName){
    	
    	if(instance==null){	    		
    		instance = new FractalManager();
    	}else{
    		throw new RuntimeException("Fractal manager already created");
    	}
    	
    	if(configFile!=null){
			try {
				instance.loadFile(configFile);
				instance.membership.loadIdenitity(myNetName);
			} catch (UnknownHostException e) {
				throw new RuntimeException(e.getMessage());
			}
		}
    	
    	return instance;
    }
    
    public void loadFile(String configFile) throws UnknownHostException{
    	
     	if(XMLUtils.openFile(configFile)==null){
    		throw new RuntimeException("Invalid XML file");
    	}
        
    	config = XMLUtils.getChildByName(XMLUtils.openFile(configFile), "FRACTAL");
    	
    	if(config==null){
    		throw new RuntimeException("Invalid XML file ");
    	}
        
        Node confNode;
        Set<Node> confNodes = null;
        
        confNode = XMLUtils.getChildByName(config, "globals");
        
        if(null != confNode){
            String confLogFile = XMLUtils.getAttribByName((Element) confNode, "logFile");
            if(confLogFile != null)
                logName = confLogFile;
        }
                        
        confNode = XMLUtils.getChildByName(config, "BootstrapIdentity");
        if(null != confNode){
        	membership.loadNodes(confNode);
        }
                
        confNode = XMLUtils.getChildByName(config, "BootstrapMembership");
        if(null != confNode){
        	membership.loadGroups(confNode);
        }

        broadcast = new BroadcastStreamManager();
        confNodes = XMLUtils.getChildrenByName(config, "BroadcastStream");
        if(!confNodes.isEmpty()){
            for(Node n: confNodes){
                broadcast.load(n);
            }
        }

        multicast = new MulticastStreamManager();
        confNodes = XMLUtils.getChildrenByName(config, "MulticastStream");
        if(!confNodes.isEmpty()){
            for(Node n: confNodes){
                multicast.load(n);
            }
        }        

        confNodes = XMLUtils.getChildrenByName(config, "PaxosStream");
        if(!confNodes.isEmpty()){
        	paxos = new PaxosStreamManager(logName);
            for(Node n: confNodes){
                paxos.load(n);
            }
        }

        confNodes = XMLUtils.getChildrenByName(config, "GPaxosStream");
        if(!confNodes.isEmpty()){
            gpaxos = new GPaxosStreamManager();
            for(Node n: confNodes){
                gpaxos.load(n);
            }
        }
      
        abcast = new ABCastStreamManager();
        confNodes = XMLUtils.getChildrenByName(config, "ABCastStream");
        if(!confNodes.isEmpty()){
            for(Node n: confNodes){
                abcast.load(n);
            }
        }
        
        confNodes = XMLUtils.getChildrenByName(config, "WanAtomicBroadcastStream");
        wanabcast= new WanABCastStreamManager();
        if(!confNodes.isEmpty()){
            for(Node n: confNodes){
                wanabcast.load(n);
            }
        }
        
        wanamcast= new WanAMCastStreamManager();
        confNodes = XMLUtils.getChildrenByName(config, "WanAtomicMulticastStream");
        if(!confNodes.isEmpty()){
            for(Node n: confNodes){
                wanamcast.load(n);
            }
        }
        
        confNodes = XMLUtils.getChildrenByName(config, "FaultTolerantWanAtomicMulticastStream");
        ftwanamcast= new FTWanAMCastStreamManager();
        if(!confNodes.isEmpty()){
            for(Node n: confNodes){
                ftwanamcast.load(n);
            }
        }
    }

}
