package net.sourceforge.fractal.membership;

/**
 * @author Pierre Sutra
 * @author Lasaro camargos
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.IPUtils;
import net.sourceforge.fractal.utils.XMLUtils;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


// FIXME thread safety
// FIXME comment this class
public class Membership {
	
	// This node related fields.
    private int myId;
	
    // Membership management fields.
    private Map<Integer,String> swid2ip;
    private Map<String,Integer> ip2swid;
    private Map<String,Group> allGroups;
        
    public Membership(int swid, String ip){
    	myId=swid;
        swid2ip = CollectionUtils.newSortedMap();
        ip2swid = CollectionUtils.newMap();
        allGroups = CollectionUtils.newSortedMap();
        addNode(swid, ip);
    }
    
    public Membership(){
    	myId = -1;
        swid2ip = CollectionUtils.newSortedMap();
        ip2swid = CollectionUtils.newMap();
        allGroups = CollectionUtils.newSortedMap();
    }
    
	public void start() {
		for(Group g : allGroups.values())
    		g.start();
		
	}
	
	public void stop(){
		for(Group g : allGroups.values())
    		g.stop();
	}

	//
	// This node related method.
	//
	
	public int myId() {
		return myId;
	}
	
	public String myIP(){
		return swid2ip.get(myId);
	}
	
	public Collection<Group> myGroups(){
		return groupsOf(myId);
	}

	//
	// Group management
	//
	
    /**
     * 
     * @param the name of the group
     * @return the group whose name is <i>name</i>
     */
    public Group group(String name){
    	if(!allGroups.containsKey(name))
    		return null;
        return allGroups.get(name);
    }
    
    public TCPGroup getOrCreateTCPGroup(String name, int port)  throws IllegalArgumentException {
    	if(allGroups.containsKey(name) && !(allGroups.get(name) instanceof TCPGroup))
    		throw new IllegalArgumentException("A group of the same name but wrong type exists");    	
    	if(allGroups.containsKey(name))
    		return (TCPGroup) allGroups.get(name);
    	TCPGroup ret = new TCPGroup(this,name,port);
    	allGroups.put(name,ret);
    	return ret;
    }

    public TCPDynamicGroup getOrCreateTCPDynamicGroup(String name, int port)  throws IllegalArgumentException {
    	if(allGroups.containsKey(name) && !(allGroups.get(name) instanceof TCPDynamicGroup))
    		throw new IllegalArgumentException("A group of the same name but wrong type exists");
    	if(allGroups.containsKey(name))
    		return (TCPDynamicGroup) allGroups.get(name);
    	TCPDynamicGroup ret = new TCPDynamicGroup(this,name,port);
    	allGroups.put(name, ret);
    	return ret;
    }

    public UDPGroup getOrCreateUDPGroup(String name, String ip, int port)  throws IllegalArgumentException {
    	if(allGroups.containsKey(name) && !(allGroups.get(name) instanceof UDPGroup))
    		throw new IllegalArgumentException("A group of the same name but wrong type exists");   
    	if(allGroups.containsKey(name))
    		return (UDPGroup) allGroups.get(name);
    	UDPGroup ret = new UDPGroup(this,name,ip, port);
    	allGroups.put(name, ret);
    	return ret;
    }   
        
    public Collection<Group> allGroups(){
    	return allGroups.values();
    }

    /**
     * 
     * Return the set of groups containing substring. 
     * 
     * 
     * @param substring
     * @return
     */
    public Collection<Group> allGroups(String substring){
    	Collection<Group> result = new HashSet<Group>();
    	for(Group g : allGroups.values()){
    		if(Pattern.matches(".*"+substring+".*",g.name()))
    			result.add(g);
    	}
    	return result;
    }

    
	/**
	 * Ventilate all the peers in <i>nbGroups</i> groups having the same size.
	 * This is done deterministically by sorting peers according to their SWIDs.
	 * 
	 * The names of the groups are the integers in [0,nbGroups-1].
	 * 
	 * @param nbGroups
	 * @return the groups 
	 */
	public Collection<Group> dispatchPeers(int nbGroups) {
		if(nbGroups==0)
			throw new IllegalArgumentException("Cannot divide the peers in 0 group");
		if(swid2ip.size()%nbGroups!=0)
			throw new IllegalArgumentException("Cannot divide the peers equally: "+swid2ip.size()+"%"+nbGroups+"!=0");		
		List<Integer> peers = new ArrayList<Integer>(swid2ip.keySet()); 
		
		for(int i=0; i<nbGroups;i++){
			Group g = getOrCreateTCPGroup(String.valueOf(i),8880+i);
			for(int p = i*(swid2ip.size()/nbGroups); p<(i+1)*(swid2ip.size()/nbGroups); p++ ){
				g.putNode(peers.get(p),swid2ip.get(peers.get(p)));
			}
		}
		return allGroups.values();
	}
	
	/**
	 * Dispatch nodes in a set of group containing at most size peers per group.
	 * The name of the groups are prefixK where K in 0..n-1 with n equals the nb of groups.
	 *  
	 * @param prefix this string prefix the name of each group. 
	 * @param port the port the gorup will listen to
	 * @param size this is the size of each group
	 * @return a collection of groups.
	 */
	public Collection<Group> dispatchPeers(String prefix, int port, int size) {
		
		if(size==0 || port <0 || port > 65536) 
			throw new IllegalArgumentException("invalid size or port");
		
		Collection<Group> ret = new HashSet<Group>();
		List<Integer> peers = new ArrayList<Integer>(swid2ip.keySet()); 
		Group g = null;
		for(int i=0; i<peers.size(); i++){
			if(i%size==0){
				g = getOrCreateTCPGroup(prefix+String.valueOf(i),port);
				ret.add(g);
			}
			int p = peers.get(i);
			g.putNode(p,swid2ip.get(p));
		}
		
		return ret;
	}
    
    //
    // Node management
    //
    
	/**
	 * Remove node {@code swid} iff no group references it.
	 */
	public synchronized boolean removeNode(int swid){
		if(groupsOf(swid).size()!=0) return false;
		ip2swid.remove(swid2ip.get(swid));
		swid2ip.remove(swid);
		return true;
	}
	
    public synchronized boolean addNode(int swid, String ip){
    	if( ip2swid.containsKey(ip)  ||  swid2ip.containsKey(swid))  return false;
    	swid2ip.put(swid, ip);
    	ip2swid.put(ip,swid);
    	if(ConstantPool.MEMBERSHIP_DL>1)
    		System.out.println(this+" add node "+swid);
    	return true;
    }

    public synchronized boolean addNodeWithIPTo(int swid, String ip, Group g) throws IllegalArgumentException{
    	
    	if(!allGroups.containsKey(g.name()))
    		throw new IllegalArgumentException("Group "+g+" is not registered");

    	addNode(swid,ip);    	
    	
    	if(allGroups.get(g.name()).putNode(swid, ip)){
    		if(g instanceof UDPGroup) swid2ip.put(swid,ip);
    		return true;
    	}
    	return false;
    }
    
	public synchronized boolean addNodeTo(int swid, String groupName){
    	return addNodeWithIPTo(swid, swid2ip.get(swid),group(groupName));
    }
    	
    
    public synchronized boolean addNodeTo(int swid, Group g){
    	return addNodeWithIPTo(swid, swid2ip.get(swid),g);
    }
            
    public Set<Integer> allNodes(){
    	return swid2ip.keySet();
    }
    
    public int peerIdOf(String address){
    	return ip2swid.get(address);
    }
    
    public Set<Group> groupsOf(int swid){
    	Set<Group> ret = new HashSet<Group>();
    	for(Group g : allGroups.values()){
    		if(g.contains(swid))
    			ret.add(g);
    	}
    	return ret;
    }
    
    public String adressOf(int swid){
    	return swid2ip.get(swid);
    }
    
    //
    // Bootstrapping methods
    //
    
    /**
     * Load the ID of this node using the network identifier <p>myNetName</p>.
     * If this parameter equals <p>null</p>, this method tries using the local IP addresses of this machine
     * to assign an ID.
     * 
     * The ID is determined using the current mapping of known IP addresses to IDs. 
     * If no IP matches then a random ID is assigned, and the node is registered.
     * In such a case, the IP address that is use is a IPv4 address that is neither 127.0.0.1, nor 0.0.0.0 .
     * 
     * @param <p>myNetName</p> either an IP address or a hostname
     */
    public void loadIdenitity(String myNetName){
    	
        Set<String> myIPs = new HashSet<String>();
        if(myNetName == null){
        	myIPs = IPUtils.getLocalIpAddresses();
        }else if(myNetName.equals("modelnet")){
        	myIPs = new HashSet<String>();
        	myIPs.add(System.getenv("SRCIP"));
        }else{
        	myIPs = new HashSet<String>();
        	try {
				myIPs.add(InetAddress.getByName(myNetName).getHostAddress());
			} catch (UnknownHostException e) {
				throw new IllegalArgumentException(e.getMessage());
			}
        }
        
        if(myIPs.isEmpty())
        	throw new RuntimeException("Unable to obtain an IP.");

        if(ConstantPool.MEMBERSHIP_DL > 0)
   		 	System.out.println(this+" found local IP addresses "+myIPs);

        for(String ip : myIPs){
        	if(ip2swid.containsKey(ip)){
        		if( myId == -1 ){
        			myId=ip2swid.get(ip);
        		}else{
        			throw new IllegalArgumentException("multiple ids for the same node.");
        		}
        	}
        }
        		
        if(myId == -1){
        	
        	if(ConstantPool.MEMBERSHIP_DL > 0)
        		 System.out.println(this+" assigning a random ID and a global scope address.");
        	
        	Random rand = new Random(System.currentTimeMillis());
        	myId = Math.abs(rand.nextInt());
			try {
				// Get global interface
				Process p = Runtime.getRuntime().exec("netstat -rn");
			    BufferedReader output = new BufferedReader(new InputStreamReader(p.getInputStream()));
			    String line;
			    while((line=output.readLine())!=null ){
					if(Pattern.compile("^0.0.0.0").matcher(line).find()){
					    NetworkInterface ni = NetworkInterface.getByName(line.split("\\s+")[7]);
					    addNode(myId,IPUtils.getIPforInterface(ni).iterator().next());
					    break;
					}		
			    }
			} catch (IOException e) {
				throw new RuntimeException("Unable to determine a global scope IP.");
			}
			
        }
        
        if(ConstantPool.MEMBERSHIP_DL > 0)
        	System.out.println(this+" found myself as "+ myId+" "+myIP());
  
    }
    
    public void loadNodes(Node config){
        Element el = (Element) XMLUtils.getChildByName(config, "nodelist");
        if(el==null){
        	throw new RuntimeException("no nodelist described.");
        }
        
        NodeList nl = el.getElementsByTagName("node");
        if(null == nl) {
            throw new RuntimeException("nodelist is empty.");        
        }

        // Build list of nodes
        for(int i = 0; i < nl.getLength(); i++) {
        	el = (Element) nl.item(i); 
        	int id = Integer.parseInt(el.getAttribute("id"));
        	String ip = el.getAttribute("ip"); 
        	swid2ip.put(id,ip);
        	ip2swid.put(ip, id);
        }
        
        if(ConstantPool.MEMBERSHIP_DL > 0)
        	System.out.println(this+" load nodes " + swid2ip);

    }
    
    public void loadGroups(Node config){ 
        Element el = (Element) XMLUtils.getChildByName(config, "grouplist");
        NodeList nl = el.getElementsByTagName("group");
        Group gp;
        
        for(int i = 0; i < nl.getLength(); i++) {
        	
        	el = (Element) nl.item(i);
        	
        	if( ! XMLUtils.getAttribByName(el, "initialPLeader").equals("") ) 
				System.err.println("initialPLeader is now deprecated");
        	
        	if( ! XMLUtils.getAttribByName(el, "pLeaderTO").equals("") ) 
        		System.err.println("pLeaderTOis now deprecated");
        	
        	if( XMLUtils.hasAttribByName(el,"tcp") && XMLUtils.getAttribByName(el, "tcp").equals("true")){        		
        		gp = new TCPGroup(this,el.getAttribute("name"),Integer.parseInt(el.getAttribute("port")));
        	}else{
        		gp = new UDPGroup(this,el.getAttribute("name"), el.getAttribute("ip"), Integer.parseInt(el.getAttribute("port")));
        	}
        	
            NodeList nl2 = el.getElementsByTagName("node");
            	
            for(int j = 0; j < nl2.getLength(); j++) {
            	int id = Integer.parseInt(el.getAttribute("id"));
            	String iface;
            	
                el = (Element) nl2.item(j);
                if(el.getAttribute("iface").equals("")){
                	iface = swid2ip.get(id);
                }else{
                	iface= el.getAttribute("ifaceNo");
                	assert swid2ip.get(id).contains(iface);
                }
                gp.putNode(id, iface);
            }
            
            allGroups.put(gp.name(), gp);
            if(ConstantPool.MEMBERSHIP_DL > 0)
            	System.out.println(this+" load groups = "+ allGroups);            

        }
    }
    
    //
    // Misc
    //
    
    public String toString(){
    	return "Membership";
    }
    
}
