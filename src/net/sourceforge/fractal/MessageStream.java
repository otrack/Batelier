/*
 Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
 file attached to it to see how you can use it, edit or distributed.
 */


package net.sourceforge.fractal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**   
* @author L. Camargos
* @author P. Sutra
* 
*/ 



abstract public class MessageStream {
    static final public Map<Integer,Class> idToMessage = new LinkedHashMap<Integer,Class>();
    static final public Map<Class,Integer> messageToId = new LinkedHashMap<Class,Integer>();
    static final String classPrefix = "net.sourceforge.fractal.";
    
    public synchronized static void addClass(String className) {
    	try {
    		int id = idToMessage.size();
			idToMessage.put(id, Class.forName(className));
	    	messageToId.put(Class.forName(className), id);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
    }
    
    static{
        try {
        	
    		MessageStream.addClass(String.class.getName());
    		MessageStream.addClass(Integer.class.getName());
    		MessageStream.addClass(HashMap.class.getName());
    		MessageStream.addClass(HashSet.class.getName());
    		MessageStream.addClass(ArrayList.class.getName());
    		
            /* Add here the names of your message classes. */
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"Message"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"UMessage"));
            
            /* ABCast */
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"abcast.ABCastMessage"));
            
            /* Paxos */
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.paxos.PMessage"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.paxos.PMBroadcast"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.paxos.PMDecision"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.paxos.PMLogRequest"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.paxos.PMNACK"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.paxos.PMPhase1A"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.paxos.PMPhase1B"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.paxos.PMPhase2A"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.paxos.PMPhase2B"));
            
            /* GPaxos */
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.ProposeMessage"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.GPMessage1A"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.GPMessage1B"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.GPMessage2A"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.GPMessage2AStart"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.GPMessage2AClassic"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.GPMessage2BClassic"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.GPMessage2B"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.GPMessage2BStart"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.GPMessage2BFast"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.cstruct.CStruct"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.cstruct.CSched"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.cstruct.CHistory"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"consensus.gpaxos.cstruct.CSet"));
            
            /* Reliable Broadcast */
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"broadcast.BroadcastMessage"));

            /* Reliable Multicast */
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"multicast.MulticastMessage"));
            
            /* Wan Atomic Broadcast */
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"wanabcast.WanABCastInterGroupMessage"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"wanabcast.WanABCastIntraGroupMessage"));
            
            /* Wan Atomic Multicast */
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"wanamcast.WanAMCastInterGroupMessage"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"wanamcast.WanAMCastMessage"));
            
            /* Fault Tolerant Wan Atomic Multicast */
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"ftwanamcast.FTWanAMCastInterGroupMessage"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"ftwanamcast.FTWanAMCastInterGroupAck"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"ftwanamcast.FTWanAMCastIntraGroupMessage"));
            
            /* State Machine Replication */
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"replication.Command"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"replication.CommutativeCommand"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"replication.CheckpointCommand"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"replication.SimpleCommand"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"replication.ArrayCommutativeCommand"));
            idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"replication.ReadWriteRegisterCommand"));
            
			/* Database Protocol */
			idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"replication.database.ReadReplyMessage"));
			idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"replication.database.ReadRequestMessage"));
			idToMessage.put(idToMessage.size(), Class.forName(classPrefix+"replication.database.certificationprotocol.genuine.VoteMessage"));
            
        } catch (ClassNotFoundException e1) { //All or nothing.
            e1.printStackTrace();
            for(Integer k: idToMessage.keySet()){
                idToMessage.remove(k);
            }
        }finally{
            for(Entry<Integer,Class> e: idToMessage.entrySet()){
                messageToId.put(e.getValue(),e.getKey());
            }
        }
    }    
}
