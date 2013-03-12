package net.sourceforge.fractal.multicast;

/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.
This library is free software; you can redistribute it and/or modify it under     
the terms of the GNU Lesser General Public License as published by the Free Software 	
Foundation; either version 2.1 of the License, or (at your option) any later version.      
This library is distributed in the hope that it will be useful, but WITHOUT      
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      
PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      
You should have received a copy of the GNU Lesser General Public License along      
with this library; if not, write to the      
Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/


/**
 * 
 * @author Pierre Sutra
 * 
 */

import java.io.Serializable;
import java.util.Collection;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;


/**   
* @author P. Sutra
* 
*/

public final class MulticastStream extends Stream implements Learner {

		private String streamName;
		private Group group;
		private Membership membership;

		private boolean isTerminated;
		
		public MulticastStream(String name, Group group, Membership membership){
			this.membership = membership;
			this.group = group;
			streamName = name;				
			this.isTerminated= false;
			
		}

		@Override
		public boolean registerLearner(String msgType, Learner learner){
			
			if(ConstantPool.MULTICAST_DL > 0) 	System.out.println(this+" register learner for "+msgType);
			
			if(super.registerLearner(msgType, learner)){
				group.registerLearner(msgType, this);
				return true;
			}
			
			return false;
		}
		
		@Override
		public boolean unregisterLearner(String msgType, Learner learner){
			
			if(ConstantPool.MULTICAST_DL > 0) System.out.println(this+" unregister learner for "+msgType);
			
			if(super.unregisterLearner(msgType, learner)){
				group.unregisterLearner(msgType, this);
				return true;
			}
			
			return false;
		}

		@Override
		public synchronized void start(){
			isTerminated = false;
		}
			
		@Override
		public void stop(){
			for(String registeredType : learners.keySet()){
				for(Learner l : learners.get(registeredType)){
					unregisterLearner(registeredType, l);
					group.unregisterLearner(registeredType, this);
				}
			}
			isTerminated=true;
		}
				
		public void multicast(Serializable s, Collection<String> dest){
			if(isTerminated)	throw new IllegalStateException("stream stopped");
			
			multicast(new MulticastMessage(s,dest,group.name(),membership.myId()));
		}

		public void multicast(MulticastMessage m){
			
			if(isTerminated)	throw new IllegalStateException("stream stopped");
			
			if(ConstantPool.MULTICAST_DL > 1 )
				System.out.println(this+" multicast "+ m+" to "+m.getDest());
			
			for(String g : m.getDest()){
				if(membership.group(g)==null)
					throw new IllegalArgumentException("Group "+g+" does not exist.");
				membership.group(g).broadcast(m);
			}
			
		}
		
		/**
		 * Unicast a message to a node in the group of the sending node.
		 * 
		 * @param m the message 
		 * @param swid the node
		 */
		public void unicast(MulticastMessage m, int swid){
			
			if(isTerminated)	throw new IllegalStateException("stream stopped");
			
			if(ConstantPool.MULTICAST_DL > 1 )
				System.out.println(this+" unicast "+ m+" to "+swid);
			
			group.unicast(swid,m);
		}
		
		/**
		 * Unicast message <i>m</i> to node<i>swid</i>  in group <i>g</i>.
		 * 
		 * @param m the message 
		 * @param swid the node
		 */
		public void unicast(MulticastMessage m, int swid, Group g){
			
			if(isTerminated)	throw new IllegalStateException("stream stopped");
			
			if(ConstantPool.MULTICAST_DL > 1 )
				System.out.println(this+" unicast "+ m+" to "+swid);
			
			g.unicast(swid,m);
		}

		public void learn(Stream stream,Serializable s) {
			deliver(s);
		}
		

		@Override
		public void deliver(Serializable s) {
			Message m = (Message)s;
			if( learners.containsKey(m.getMessageType()) && !learners.get(m.getMessageType()).isEmpty() ){
				for(Learner l : learners.get(m.getMessageType())){
					if(ConstantPool.MULTICAST_DL > 1)
						System.out.println(this+" deliver "+m+" to "+l);
					l.learn(this,(m));
				}
			}else{
				System.out.println(this+" got a "+ m.getMessageType() +" to nobody");
			}
		}
		
		public String toString(){
			return "Multicast:" +streamName+"@"+membership.myId();
		}
		
}
