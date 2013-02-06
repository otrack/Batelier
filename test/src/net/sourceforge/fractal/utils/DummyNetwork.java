package net.sourceforge.fractal.utils;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;

public class DummyNetwork {

	private static final String lub="10.0.0.0";
	private static final Executor executor = new DefaultExecutor();
	private static CommandLine cmdL;
	
	private static Map<Node,Membership> network = new TreeMap<Node, Membership>();
	
	public static Map<Node,Membership> create(int nnodes){

		try {
			
			cmdL=CommandLine.parse("sudo modprobe -a dummy"); 
			if(executor.execute(cmdL)!=0)
				return null;
			
			cmdL=CommandLine.parse("sudo ifconfig dummy0 "+lub+" up");
			if(executor.execute(cmdL)!=0)
				return null;

			for(int i=0;i<nnodes;i++){

				String ip = lub.substring(0,7);
				ip+=Integer.toString(i+1);
				Node n = new Node(i,ip);

				cmdL=CommandLine.parse("sudo ifconfig dummy0:"+i+" "+n.ip+" up");
				if(executor.execute(cmdL)!=0)
					return null;

				Membership m = new Membership(n.id,n.ip);
				network.put(n,m);

			}			
			
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return network;
	}
	
	public static Map<Node,Membership> create(int nnodes, int ngroups){
		if(ConstantPool.TEST_DL != 1 ){
			System.err.println("ConstantPool.TEST_DL mist equal 1");
			System.exit(-1);
		}
		 create(nnodes);
		 for(Node n: network.keySet()){
			 for(Node m : network.keySet()){
				 network.get(n).addNode(m.id, m.ip);
			 }
		 }

		 for(Node n: network.keySet()){
			 network.get(n).dispatchPeers(ngroups);
		 }
		 
		 for(Node n: network.keySet()){
			 for(Group g : network.get(n).allGroups()){
				 g.start();
			 }
		 }
		 
		 return network;
	}
	
	public static void destroy(){
		try {
			
			cmdL=CommandLine.parse("sudo ifconfig dummy0 down");
			executor.execute(cmdL);
			
			for(int i=0; i<network.keySet().size();i++){
				cmdL = CommandLine.parse("sudo ifconfig dummy0:"+i+" down");
				executor.execute(cmdL);
			}			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
		
}
