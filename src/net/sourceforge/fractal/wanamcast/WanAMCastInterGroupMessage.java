package net.sourceforge.fractal.wanamcast;

import java.io.Serializable;
import java.util.Collection;

import net.sourceforge.fractal.Messageable;
import net.sourceforge.fractal.multicast.MulticastMessage;


public class WanAMCastInterGroupMessage extends MulticastMessage {

	private static final long serialVersionUID = Messageable.FRACTAL_MID;
	
	public WanAMCastInterGroupMessage(){
		
	}
	
	public WanAMCastInterGroupMessage(Serializable s,Collection<String> dest,String gSource,int swid){
		super(s,dest,gSource, swid);
	}
	
}
