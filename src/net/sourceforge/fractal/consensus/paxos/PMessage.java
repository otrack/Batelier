/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/


package net.sourceforge.fractal.consensus.paxos;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.Messageable;
import net.sourceforge.fractal.utils.SerializationUtils;
/**   
* @author L. Camargos
* 
*/ 

public class PMessage extends Message {
  
	private static final long serialVersionUID = Messageable.FRACTAL_MID;

    protected static final byte BROADCAST = 0x01,
                                PHASE1_A  = 0x02,
                                PHASE1_B  = 0x03,
                                PHASE2_A  = 0x04,
                                PHASE2_B  = 0x05,
                                NACK      = 0x06,
                                DECISION  = 0x07,
                                LOGREQUEST= 0x08;

    public PMessage(){}
    
    //Common Paxos Messages' fields.
    byte type;
    String tag;
    int instance;
    Serializable serializable;
    
    
    public PMessage(byte type, String streamName, int instance, int swid) {
        this.type = type;
        this.tag = streamName;
        this.instance = instance;
        this.source = swid;
    }
    
//    static public String getMessageType(byte mtype){
//        switch(mtype){
//        case BROADCAST : return "BROADCAST";
//        case PHASE1_A  : return "PHASE1_A";
//        case PHASE1_B  : return "PHASE1_B";
//        case PHASE2_A  : return "PHASE2_A";
//        case PHASE2_B  : return "PHASE2_B";
//        case NACK      : return "NACK";
//        case DECISION  : return "DECISION";
//        case LOGREQUEST: return "LOGREQUEST";
//        default        : return "NOTYPE";
//        }
//    }
    
    public String toString(){
        return type + "(" + instance +")";
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        tag = in.readUTF();
        instance = in.readInt();
        type = in.readByte();
        serializable = (Serializable)in.readObject();
//        try{
//        	serializable = SerializationUtils.byteArrayToSerializable((byte[]) in.readObject());
//        }catch(Exception e){
//        	e.printStackTrace();
//        }	
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(tag);
        out.writeInt(instance);
        out.writeByte(type);
//        byte[] byteValue=null;
//        try{
//        	byteValue = SerializationUtils.serializableToByteArray(serializable);
//        } catch (IOException e) {
//        	e.printStackTrace();
//        }
//	    out.writeObject(byteValue);
        out.writeObject(serializable);
    }
};
