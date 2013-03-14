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
*/


package net.sourceforge.fractal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;


/**   
* @author L. Camargos
* @author P. Sutra
* 
*/ 


public class Message implements Messageable, Cloneable{
	
    private static final long serialVersionUID = Messageable.FRACTAL_MID;
    private static ValueRecorder packProcessingTime;
    private static ValueRecorder unpackProcessingTime;
    static{
		if(ConstantPool.MEMBERSHIP_DL>2){
			packProcessingTime = new ValueRecorder("Message#packProcessingTime(us)");
			packProcessingTime.setFactor(1000);
			packProcessingTime.setFormat("%a");
			unpackProcessingTime = new ValueRecorder("Message#unpackProcessingTime(us)");
			unpackProcessingTime.setFactor(1000);
			unpackProcessingTime.setFormat("%a");
		}
    }

    public int source;
       
    public Message(){}
       
    public Message(Message m){
    	this.source = m.source;
    }
    
    public String getMessageType(){
    	return this.getClass().getSimpleName();
    }
        
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        source = in.readInt();
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(source);
    }
        
    @Override
    public Object clone() throws CloneNotSupportedException{
    	Message m = (Message) super.clone();
    	m.source = this.source;
    	return m;
    }
    
    
    //
    // CLASS INTERFACE
    //
    
	public static ByteBuffer pack(Message m, int id) {

		long start;
		if(ConstantPool.MEMBERSHIP_DL>2)	
			 start = System.nanoTime();
		
		// 1 - Assign source
		m.source=id;
		
		// 2 - Serialize data
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		MessageOutputStream mos;
		try {
			mos = new MessageOutputStream(baos);
			mos.writeObject(m);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		byte [] data = baos.toByteArray(); 

		// 3 - Pack into a ByteBuffer
		ByteBuffer bb = ByteBuffer.allocate(data.length);
		bb.put(data);
		bb.flip();
		
		if(ConstantPool.MEMBERSHIP_DL>2)
			packProcessingTime.add(System.nanoTime()-start);
		
		return bb;
	}
	
	public static Message unpack(ByteBuffer buff) throws IOException, ClassNotFoundException{
		
		long start ;
		if(ConstantPool.MEMBERSHIP_DL>2)
			start= System.nanoTime();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(buff.array());
		MessageInputStream mis = new MessageInputStream(bais);
		Message m = (Message)mis.readObject();
		
		if(ConstantPool.MEMBERSHIP_DL>2)
			unpackProcessingTime.add(System.nanoTime()-start);
		
		return m;
	}
    	
}
