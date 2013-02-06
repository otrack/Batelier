/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit it, or distribute it.
*/


package net.sourceforge.fractal;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**   
* @author L. Camargos
* 
*/


public class MCMessage extends Message{
    private static final long serialVersionUID = Messageable.FRACTAL_MID;
    public String s = "test message";
    
    public MCMessage(){}
    
    public MCMessage(String ss){
        s = ss;
    }
    
    public String toString(){
        return s;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(s);
        
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        s = (String) in.readObject();
    }
}
