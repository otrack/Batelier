/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/


package net.sourceforge.fractal;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**   
* @author L. Camargos
* 
*/ 


public class MessageInputStream extends ObjectInputStream{
    public MessageInputStream(InputStream stream) throws IOException {
        super(stream);
    }

    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
        ObjectStreamClass retval;
        Integer magic_num = readInt();

        if(magic_num == -1){
            retval = super.readClassDescriptor();
        }else{
        	Class c = MessageStream.idToMessage.get(magic_num);
            retval = ObjectStreamClass.lookup(c);
            if(retval == null)
                throw new ClassNotFoundException("not in the table " + magic_num);
        }            
        if(ConstantPool.MESSAGE_STREAM_DEBUG) System.out.println("MessageInputStream " + magic_num + " ==> " + retval.getName());
        return retval;
    }
}
