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

import net.sourceforge.fractal.utils.XMLUtils;

import org.w3c.dom.Node;


/**   
* @author L. Camargos
* 
*/



public class ConfigParserTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
        String configFile = "./src/test/basicConfig.xml";
        Node config = XMLUtils.getChildByName(XMLUtils.openFile(configFile), "FRACTAL");
        if(config != null){
            if(XMLUtils.getChildByName(config, "StaticBootstrapMembership") != null){
                System.out.println("StaticBootstrapMembership");
            }
        }
    }
}
