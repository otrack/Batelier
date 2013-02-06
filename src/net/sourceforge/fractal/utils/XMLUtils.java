/*
    Fractal. Copyright (C) 2005, Sprint Project

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

package net.sourceforge.fractal.utils;

/**   
 * @author L. Camargos
* 
*/


import java.io.File;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;


import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;





public class XMLUtils {
    public static Node openFile(String fileName){
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(false);
        factory.setNamespaceAware(false);
        DocumentBuilder builder;
        Document document = null;
        try {
            builder = factory.newDocumentBuilder();
            document = builder.parse(new File(fileName));
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return document;
    }
    
    public static Map<String, Node> getChildMap(Node node){
        NodeList nl = node.getChildNodes();
        Map<String, Node> result = CollectionUtils.newMap();
        for(int i = 0; i < nl.getLength(); i++) {
            Node no =  nl.item(i);
            result.put(no.getNodeName(),no);
        }
        return result;
    }

    public static Set<Node> getChildrenByName(Node node, String name){
        NodeList nl = node.getChildNodes();
        Set<Node> result = CollectionUtils.newSet();
        for(int i = 0; i < nl.getLength(); i++) {
            if(nl.item(i).getNodeName().equals(name))
                result.add(nl.item(i));
        }
        return result;
    }

    public static Node getChildByName(Node node, String name){
        NodeList nl = node.getChildNodes();
        for(int i = 0; i < nl.getLength(); i++) {
            if(nl.item(i).getNodeName().equals(name))
                return nl.item(i);
        }
        return null;
    }

    public static Map<String,String> getAttribMap(Node node){
        NamedNodeMap nnm = node.getAttributes();
        Map<String,String> result = CollectionUtils.newMap();
        for(int x = 0; x < nnm.getLength(); x++){
            Node n = nnm.item(x);
            result.put(n.getNodeName(), n.getNodeValue());
        }
        return result;
    }

    public static String getAttribByName(Element node, String name){
        return node.getAttribute(name);
    }
    
    public static boolean hasAttribByName(Element node, String name){
    	return node.hasAttribute(name);
    }
}
