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


package net.sourceforge.fractal.consensus.paxos;

import java.io.Serializable;

/**   
* @author L. Camargos
* 
*/ 


public class LogEntry implements Comparable<LogEntry>, Serializable{
    private static final long serialVersionUID = 1L;
    public int inst;
    public int ts_promised;
    public Integer ts_accepted;
    public boolean decided;
    public Serializable accepted;
    
    public int compareTo(LogEntry arg0) {
        return this.inst < arg0.inst? -1 : this.inst == arg0.inst? 0 : 1;
    }
    
    public LogEntry(int inst, int ts_promised, Integer ts_accepted, boolean decided, Serializable accepted){
        this.inst = inst;
        this.ts_promised = ts_promised;
        this.ts_accepted = ts_accepted;
        this.decided = decided;
        this.accepted = accepted;
    }
    
    public String toString(){
        return "Inst=" + inst + (decided?" decided":" promised==" + ts_promised + " accepted==null?" + (accepted == null)) ;
    }
}




