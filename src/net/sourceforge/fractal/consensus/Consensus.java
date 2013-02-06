/*
Daisy, a Distributed Algorithms Library. Copyright (C) 2005, Sprint Project

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

package net.sourceforge.fractal.consensus;

import java.io.Serializable;


/**   
* @author L. Camargos
* 
*/ 


/**
 * This file defines a Consensus interface.
 */

public interface Consensus {
    
    /**
     * Propose a value in a consensus instance.
     * @param value the proposed value.
     * @param inst the consensus instance in which it is proposing.
     */
    public void propose(Serializable value, int inst);
    
    /**
     * Optimistic propose (for those algorithms that have this option).
     */
    public void optPropose(Serializable value, int inst);

    /**
     * Gets the decision of an specific intance.
     * This method blocks until a decision is reached.
     * @return the decision.
     * @throws InterruptedException
     */
    public Serializable decide(int inst) throws InterruptedException;
    public Serializable decide(int inst,long timeout) throws InterruptedException;
    
    /**
     * This method returns the decided value for this instance or null in the case a decision
     * has not been reached.
     * @return The decision.
     */
    public Serializable pollDecision(int inst);
}
