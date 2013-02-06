/*
Fractal is a light-weight group communication system. Copyright (C) 2005, Sprint Project.      This library is free software; you can redistribute it and/or modify it under     the terms of the GNU Lesser General Public License as published by the Free Software 	Foundation; either version 2.1 of the License, or (at your option) any later version.      This library is distributed in the hope that it will be useful, but WITHOUT      ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A      PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.      You should have received a copy of the GNU Lesser General Public License along      with this library; if not, write to the      Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA 
file attached to it to see how you can use it, edit or distributed.
*/


package net.sourceforge.fractal.commit;

import net.sourceforge.fractal.utils.CloseableLinkedBlockingQueue;

/**   
* @author L. Camargos
* 
*/ 


public interface CommitStream {
    public void setRM(ResourceManager rm);
    public void setMessageType(CommitValue cvm);
    public void registerLearner(CommitLearner learner);    
    public void unregisterLeaner(CommitLearner learner);
 
    /**
     * Called by TransactionManagers.
     * 
     * start the termination procedure of transaction Tid.
     * 
     * @param v CommitValue containing all the information about the
     * transaction to be terminated.
     * @param retryTimeout Keep retrying with this timeout untill the transaction is finished.
     * @throws InterruptedException Thrown if the system could not start termination procedure.
     */
    public void terminate(CommitValue v, long retryTimeout) throws InterruptedException;
    public void commit(CommitValue v, long timeoutOtherRMs) throws InterruptedException;
    
    public  CloseableLinkedBlockingQueue<CommitValue> getLogs2(int recRm, int firstInstance, int lastInstance);
    public  CommitValue[] getLogs(int recRm, int firstInstance, int lastInstance);

}
