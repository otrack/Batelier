package net.sourceforge.fractal.membership;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.utils.CollectionUtils;
/**   
* @author L. Camargos
* 
*/
class SocketListener extends Thread{
    private DatagramSocket socket;

    private List<BlockingQueue<Message>> queues = CollectionUtils.newList();
    private boolean terminate = false;
    
    public SocketListener(DatagramSocket s, BlockingQueue<Message> queue){
        super(s.getLocalSocketAddress().toString());
        if(null != queue)
            this.queues.add(queue);
        this.socket = s;
    }
    
    public void addQueue(BlockingQueue<Message> queue){
        queues.add(queue);
    }
    
    public void removeQueue(BlockingQueue<Message> queue){
        queues.remove(queue);
    }
    
    public boolean isEmpty(){
        return queues.isEmpty();
    }
    
    public void run() {
        Message message;
        ObjectInputStream obins;        
        byte rcvBuff[] = new byte[ConstantPool.NETWORK_MAX_DGRAM_SIZE]; //anything will fit here
        DatagramPacket rcvPack = new DatagramPacket(rcvBuff, rcvBuff.length);
        
        while(! terminate){
            try {
            		socket.receive(rcvPack);
            		if(ConstantPool.MEMBERSHIP_DL > 4) System.out.println("SocketListener : read from " + rcvPack.getSocketAddress());
                	obins = new ObjectInputStream(new ByteArrayInputStream(rcvPack.getData(),0,rcvPack.getLength()));
                	message =  (Message) obins.readObject();
                    for(BlockingQueue<Message> queue : queues){
                        queue.put(message);
                        if(ConstantPool.MEMBERSHIP_DL > 5) System.out.println("SocketListener : queue size is " + queue.size());
                    }
                    obins.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            
        }
    }
    
    public void stopThread() {
        terminate = true;
        this.interrupt();
        socket.close();
        try {
            this.join();
        } catch (InterruptedException e) {}
    }    
}