package net.sourceforge.fractal.failuredetection;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.CollectionUtils;

// FIXME remove this node heartbeat detection 
//       find a way to assign fd to groups.
public class HeartBeatDetector {
	
	private Set<Integer> heardOf;
	
	private int period;
	private Timer timer;
	private TimerTask etask, rtask;

	private BlockingQueue<Message> queue;
	private Group group;

	public HeartBeatDetector(Group g, int p){

		heardOf = new HashSet<Integer>(g.members());
		
		period = p;
		timer = new Timer();
		etask = new HeartBeatEmitterTask();
		rtask = new HeartBeatReceiverTask();

		group = g;
		queue = CollectionUtils.newBlockingQueue();

	}
	
	public boolean heardOf(int swid){
		return heardOf.contains(swid);
	}

	public void start(){
		group.registerQueue("HeartBeatMessage", queue);
		timer.scheduleAtFixedRate(etask, new Date(), period);
		timer.scheduleAtFixedRate(rtask, new Date(), period*2);
	}

	public void stop(){
		group.unregisterQueue("HeartBeatMessage", queue);
		timer.cancel();
	}


	public class HeartBeatReceiverTask extends TimerTask{

		public void run()  {
			heardOf.clear();
			HeartBeatMessage msg;
			while((msg=(HeartBeatMessage)queue.poll())!=null)
				heardOf.add(msg.source);
		}
		
	}

	public class HeartBeatEmitterTask extends TimerTask{

		public void run()  {
			group.broadcast(new HeartBeatMessage());
		}

	}


}
