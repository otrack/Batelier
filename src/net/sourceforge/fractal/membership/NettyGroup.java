package net.sourceforge.fractal.membership;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.RuntimeErrorException;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.ExecutorPool;

import org.jboss.netty.bootstrap.Bootstrap;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ServerChannel;
import org.jboss.netty.channel.ServerChannelFactory;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;


/**
 * 
 * Dynamic group object.
 * 
 * This class is based on the JBoss Netty framework.
 * 
 * 
 * @author P. Sutra
 *
 */
public class NettyGroup extends Group {

	private Map<Integer, Channel> swid2Channel = CollectionUtils.newMap();

	private ServerChannel server;
	
	private ChannelGroup group = new DefaultChannelGroup();
	
	private static ChannelFactory cfactory = new NioClientSocketChannelFactory(ExecutorPool.getInstance().getExecutorService(),
			ExecutorPool.getInstance().getExecutorService(),Runtime.getRuntime().availableProcessors());
	
	private static ServerChannelFactory sfactory = new NioServerSocketChannelFactory(ExecutorPool.getInstance().getExecutorService(),
				ExecutorPool.getInstance().getExecutorService(),Runtime.getRuntime().availableProcessors());
	
	private ChannelPipelineFactory pipelineFactory = new ChannelPipelineFactory() {
		public ChannelPipeline getPipeline() throws Exception {
			ChannelPipeline pipeline = Channels.pipeline();
			pipeline.addLast("decoder", new ObjectDecoder());
			pipeline.addLast("encoder", new ObjectEncoder());
			pipeline.addLast("handler", new FractalChannelHander());
			return pipeline;
		}
	};	
	
	private boolean isTerminated;
	private boolean isDynamic;
	
	public NettyGroup(Membership m, String n, int p) {
		super(m, n, p);
		isDynamic = false;
		isTerminated = true;
	}
	
	public NettyGroup(Membership m, String n, int p, boolean dyn) {
		super(m, n, p);
		isDynamic = dyn;
		isTerminated = true;
	}
	
	//
	//	Interface		
	//
	
	@Override
	public synchronized boolean joinGroup() {

		assert contains(membership.myId());

		if (ConstantPool.MEMBERSHIP_DL > 6)
			System.out.print(this + " now ");

		ServerBootstrap bootstrap = new ServerBootstrap(sfactory);
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);
		bootstrap.setPipelineFactory(pipelineFactory);
		server = (ServerChannel)bootstrap.bind(new InetSocketAddress(membership.myIP(), port));

		if (ConstantPool.MEMBERSHIP_DL > 6) 
			System.out.println(" ... serving on "+port);

		return true;

	}

	@Override
	public synchronized boolean leaveGroup() {

		assert contains(membership.myId());
		
		if (ConstantPool.MEMBERSHIP_DL > 6)
			System.out.print(this + " now ");

		server.close();

		if (ConstantPool.MEMBERSHIP_DL > 6) 
			System.out.println(" ... closing "+port);

		return true;
	}
	
	@Override
	public synchronized void closeConnections() {

		if (ConstantPool.MEMBERSHIP_DL > 6)
			System.out.println(this + " closing connections ");
		
		for(int swid : this.allNodes())
			unregisterChannel(swid);
	}
	
	@Override
	public synchronized void stop(){

		if (ConstantPool.MEMBERSHIP_DL > 6)
			System.out.println(this + " stopping group ");
		
		if(!isTerminated){
			if(contains(membership.myId())) leaveGroup();
			closeConnections();
			isTerminated=true;
		}
		
	}

	@Override
	public synchronized void start(){
		
		if (ConstantPool.MEMBERSHIP_DL > 6)
			System.out.println(this + " starting");
		
		if(isTerminated){
			if(contains(membership.myId())) joinGroup();
			isTerminated=false;
		}
		
	}
	
	@Override
	public void broadcastTo(ByteBuffer bb, Set<Integer> swids) {
		throw new RuntimeErrorException(null, "NYI");
	}

	@Override
	public void broadcast(ByteBuffer bb){
		throw new RuntimeErrorException(null, "NYI");
	}
	
	@Override
	public void broadcast(Message msg){
		performBroadcast(msg);
	}

	public void broadcastToOthers(Message msg){
		Set<Integer> swids = new HashSet<Integer>(swid2ip.keySet());
		swids.remove(membership.myId());
		broadcastTo(msg, swids);
	}
	
	@Override
	public void broadcastTo(Message msg, Set<Integer> swids) {
		for(int swid : swids){
			performUnicast(swid, msg);
		}
	}
		
	@Override
	public void unicast(int swid, Message msg) {
		performUnicast(swid,msg);
	}
	
	public String toString() {
		return "TCPGroup:"+name+ ((ConstantPool.MEMBERSHIP_DL > 3) ? "@"+membership.myId() : "");
	}
	
	//
	// Channel Management
	// 

	private Channel getChannel(int swid) {
		
		if (ConstantPool.MEMBERSHIP_DL > 6) 
			System.out.println(this+" get channel for "+swid);

		if ( swid2Channel.containsKey(swid) )
			return swid2Channel.get(swid);		
		
		return null;
		
	}
	
	private boolean unregisterChannel(int swid) {

		if (ConstantPool.MEMBERSHIP_DL > 6) 
			System.out.println(this+" remove channel for "+swid);
		
		if( !swid2Channel.containsKey(swid) )
			return false;
		
		synchronized(this){

			if ( !swid2Channel.containsKey(swid) )
				return false;
			
			Channel c = swid2Channel.get(swid);
			c.close();
			group.remove(c);
			swid2Channel.remove(swid);

		}
		
		return true;
	}
	
	private boolean registerChannel(int swid, Channel c){
		
		if (ConstantPool.MEMBERSHIP_DL > 6) 
			System.out.println(this+" register channel for "+swid);
		
		synchronized(this){

			if( swid2Channel.containsKey(swid) ){
				if (ConstantPool.MEMBERSHIP_DL > 1) 
					System.out.println(this+" a channel already exists.");
				return false;
			}

			swid2Channel.put(swid,c);
			group.add(c);
		}
		
		return true;
		
	}
	
	private Channel createChannel(int swid, boolean isServer) {

		assert (membership.myId() != swid || isServer) && (!isServer || membership.myId() == swid);
		
		if (ConstantPool.MEMBERSHIP_DL > 6) 
			System.out.print(this+" now ");

		Bootstrap bootstrap = new ClientBootstrap(cfactory);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);
		bootstrap.setPipelineFactory(pipelineFactory);
		Channel channel = ((ClientBootstrap) bootstrap).connect(
					new InetSocketAddress(membership.adressOf(swid), port)).awaitUninterruptibly().getChannel();

		if (ConstantPool.MEMBERSHIP_DL > 6) 
				System.out.println(" ... connected to "+swid+" on "+port);
		
		return channel;
		
	}

	//
	// Broadcast Management
	//

	private void performUnicast(int swid, Message m) {

		if (ConstantPool.MEMBERSHIP_DL > 6)
			System.out.println(this + " unicasting to "+swid+" by "+Thread.currentThread().getName());

		if(isTerminated){
			throw new IllegalArgumentException(this + " service down.");
		}
		
		if( !membership.contains(swid) ){
			throw new IllegalArgumentException(this+" node "+swid+" does not exist");
		}
		
		// FIXME
		if( !contains(swid) ){
			if(!isDynamic){
				throw new IllegalArgumentException( this + " node "+ swid+" is not part of this group.");
			}else{
				putNode(swid, membership.adressOf(swid));
			}
		}
		
		// FIXME
		m.source = membership.myId();
		Channel c = getChannel(swid);
		if(c==null){
			c = createChannel(swid, swid==membership.myId());
			registerChannel(swid, c);
		}

		try{			
			c.write(m);
		}catch (Exception e) {

			if (ConstantPool.MEMBERSHIP_DL > 1)
				System.out.println(this + " connection to "+swid+" is down; resaon: "+e.getMessage());

		}

	}
	
	private void performBroadcast(Message m){

		if (ConstantPool.MEMBERSHIP_DL > 6)
			System.out.println( this + " broadcasting "+m);		

		if(isTerminated){
			if (ConstantPool.MEMBERSHIP_DL > 0)
				System.out.println(this + " service down ");
			return;
		}

		try{
				
			// FIXME
			for(int swid : allNodes()){
				if(!swid2Channel.containsKey(swid)){
					Channel c = createChannel(swid, swid==membership.myId());
					registerChannel(swid, c);
				}
			}
			
			m.source = membership.myId();
			group.write(m);
			
		}catch (Exception e) {

			if (ConstantPool.MEMBERSHIP_DL > 1)
				System.out.println(this + " group connection down; resaon: "+e.getMessage());

		}

		
	}
	

	
	class FractalChannelHander extends SimpleChannelHandler{
			
		public FractalChannelHander() {}
		
		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			try {
				Message m = (Message)e.getMessage();
				if( isDynamic && ! contains(m.source) ){
					Channel c = ctx.getChannel();
					putNode(m.source, ((InetSocketAddress)c.getRemoteAddress()).getAddress().getHostAddress());
					registerChannel(m.source, c);
				}
				deliver(m);
			} catch (Exception e1) {
				e1.printStackTrace();
				System.out.println(ctx.getChannel().getRemoteAddress());
			}			
		}
		
		@Override
	    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
			// FIXME
		}
		
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
			if (ConstantPool.MEMBERSHIP_DL > 1)
				System.out.println( this + " channel closed unexpectly; reason: "+e.getCause());					
		}
		
	}
	
}
