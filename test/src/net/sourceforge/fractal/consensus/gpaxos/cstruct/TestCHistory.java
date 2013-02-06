package net.sourceforge.fractal.consensus.gpaxos.cstruct;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import net.sourceforge.fractal.MessageInputStream;
import net.sourceforge.fractal.MessageOutputStream;
import net.sourceforge.fractal.consensus.gpaxos.GPMessage2BStart;
import net.sourceforge.fractal.replication.Command;
import net.sourceforge.fractal.replication.ReadWriteRegisterCommand;
import net.sourceforge.fractal.replication.SimpleCommutativeCommand;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;
/**   
* @author P. Sutra
* 
*/
public class TestCHistory {

	private static TimeRecorder llubTime = new TimeRecorder("TestCHistory:llubTime");
	private static TimeRecorder glbTime = new TimeRecorder("TestCHistory:glbTime");
	private static TimeRecorder cloneTime = new TimeRecorder("TestCHistory:cloneTime");
	private static TimeRecorder appendTime = new TimeRecorder("TestCHistory:appendTime");
	private static TimeRecorder serializationTime = new TimeRecorder("TestCHistory:serializationTime");
	private static TimeRecorder deserializationTime = new TimeRecorder("TestCHistory:deserializationTime");
	private static ValueRecorder serializationSize = new ValueRecorder("TestCHistory:serializationSize");
	
	
	private static Random rand = new Random(0);
	
	public static void main(String[] args){
		
//		testCorrectness();
//		leftLubTest();
		
		for(int i=0;i<200;i++){
//			cloneTest();
			serializationTest();
//			glb();
		}

		
	}
	
	public static void testCorrectness(){
		CHistory h1 = new CHistory();
		CHistory h2 = new CHistory();
		CHistory h3 = new CHistory();
		
		SimpleCommutativeCommand c0 = new SimpleCommutativeCommand(0);
		SimpleCommutativeCommand c1 = new SimpleCommutativeCommand(0);
		SimpleCommutativeCommand c2 = new SimpleCommutativeCommand(1);
		SimpleCommutativeCommand c3 = new SimpleCommutativeCommand(2);
		
		h1.append(c0);
		h1.append(c1);
		h1.append(c2);
		
		h2.append(c2);
		h2.append(c3);
		
		h2.append(c3);
		h2.append(c1);
		
		Set<CHistory> chset = new HashSet<CHistory>();
		chset.add(h1);
		chset.add(h2);
		chset.add(h3);
		assert !CHistory.isCompatible(chset) : chset;
	}

	
	public static void serializationTest(){
		CHistory h = new CHistory();

		List<Command> l = new ArrayList<Command>(); 
		for(int i=0; i<1000;i++){
			l.add(new ReadWriteRegisterCommand(rand.nextInt(3000),rand.nextInt(3),rand.nextBoolean()));
		}
		
		appendTime.start();
		h.appendAll(l);
		appendTime.stop();
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {

			MessageOutputStream oos = new MessageOutputStream(bos);
			
			serializationTime.start();
			oos.writeObject(new GPMessage2BStart(0,1,h));
			byte [] data = bos.toByteArray();
			serializationTime.stop();
			ByteBuffer bb = ByteBuffer.allocate(4+4+data.length);
			bb.putInt(data.length);
			bb.put(data);
			bb.flip();
			
			oos.flush();
			
			serializationSize.add(data.length/1000);
			ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
			MessageInputStream ois = new MessageInputStream(bis);
			deserializationTime.start();
			ois.readObject();
			deserializationTime.stop();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public static void cloneTest(){
		CHistory h = new CHistory();
		for(int i=0; i<5000;i++){
			h.append(new SimpleCommutativeCommand(0));
		}
		cloneTime.start();
		h.clone();
		cloneTime.stop();
	}
	
	public static void leftLubTest(){

		CHistory h = new CHistory();
		CHistory h1 = new CHistory();
		
		Command a = new ReadWriteRegisterCommand(2000,1,true); // 2000:1:R:1
		Command b = new ReadWriteRegisterCommand(2001,1,false); // 2001:1:W:1
		Command c = new ReadWriteRegisterCommand(1000,1,true); // 1000:1:R:1
		Command d = new ReadWriteRegisterCommand(1001,2,true); // 1001:1:R:2
		
		h.append(a);
		h.append(b);
		System.out.println(h);
		
		h1.append(a);
		h1.append(c);
		h1.append(b);
		h1.append(d);
		System.out.println(h1);
		
		CStruct h2 = CHistory.leftLubOf(h, h1);
		System.out.println(h2);
		
	}
	
	public static void glb(){
		
		CStructFactory factory = CStructFactory.getInstance("net.sourceforge.fractal.consensus.gpaxos.cstruct.CHistory");

		CHistory h = new CHistory();
		
		for(int i=0; i<5000;i++){
			h.append(new ReadWriteRegisterCommand(rand.nextInt(4),rand.nextInt(32000),rand.nextBoolean()));
		}
		
		CHistory h1 = new CHistory();
		for(int i=0; i<5000;i++){
			h1.append(new ReadWriteRegisterCommand(rand.nextInt(4),rand.nextInt(32000),rand.nextBoolean()));
		}
		
		CHistory h2 = new CHistory();
		for(int i=0; i<5000;i++){
			h2.append(new ReadWriteRegisterCommand(rand.nextInt(4),rand.nextInt(32000),rand.nextBoolean()));
		}
		
		Set<CStruct> s = new HashSet<CStruct>();
		s.add(h);
		s.add(h1);
		s.add(h2);
		

		glbTime.start();
		CHistory glb = (CHistory) factory.glb(s);
		h1.removeAll(glb);
		h2.removeAll(glb);
		h.removeAll(glb);
		glbTime.stop();
	}
	
}
