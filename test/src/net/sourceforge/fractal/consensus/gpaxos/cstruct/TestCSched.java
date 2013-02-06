package net.sourceforge.fractal.consensus.gpaxos.cstruct;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import net.sourceforge.fractal.replication.Command;
import net.sourceforge.fractal.replication.SimpleCommand;
import net.sourceforge.fractal.replication.SimpleCommutativeCommand;
/**   
* @author P. Sutra
* 
*/

public class TestCSched {

	Random r;

	public TestCSched(){
		r =  new Random(System.currentTimeMillis());
	}

	public static void main(String args[]){
		for(int i=0;i<100;i++)
			serializationTest();
//		TestCSched test = new TestCSched();
//		test.testGLB();
//		test.testLUB();
	}
	
	public static void serializationTest(){
		CSched h = new CSched();
		long start1 = System.currentTimeMillis();
		for(int i=0; i<1000;i++){
			h.append(new SimpleCommutativeCommand(i));
		}
		System.out.println(System.currentTimeMillis()-start1);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			long start = System.currentTimeMillis();
			oos.writeObject(h);
			System.out.println((System.currentTimeMillis()-start)+" => "+bos.size()+" / "+h.size());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void testGLB(){
		int nbCommands = 5000;
		CStruct s = new CSched();
		CStruct s2 = new CSched();
		List<Command> cseq = new ArrayList<Command>();
		
		for(int i=0;i<nbCommands;i++){
			cseq.add(new SimpleCommand(r.nextInt(nbCommands)));
		}
		s.appendAll(cseq);
		
		for(int i=0;i<nbCommands;i++){
			cseq.add(new SimpleCommand(r.nextInt(nbCommands)));
		}
		s2.appendAll(cseq);

		Set<CStruct> set = new HashSet<CStruct>();
		set.add(s);
		set.add(s2);

		try {
			long start = System.currentTimeMillis();
			CSched.glb(set);
			System.out.println("GLB("+nbCommands+")="+(System.currentTimeMillis() - start));
		} catch (EmptyCStructSetException e) {
			e.printStackTrace();
		}

	}

	public void testLUB(){
		int nbCommands = 1;
		CSched s = new CSched();
		ArrayList<Command> cseq = new ArrayList<Command>();

		for(int i=0;i<nbCommands;i++){
			cseq.add(new SimpleCommand(i));
		}
		s.appendAll(cseq);
		
		CSched s2 = new CSched();
		s2.appendAll(cseq);

		cseq.clear();
		for(int i=0;i<nbCommands;i++){
			cseq.add(new SimpleCommand(r.nextInt(nbCommands)));
		}
		s2.appendAll(cseq);

		List<CStruct> set = new ArrayList<CStruct>();
		set.add(s);
		set.add(s2);
		
		if(CSched.isCompatible(set)){
			try {
				long start = System.currentTimeMillis();
				CSched.lub(set);
				System.out.println("LUB("+nbCommands+")="+(System.currentTimeMillis() - start));
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}

	}

	public static void cloneTest(){
		CSched s = new CSched();
		for(int i=0; i<1000;i++){
			s.append(new SimpleCommutativeCommand(i));
		}
		long start1 = System.currentTimeMillis();
		s.clone();
		System.out.println(System.currentTimeMillis()-start1);
	}


}
