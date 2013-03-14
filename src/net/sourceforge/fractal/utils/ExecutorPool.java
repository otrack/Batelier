package net.sourceforge.fractal.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.sourceforge.fractal.utils.ObjectUtils.AbstractFactory;

/**
 * 
 * A facility that provides a set of executors for Callable objects.
 * 
 * @author Pierre Sutra
 * @author Masoud Saeida Ardekani
 * 
 */

public class ExecutorPool {

	private ExecutorService es;
	private static ExecutorPool instance;
	static{
		instance=new ExecutorPool();
	}

	private ExecutorPool() {
		es=Executors.newCachedThreadPool();
	}

	public <T> Future<T> submit(Callable<T> t) {
		return es.submit(t);
	}
	
	public <T extends Runnable> void submitMultiple(AbstractFactory<T> factory){
		for(int i=0; i<Runtime.getRuntime().availableProcessors(); i++)
			es.submit(factory.newInstance());
	}
	
	public <T extends Runnable> void submitMultiple(AbstractFactory<T> factory, int occ){
		for(int i=0; i<occ; i++)
			es.submit(factory.newInstance());
	}
	
	public void submit(Runnable t) {
		es.submit(t);
	}

	public void execute(Runnable  t){
		es.execute(t);
	}

	public static ExecutorPool getInstance() {
		return instance;
	}
	
	public ExecutorService getExecutorService(){
		return es;
	}	

}
