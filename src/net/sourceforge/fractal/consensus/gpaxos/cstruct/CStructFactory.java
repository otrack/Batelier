package net.sourceforge.fractal.consensus.gpaxos.cstruct;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**   
* @author P. Sutra
* 
*/ 


public class CStructFactory {

	private static Map<String,CStructFactory> instances;
	
	private String cstructName;
	private Class cstructClass;
	private Method glb, lub, isCompatible,leftLubOf;
	private Constructor<CStruct> newCStruct;
		
	@SuppressWarnings("unchecked")
	private CStructFactory(String name){
		cstructName = name;

		try {
			cstructClass = Class.forName(cstructName);
			Class[] parameters = {Collection.class};
			Class[] parameters2 = {cstructClass, cstructClass}; 
			newCStruct = (Constructor<CStruct>) cstructClass.getConstructor();
			glb =  Class.forName(cstructName).getMethod("glb",parameters);
			lub =  Class.forName(cstructName).getMethod("lub",parameters);
			isCompatible =  Class.forName(cstructName).getMethod("isCompatible",parameters);
			leftLubOf = Class.forName(cstructName).getMethod("leftLubOf",parameters2);
			
		}catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(this+" Invalid CStruct name "+cstructName);
		}
	}
	
	//
	// Class' interfaces
	//
		
	public static synchronized CStructFactory getInstance(String cstructName){
		if(instances==null)
			instances = new HashMap<String, CStructFactory>();
		if(instances.get(cstructName)==null)
			instances.put(cstructName, new CStructFactory(cstructName));
		return instances.get(cstructName);
	}
	
	//
	// Object's interfaces
	//
	
	public CStruct newCStruct() {
		try {
			return (CStruct) newCStruct.newInstance();
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(this+" Invalid cstruct name " + cstructName);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(this+" Invalid cstruct name " + cstructName);
		} catch (InstantiationException e) {
			throw new RuntimeException(this+" Invalid cstruct name " + cstructName);
		}
	}

	public CStruct lub(Collection<CStruct> collection){
		Object[] parameters = { collection };
		try {
			CStruct ret = (CStruct) lub.invoke(null, parameters);
			return ret;
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(this+" Invalid cstruct name " + cstructName);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(this+" Invalid cstruct name " + cstructName);
		}
	}
	
	public CStruct leftLubOf(CStruct u, CStruct v){
		Object[] parameters = { u, v};
		try {
			CStruct ret = (CStruct) leftLubOf.invoke(null, parameters);
			return ret;
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(this+" Invalid cstruct name " + cstructName);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(this+" Invalid cstruct name " + cstructName);
		}		
	}
	
	public CStruct glb(Collection<CStruct> collection){
		Object[] parameters = { collection };
		try {
			CStruct ret = (CStruct) glb.invoke(null, parameters);
			return ret;
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(this+" Invalid cstruct name " + cstructName);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(this+" Invalid cstruct name " + cstructName);
		}

	}

	public boolean isCompatible(Collection<CStruct> collection) {
		Object[] parameters = { collection };
		try {
			return (Boolean) isCompatible.invoke(null, parameters);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(this+" Invalid cstruct name " + cstructName);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(this+" Invalid cstruct name " + cstructName);
		}

	}

	public CStruct clone(CStruct u) {
		CStruct v = (CStruct) u.clone();
		return v;
	}
	
}
