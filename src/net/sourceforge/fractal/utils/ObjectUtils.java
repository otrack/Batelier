package net.sourceforge.fractal.utils;

/*
 * Telex - A communication infrastructure for collaborative nomadic applications
 * Copyright (c) 2007, INRIA Rocquencourt - Regal Team. All rights reserved.
 * Licence to Telex is in file <project root>/LICENCE.txt.
 */

import java.io.*;
import java.lang.reflect.*;
import java.security.*;
import java.util.*;

import net.sourceforge.fractal.utils.CollectionUtils.Predicate;

/**
 * A utility class for copying, storing and loading objects.
 * 
 * @author J-M. Busca INRIA/Regal
 * 
 */
public class ObjectUtils {

	//
	// CONSTANTS
	//
	/**
	 * Value returned by some methods of this class to indicate an error.
	 */
	public static final Object ERROR = new Object();

	//
	// CLASS FIELDS
	//
	private static MessageDigest digest;

	//
	// STATIC INITIALIZATION
	//
	static {
		try {
			digest = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			System.err.println("cannot instantiate message digest: " + e);
			System.exit(1);
		}
	}

	//
	// CONVERTION AND SIZE
	//
	public static byte[] toBytes(Serializable serializable) throws IOException {
		ByteArrayOutputStream array = new ByteArrayOutputStream();
		ObjectOutputStream stream = new ObjectOutputStream(array);
		stream.writeObject(serializable);
		stream.close();
		return array.toByteArray();
	}

	public static byte[] toBytes(Serializable serializable, boolean noException) {
		try {
			return toBytes(serializable);
		} catch (IOException e) {
			return null;
		}
	}

	public static Serializable toObject(byte[] bytes) throws IOException,
		ClassNotFoundException {
		ByteArrayInputStream array = new ByteArrayInputStream(bytes);
		ObjectInputStream input = new ObjectInputStream(array);
		Serializable object = (Serializable) input.readObject();
		input.close();
		return object;
	}

	public static Serializable toObject(byte[] bytes, boolean noException) {
		try {
			return toObject(bytes);
		} catch (IOException e) {
			return null;
		} catch (ClassNotFoundException e) {
			return null;
		}
	}

	public static int size(Serializable serializable) throws IOException {
		return toBytes(serializable).length;
	}

	public static int size(Serializable serializable, boolean noException) {
		try {
			return toBytes(serializable).length;
		} catch (IOException e) {
			return -1;
		}
	}

	//
	// OBJECT COPY
	//
	@SuppressWarnings("unchecked")
	public static <T extends Serializable> T getCopy(T serializable)
		throws IOException, ClassNotFoundException {
		byte[] bytes = toBytes(serializable);
		return (T) toObject(bytes);
	}

	public static <T extends Serializable> T getCopy(T serializable,
		boolean noException) {
		try {
			return getCopy(serializable);
		} catch (IOException e) {
			return null;
		} catch (ClassNotFoundException e) {
			return null;
		}
	}

	//
	// REFLEXION METHODS
	//
	/**
	 * Instantiates the class with the specified name. The method assumes that
	 * the class defines a nullary constructor.
	 * 
	 * @param name
	 *            the name of the class to instantiate.
	 * @return a new instance of the class or {@code null} if the class cannot
	 *         be instantiated.
	 * 
	 * @param <T>
	 *            the type of the object to handle.
	 */
	@SuppressWarnings("unchecked")
	public static <T> T newInstance(String name) {
		if (name == null) {
			throw new NullPointerException("name");
		}
		try {
			Class<T> clazz = (Class<T>) Class.forName(name);
			return newInstance(clazz);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Instantiates the specified class. The method assumes that the class
	 * defines a nullary constructor.
	 * 
	 * @param clazz
	 *            the class to instantiate.
	 * @return a new instance of the class or {@code null} if the class cannot
	 *         be instantiated.
	 * 
	 * @param <T>
	 *            the type of the object to handle.
	 */
	public static <T> T newInstance(Class<T> clazz) {
		if (clazz == null) {
			throw new NullPointerException("clazz");
		}
		try {
			return clazz.newInstance();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			return null;
		} catch (InstantiationException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Instantiates the class with the specified name by invoking its
	 * constructor with the specified signature with the specified values.
	 * 
	 * @param name
	 *            the fully qualified name of the class.
	 * @param signature
	 *            the array of parameter types of the constructor.
	 * @param parameters
	 *            the array of parameters to call the constructor with.
	 * @return a new object of the specified class or {@code null} if an error
	 *         occurs.
	 * 
	 * @param <T>
	 *            the type of the object to return.
	 */
	@SuppressWarnings("unchecked")
	public static <T> T newInstance(String name, Class<?>[] signature,
		Object[] parameters) {
		if (name == null) {
			throw new NullPointerException("name");
		}
		try {
			Class<T> clazz = (Class<T>) Class.forName(name);
			return newInstance(clazz, signature, parameters);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Instantiates the specified class by invoking its constructor with the
	 * specified signature with the specified values.
	 * 
	 * @param clazz
	 *            the class to instantiate.
	 * @param signature
	 *            the array of parameter types of the constructor.
	 * @param parameters
	 *            the array of parameters to call the constructor with.
	 * @return a new object of the specified class or {@code null} if an error
	 *         occurs.
	 * 
	 * @param <T>
	 *            the type of the object to return.
	 */
	public static <T> T newInstance(Class<T> clazz, Class<?>[] signature,
		Object[] parameters) {
		if (clazz == null) {
			throw new NullPointerException("clazz");
		}
		if (signature == null) {
			throw new NullPointerException("signature");
		}
		if (parameters == null) {
			throw new NullPointerException("parameters");
		}
		try {
			Constructor<T> constructor = clazz.getConstructor(signature);
			return constructor.newInstance(parameters);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			return null;
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			return null;
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			return null;
		} catch (InstantiationException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Returns the constructor of the class with the specified name with the
	 * specified signature.
	 * 
	 * @param name
	 *            the name of the class to return the constructor of.
	 * @param signature
	 *            the array of parameter types of the constructor.
	 * @return the constructor of the specified class with the specified
	 *         signature or {@code null} if such constructor does not exist.
	 * 
	 * @param <T>
	 *            the type of the class.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Constructor<T> getConstructor(String name,
		Class<?>[] signature) {
		if (name == null) {
			throw new NullPointerException("name");
		}
		try {
			Class<T> clazz = (Class<T>) Class.forName(name);
			return getConstructor(clazz, signature);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Returns the constructor of the specified class with the specified
	 * signature.
	 * 
	 * @param clazz
	 *            the class to return the constructor of.
	 * @param signature
	 *            the array of parameter types of the constructor.
	 * @return the constructor of the specified class with the specified
	 *         signature or {@code null} if such constructor does not exist.
	 * 
	 * @param <T>
	 *            the type of the class.
	 */
	public static <T> Constructor<T> getConstructor(Class<T> clazz,
		Class<?>[] signature) {
		if (clazz == null) {
			throw new NullPointerException("clazz");
		}
		try {
			return clazz.getConstructor(signature);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Invokes the no-argument method with the specified name on the specified
	 * object.
	 * 
	 * @param object
	 *            the object to invoke the method upon.
	 * @param name
	 *            the name of the no-argument method to invoke.
	 * @return the return value of the method, or {@code null} if the return
	 *         type of the method is {@code void} or {@link #ERROR} if an error
	 *         occurred.
	 */
	public static Object invoke(Object object, String name) {
		if (object == null) {
			throw new NullPointerException("target");
		}
		if (name == null) {
			throw new NullPointerException("method");
		}
		try {
			Class<? extends Object> clazz = object.getClass();
			Method method = clazz.getDeclaredMethod(name, new Class[0]);
			return method.invoke(object);
		} catch (Exception e) {
			return ERROR;
		}
	}

	/**
	 * Clones the specified object. This method is sometimes required since the
	 * {@link Cloneable} interface does not define any (public clone()) method
	 * and the {@link Object#clone()} method is declared protected.
	 */
	static <T> T clone(T object) {
		try {
			Class<? extends Object> clazz = object.getClass();
			Method method = clazz.getDeclaredMethod("clone", new Class[0]);
			@SuppressWarnings("unchecked")
			T copy = (T) method.invoke(object);
			return copy;
		} catch (Exception e) {
			return null;
		}
	}

	public static interface AbstractFactory<T>{
		public abstract T newInstance();
	}
	
	public static class InnerObjectFactory<T> implements AbstractFactory<T>{
		
		Constructor<T> constructor;
		Object outer;
					
		public InnerObjectFactory(Class<T> innerClass, Class outerClass, Object o){
			try {
				outer = o;
				constructor = innerClass.getConstructor(outerClass);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		public T newInstance(){
			try {
				return constructor.newInstance(outer);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
		
	}
	
	//
	// SIMPLE STORE/LOAD METHODS
	//
	/**
	 * Stores the specified object in a file with the specified pathname.
	 * 
	 * @param serializable
	 *            the object to store.
	 * @param pathname
	 *            the pathname of the file in which to store the object.
	 * 
	 * @throws IOException
	 *             if an I/O error occurs.
	 */
	public static void storeObject(Serializable serializable, String pathname)
		throws IOException {
		ObjectOutputStream output =
			new ObjectOutputStream(new FileOutputStream(pathname));
		output.writeObject(serializable);
		output.close();
	}

	/**
	 * Stores the specified object in a file with the specified pathname.
	 * 
	 * @param serializable
	 *            the object to store.
	 * @param pathname
	 *            the pathname of the file in which to store the object.
	 * @param noException
	 *            any value.
	 * @return {@code true} if the object was stored correctly, and {@code
	 *         false} otherwise.
	 */
	public static boolean storeObject(Serializable serializable,
		String pathname, boolean noException) {
		try {
			storeObject(serializable, pathname);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Loads the object from the file with the specified pathname.
	 * 
	 * @param pathname
	 *            the pathanme of the file to load the object from.
	 * @return the loaded object.
	 * 
	 * @throws FileNotFoundException
	 *             if the file does not exist
	 * @throws ClassNotFoundException
	 *             if the object class can not be found.
	 * @throws IOException
	 *             if an I/O error occurs.
	 */
	public static Object loadObject(String pathname)
		throws FileNotFoundException, ClassNotFoundException, IOException {
		ObjectInputStream input =
			new ObjectInputStream(new FileInputStream(pathname));
		Object object = input.readObject();
		input.close();
		return object;
	}

	/**
	 * Loads the object from the file with the specified pathname.
	 * 
	 * @param pathname
	 *            the pathanme of the file to load the object from.
	 * @param noException
	 *            any value.
	 * @return the loaded object or {@code null} if the object cannot be loaded.
	 */
	public static Object loadObject(String pathname, boolean noException) {
		try {
			return loadObject(pathname);
		} catch (Exception e) {
			return null;
		}
	}
	
}

