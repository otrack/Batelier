package net.sourceforge.fractal;

import java.io.*;
import java.util.*;

import static net.sourceforge.fractal.ValueVector.ComparisonResult.*;

/**
 * A generic value vector. It consist of a set of (key, value) pairs. Values are
 * {@link Comparable}. The value implicitly associated with a non-existing key
 * is specified in the constructor. This value should compare lower or equal to
 * any other value.
 * <p>
 * The vector is cloneable. The clone method makes a shallow copy of the (key,
 * value) pairs it contains. Values must therefore be <b>immutable</b>. The
 * vector is serializable if both keys and values are.
 * 
 * @author J-M. Busca INRIA/Regal
 * 
 * @param <K>
 *            the type of the keys of the vector.
 * @param <V>
 *            the type of the values of the vector.
 */

/**   
* @author P. Sutra
* 
*/ 


public class ValueVector<K, V extends Comparable<V>> implements Cloneable,
	Messageable {

	//
	// CONSTANTS
	//
	private static final long serialVersionUID = 1L;

	/**
	 * The return type of the {@link ValueVector#compareTo(ValueVector)} method.
	 * 
	 * @author J-M. Busca INRIA/Regal
	 * 
	 */
	public enum ComparisonResult {
		/**
		 * Vectors are not comparable.
		 */
		NOT_COMPARABLE,
		/**
		 * First vector is lower than second vector.
		 */
		LOWER_THAN,
		/**
		 * First and second vectors are equal.
		 */
		EQUAL_TO,
		/**
		 * First vector is greater than second vector.
		 */
		GREATER_THAN
	};

	//
	// OBJECT FIELDS
	//
	private HashMap<K, V> map;
	private V bydefault;

	//
	// CONSTRUCTORS
	//
	
	/**
	 * To be externalizable.
	 */
	public ValueVector(){
		
	}
	
	/**
	 * Creates a new empty value vector with the specified default value.
	 * 
	 * @param bydefault
	 *            the default associated to a non-existing key.
	 * 
	 */
	
	public ValueVector(V bydefault) {
		this.map = new HashMap<K, V>();
		this.bydefault = bydefault;
	}

	/**
	 * @inheritDoc
	 */
	public String toString() {
		return map.toString();
	}

	//
	// ACCESSORS
	//
	/**
	 * Sets the value of the specified key to the specified value.
	 * 
	 * @param key
	 *            the key whose value is being set.
	 * @param value
	 *            the new value of the key.
	 * 
	 */
	public void setValue(K key, V value) {
		map.put(key, value);
	}

	/**
	 * Returns the value of the specified key. If the key does not exist, the
	 * method returns the default value specified in the constructor.
	 * 
	 * @param value
	 *            the id of the log whose offset must be returned.
	 * 
	 * @return the value of the specified key if the key exists, or the default
	 *         value specified in the constructor otherwise.
	 */
	public V getValue(K value) {
		V found = map.get(value);
		if (found != null) {
			return found;
		}
		return bydefault;
	}

	//
	// UPDATE
	//
	/**
	 * Updates this vector with the specified vector. The method supersedes any
	 * entry of this vector that is lower than its counterpart in the specified
	 * vector.
	 * 
	 * @param vector
	 *            the vector to update this vector with.
	 */
	public synchronized void update(ValueVector<K, V> vector) {
		for (Map.Entry<K, V> entry : vector.map.entrySet()) {
			K key = entry.getKey();
			V value = entry.getValue();
			if (getValue(key).compareTo(value) < 0) {
				setValue(key, value);
			}
		}
	}

	//
	// COMPRESSION
	//
	/**
	 * Compresses the specified vector with respect to this vector and
	 * supersede this vector with the specified vector. The method returns a
	 * new vector containing entries of the specified vector that do not exist
	 * in this vector. It then replaces this vector with the specified vector.
	 * 
	 * @param vector
	 *            the vector to compress.
	 * 
	 * @return the compressed representation of the specified vector.
	 */
	@SuppressWarnings("unchecked")
	public ValueVector<K, V> compress(ValueVector<K, V> vector) {

		ValueVector<K, V> result = (ValueVector<K, V>) vector.clone();
		for (Map.Entry<K, V> entry : vector.map.entrySet()) {
			K key = entry.getKey();
			V value = entry.getValue();
			if (value.equals(getValue(key))) {
				result.map.remove(key);
			}
		}
		map = vector.map;
		return result;

	}

	/**
	 * Expands this vector with the specified compressed vector. The method adds
	 * to or replaces entries of this vector by those contained in the specified
	 * vector and leaves other entries unchanged.
	 * 
	 * @param compressed
	 *            the compressed vector to expand.
	 * @return a clone of this vector after it has been expanded.
	 */
	@SuppressWarnings("unchecked")
	public ValueVector<K, V> expand(ValueVector<K, V> compressed) {
		map.putAll(compressed.map);
		return (ValueVector<K, V>) clone();
	}

	//
	// OBJECT COMPARISON
	// 
	/**
	 * @inheritDoc
	 */
	@SuppressWarnings("unchecked")
	public boolean equals(Object object) {
		if (object == this) {
			return true;
		}
		if (!(object instanceof ValueVector)) {
			return false;
		}
		ValueVector<K, V> other = (ValueVector<K, V>) object;
		return other.map.equals(map);
	}

	/**
	 * @inheritDoc
	 */
	public int hashCode() {
		return map.hashCode();
	}

	/**
	 * Compares this vector to the specified vector. Note that this method
	 * differs from the {@link Comparable#compareTo(ValueVector)} method since
	 * it has to return the "not comparable" value.
	 * 
	 * @param other
	 *            the vector to compare this vector to.
	 * 
	 * @return {@link ComparisonResult#NOT_COMPARABLE NOT_COMPARABLE} if the
	 *         vectors cannot be compared, or one of the
	 *         {@link ComparisonResult#LOWER_THAN LOWER_THAN},
	 *         {@link ComparisonResult#EQUAL_TO EQUAL_TO},
	 *         {@link ComparisonResult#GREATER_THAN GREATER_THAN} values
	 *         otherwise.
	 */
	public ComparisonResult compareTo(ValueVector<K, V> other) {

		// check special values
		if (other == null) {
			return NOT_COMPARABLE;
		}
		if (map.isEmpty()) {
			if (other.map.isEmpty()) {
				return EQUAL_TO;
			}
			return LOWER_THAN;
		}

		if (other.map.isEmpty()) {
			return GREATER_THAN;
		}


		// compare values pairs one by one
		int global = 0;
		Set<K> remaining = new HashSet<K>(other.map.keySet());

		for (K key : map.keySet()) {
			int local = getValue(key).compareTo(other.getValue(key));
			if (global == 0 && local != 0) {
				global = local;
			} else if (global != 0 && global * local < 0) { 
				return NOT_COMPARABLE;
			}
			remaining.remove(key);
		}
		
		for (K key : remaining) {
			int local = getValue(key).compareTo(other.getValue(key));
			if (global == 0 && local != 0) {
				global = local;
			} else if (global != 0 && global * local < 0) { 
				return NOT_COMPARABLE;
			}
		}

		// vectors are comparable: return result
		if (global == 0) {
			return EQUAL_TO;
		}
		return global < 0 ? LOWER_THAN : GREATER_THAN;
		
	}
	

	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		map = (HashMap<K, V>) in.readObject();
		bydefault = (V) in.readObject();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(map);
		out.writeObject(bydefault);
	}

	//
	// CLONING
	//
	/**
	 * @inheritDoc
	 */
	@SuppressWarnings("unchecked")
	public Object clone() {
		try {
			ValueVector<K, V> result = (ValueVector<K, V>) super.clone();
			result.map = (HashMap<K, V>) map.clone();
			return result;
		} catch (CloneNotSupportedException e) {
			// should not happen
			return null;
		}
	}

	/**
	 * A version vector, i.e. a vector of Integer values.
	 * 
	 * @param <K>
	 *            the type of the keys of the vector.
	 */
	public static class VersionVector<K> extends ValueVector<K, Integer> {

		// 
		// Constants
		//
		private static final long serialVersionUID = 1L;

		// 
		// Object fields
		//
		private K local;

		// 
		// Constructors
		//
		
		/**
		 * To be externalizable.
		 */
		public VersionVector(){
		}

		
		/**
		 * Creates a version vector associated to the specified local key.
		 * 
		 * @param local
		 *            the key the vector is associated with.
		 * 
		 */
		public VersionVector(K local) {
			super(0);
			this.local = local;
		}

		/**
		 * @inheritDoc
		 */
		public Object clone() {
			return super.clone();
		}

		//
		// METHODS
		//
		/**
		 * Increments the value of the local key.
		 * 
		 * @return a clone of this version vector.
		 */
		@SuppressWarnings("unchecked")
		public synchronized VersionVector<K> incr() {
			setValue(local, getValue(local) + 1);
			return (VersionVector<K>) clone();
		}

		/**
		 * Compresses the specified version vector. See
		 * {@link ValueVector#compress(ValueVector)}.
		 * 
		 * @param vector
		 *            the vector to compress.
		 * @return the compressed representation of the specified vector.
		 */
		public VersionVector<K> compress(VersionVector<K> vector) {
			return (VersionVector<K>) super.compress(vector);
		}

		/**
		 * Expands the specified version vector. See
		 * {@link ValueVector#expand(ValueVector)}.
		 * 
		 * @param vector
		 *            the vector to compress.
		 * @return the compressed representation of the specified vector.
		 */
		public VersionVector<K> expand(VersionVector<K> vector) {
			return (VersionVector<K>) super.expand(vector);
		}

	}
	
}

