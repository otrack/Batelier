package net.sourceforge.fractal.utils;


import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class CollectionUtils {
	
	private static Random rand = new Random(System.currentTimeMillis());
	
    public static <K,V> Map<K,V> newMap() {
        return new HashMap<K,V>();
    }
    
    public static <K,V> Map<K,V> newSortedMap() {
        return new TreeMap<K,V>();
    }

    public static <K,V> Map<K,V> newCacheMap(int initialCapacity, float loadFactor, boolean accessOrder) {
        return new LinkedHashMap<K,V>(initialCapacity,loadFactor, accessOrder);
    }

    public static <K,V> Map<K,V> newMap(int initCap) {
        return new HashMap<K,V>(initCap);
    }

    public static <K> BlockingQueue<K> newPriorityQueue() {
        return new PriorityBlockingQueue<K>();
    }

    public static <K> BlockingQueue<K> newPriorityQueue(int size) {
        return new PriorityBlockingQueue<K>(size);
    }

    public static <K> BlockingQueue<K> newBlockingQueue() {
        return new LinkedBlockingQueue<K>();
    }

    public static <K> BlockingQueue<K> newBlockingQueue(int size) {
        return new LinkedBlockingQueue<K>(size);
    }

    public static <K> BlockingQueue<K> newIdBlockingQueue(String id) {
        return new IdBlockingQueue<K>(id);
    }

    public static <K> BlockingQueue<K> newIdBlockingQueue(int size, String id) {
        return new IdBlockingQueue<K>(size, id);
    }

    public static <K> Vector<K> newVector() {
        return new Vector<K>();
    }

    public static <K> Vector<K> newVector(int size) {
        return new Vector<K>(size);
    }

    public static <K> Set<K> newSet() {
        return new HashSet<K>();
    }

    public static <K> Set<K> newSet(int size) {
        return new HashSet<K>(size);
    }

    public static <K1,K2,V> Map<K2,V> mapGetOrCreateMap(Map<K1,Map<K2,V>> mapOfMaps, K1 firstKey) {
        Map<K2,V> m = mapOfMaps.get(firstKey);
        if(null == m){
            m = newMap();
            mapOfMaps.put(firstKey, m);
        }
        return m;
    }
    
    public static <K1,K2,V> Map<K2,V> mapGetOrCreateVector(Vector<Map<K2,V>> vectorOfMaps, int index) {
        Map<K2,V> m = vectorOfMaps.elementAt(index);
        if(null == m){
            m = newMap();
            vectorOfMaps.add(index, m);
        }
        return m;
    }

    public static <K> K getElementAt(Vector<K> vec, int index) {
        if(vec.size() <= index){
            vec.setSize(index + 10);
            return null;
        }
        return vec.elementAt(index);
    }

    //mw-
    //public static <K> void setElementAt(Vector<K> vec, int index, K value) {
    //    if(vec.size() <= index){
    //        vec.setSize(index + 10);
    //    }
    //    vec.setElementAt(value,index);
    //}
    //mw- end
    //mw+
    //returns previous value at the index position, if position didn't exist returns null
    public static <K> K setElementAt(Vector<K> vec, int index, K value) {
    	K retVal;
        if(vec.size() <= index){
            vec.setSize(index + 10);
            retVal=null;
        } else retVal=vec.elementAt(index);
        vec.setElementAt(value,index);
        return retVal;
    }
    //mw+ end
    
    public static <K> K randomElementIn(Collection<K> c){
    	K[] a = null;
    	return c.toArray(a)[rand.nextInt(c.size())];
    }
    
    public static <K> List<K> newList() {
        return new ArrayList<K>();
    }

    public static <K> List<K> newList(int initSize) {
        return new ArrayList<K>(initSize);
    }

    public static <K,V> ConcurrentMap<K,V> newConcurrentMap() {
        return new ConcurrentHashMap<K,V>();
    }
        
    public static <K,V> ConcurrentMap<K,V> newConcurrentMap(int initCap) {
        return new ConcurrentHashMap<K,V>(initCap);
    }

    public static <K> Set<K> intersect(Collection<K> a, Collection<K> b){
    	Set<K> ret = new HashSet<K>();
    	for(K v: a){
    		if(b.contains(v))
    			ret.add(v);
    	}
    	return ret;
    }
    
    public static <K> boolean isIntersectingWith(Collection<K> a, Collection<K> b){
    	
    	for(K v: a){
    		if(b.contains(v))
    			return true;
    	}
    	return false;
    	
    }
    
    /**
	 * 
	 * A predicate over an element V
	 *
	 * @param <V>
	 */
	public interface Predicate<V>{
		public boolean isTrue(V element);
	}
	
	public static class SetPredicate<V> implements Predicate<V>{
		
		private Set<V> set;
		
		public SetPredicate(Set<V> s){
			set=s;
		}
		
		public boolean isTrue(V e){
			return set.contains(e);
		}
	}

	public static class PredicateBasedIterator<V> implements Iterator<V> {
		
		private Iterator<V> iterator;	
		private Predicate<V> mask;
		private V next;
		
		public PredicateBasedIterator(Iterator<V> it, Predicate<V> pred){
			iterator = it;
			mask = pred;
		}

		public boolean hasNext() {
			do{
				if(!iterator.hasNext())
					return false;
				next = iterator.next();
			}
			while(next==null || !mask.isTrue(next));
			return true;
		}

		public V next() {
			if(next==null){
				if(hasNext()){
					V temp = next;
					next = null;
					return temp;
				}else{
					throw new IllegalArgumentException();
				}
			}else{
				return next;
			}
		}

		public void remove() throws UnsupportedOperationException {
			throw new UnsupportedOperationException();
		}
	};
	
    
	/**
	 * Returns the combinations of elements of the specified collections. The
	 * combinations are backed by the specified collections. Any change to one
	 * of the specified collections requires that this method be called again.
	 * 
	 * @param collections
	 *            the array of collections to process.
	 *            
	 * @return the combinations of elements of the specified collections.
	 * 
	 * @author Jean-Michel Busca (INRIA/LIP6)
	 * @param <E>
	 *            the type of the elements of the collections.
	 */
	@SuppressWarnings("unchecked")
	public static <E> Collection<Collection<E>> combinationsOf(Collection<Collection<E>> collections) {
		return combinationsOf(collections.toArray(new Collection[0]));
	}
	
	@SuppressWarnings("unchecked")
	public static <E> Collection<Collection<E>> combinationsOf(final Collection<E>... collections) {
		return new AbstractCollection<Collection<E>>() {

			// Objects fields
			private final int size = computeSize();
			private final Collection[] toProcess = nonEmptyCollections();
			
			// Interface methods
			public Iterator<Collection<E>> iterator() {
				return new Iterator<Collection<E>>() {

					// Object fields
					private int count = 0;
					private E[] elements = null;
					private Iterator<E>[] iterators = null;
					
					// Interface methods
					public boolean hasNext() {
						return count < size;
					}

					public Collection<E> next() {
						if (!hasNext()) {
							throw new NoSuchElementException();
						}
						defineNextCombination();
						return asCollection(elements);
					}

					public void remove() {
						throw new UnsupportedOperationException("remove");
					}
										
					// Internal methods
					private void defineNextCombination() {
						count += 1;
						if (elements == null) {
							iterators = new Iterator[toProcess.length];
							elements = (E[]) new Object[toProcess.length];
							for (int i = 0; i < iterators.length; i++) {
								iterators[i] = toProcess[i].iterator();
								elements[i] = iterators[i].next();
							}
							return;
						}
						for (int i = elements.length - 1; i >= 0; i--) {
							if (iterators[i].hasNext()) {
								elements[i] = iterators[i].next();
								break;
							}
							iterators[i] = toProcess[i].iterator();
							elements[i] = iterators[i].next();
						}
					}
					
				};
			}

			public int size() {
				return size;
			}
			
			// Internal methods
			private int computeSize() {
				int result = 1;
				for (Collection<E> collection : collections) {
					if (collection.size() == 0) {
						continue;
					}
					result *= collection.size();
				}
				return result;
			}
			
			private Collection<E>[] nonEmptyCollections() {
				List<Collection<E>> result = new ArrayList<Collection<E>>();
				for (Collection<E> collection : collections) {
					if (collection.size() > 0) {
						result.add(collection);
					}
				}
				return result.toArray(new Collection[0]);
			}
			
		};
		
	}
	
    private static <E> Collection<E> asCollection(final E[] elements) {
        return new AbstractCollection<E>() {

                public Iterator<E> iterator() {
                        return new Iterator<E>() {

                                // Object field
                                private int index = 0;

                                // Interface methods
                                public boolean hasNext() {
                                        return index < elements.length;
                                }

                                public E next() {
                                        if (!hasNext()) {
                                                throw new NoSuchElementException();
                                        }
                                        return elements[index++];
                                }

                                public void remove() {
                                        throw new UnsupportedOperationException("remove");
                                }

                        };
                }

                public int size() {
                        return elements.length;
                }

        };
    }
    
}
