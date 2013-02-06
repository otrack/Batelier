package net.sourceforge.fractal.utils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

/**
 * 
 * @author Pierre Sutra
 * 
 * A graph implementation optimized for graphs with few edges.
 *
 */
public class GraphUtils{

	public static class Graph<T> implements Iterable<T>, Comparable<Graph<T>>, Cloneable, Externalizable{
		
		private static final long serialVersionUID = 1L;
		
		private LinkedHashMap<T,ArrayList<T>> in;
		private LinkedHashMap<T,ArrayList<T>> out;
		private int nbEdges;
		
		public Graph() {
			in = new LinkedHashMap<T,ArrayList<T>>(0);
			out = new LinkedHashMap<T, ArrayList<T>>(0);
			nbEdges = 0;
		}

		public boolean addVertex(T u){
			if(in.keySet().contains(u)) return false;
			in.put(u, new ArrayList<T>());
			return true;
		}

		public boolean containsVertex(T u){
			return in.keySet().contains(u);
		}

		public boolean removeVertex(T u){
			
			if(!in.containsKey(u)) return false;
			
			for(T v : in.get(u)){
				out.get(v).remove(u);
				nbEdges--;
			}
			in.remove(u);
			
			if(out.containsKey(u)){
				for(T v : out.get(u)){
					in.get(v).remove(u);
					nbEdges--;
				}
				out.remove(u);
			}
			
			return true;
		}
		
		public boolean removeAllVertices(Collection<T> c){
			boolean ret = false;
			for(T u : c){
				ret |= removeVertex(u);
			}			
			return ret;
		}

		public boolean removeEdge(T u, T v){
			if(in.get(v).remove(u)){
				out.get(u).remove(v);
				nbEdges--;			
				return true;
			}else{
				return false;
			}
		}

		public boolean addEdge(T u, T v){
			if(in.get(v).contains(u)) return false;
			in.get(v).add(u);
			if(!out.containsKey(u)){
				out.put(u, new ArrayList<T>(1));
			}
			out.get(u).add(v);
			nbEdges++;				
			return true;
		}

		public boolean containsEdge(T u, T v){
			return in.containsKey(v) && in.get(v).contains(u);
		}

		public Set<T> vertexSet(){
			return in.keySet();
		}
		
		public int inDegree(T v){
			return in.get(v).size();
		}
		
		public int outDegree(T v){
			if( !out.containsKey(v) ) return 0;
			return out.get(v).size();
		}
		

		public Collection<T> incomingOf(T u){
			return in.get(u);
		}
		
		@SuppressWarnings("unchecked")
		public Collection<T> outgoingOf(T u){
			if( !out.containsKey(u) ) return java.util.Collections.EMPTY_LIST;
			return out.get(u);
		}
		
		public int degree(T v ){
			return inDegree(v)+outDegree(v);
		}

		public int compareTo(Graph<T> g) {
			if(vertexSet().containsAll(g.vertexSet())){
				if(nbEdges >= g.nbEdges){
					for(T u : g.vertexSet()){
						for(T v : g.in.get(u)){
							if(! containsEdge(v, u) )throw new ClassCastException();
						}
					}
					return nbEdges==g.nbEdges && in.keySet().size() == g.in.keySet().size() ? 0 : 1;
				}else{
					throw new ClassCastException();
				}
			}else{
				if(g.nbEdges>=nbEdges){
					for(T u : vertexSet()){
						for(T v : in.get(u)){
							if( ! g.containsEdge(v, u) )throw new ClassCastException();
						}
					}
					return -1;
				}else{
					throw new ClassCastException();
				}
			}
		}

		public void mergeWith(Graph<T> g){
			for(T u : g.vertexSet()){
				addVertex(u);
				for(T v : g.incomingOf(u)){
					addVertex(v); 
					addEdge(v,u);
				}
			}
		}

		public String toString(){
			String ret = "[";
			for(T u : out.keySet()){
				for(T v : out.get(u)){
					ret+="("+u+","+v+")";
				}
			}
			ret = "["+in.keySet()+","+ret+"]]";
			return ret;
		}
		

		public void clear() {
			in.clear();
			out.clear();
			nbEdges = 0;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public Object clone(){
			
			try {
				
				Graph<T> g = (Graph<T>)super.clone();
				
				g.in = new LinkedHashMap<T, ArrayList<T>>();
				for(T u : in.keySet()){
					g.in.put(u, new ArrayList<T>(in.get(u)));
				}
				
				g.out = new LinkedHashMap<T, ArrayList<T>>();
				for(T u : out.keySet()){
					g.out.put(u, new ArrayList<T>(out.get(u)));
				}
				
				g.nbEdges = nbEdges;

				return g;
				
			} catch (CloneNotSupportedException e) {
				return null;	
			}
		}

		@Override
		public int hashCode(){
			return nbEdges;
		}
		
		public boolean equals(Graph<T> g){
			return this==g;
		}

		public boolean isCyclic() {
			try{
				Iterator<T> it = new TopologicalOrderIterator();
				int i=0;
				while(it.hasNext()){
					it.next();
					i++;
				}
				if(i!=vertexSet().size()) return true;
			}catch(IllegalArgumentException e){
				return true;
			}
			return false;
		}
		
		public Iterator<T> iterator(){
			if( nbEdges==0 ) return in.keySet().iterator();
			return (Iterator<T>) new TopologicalOrderIterator();
		}
		
		public int nbEdges(){
			return nbEdges;
		}
		

		public void writeExternal(ObjectOutput s) throws IOException {
			s.writeObject(in);
			s.writeObject(out);
			s.writeInt(nbEdges);
		}
		
		@SuppressWarnings("unchecked")
		public void readExternal(ObjectInput s) throws IOException, ClassNotFoundException {
			in = (LinkedHashMap<T, ArrayList<T>>) s.readObject();
			out = (LinkedHashMap<T, ArrayList<T>>) s.readObject();
			nbEdges = s.readInt();
		}
		
		//
		// inner classes
		//

		/**
		 * Deterministic topological order.
		 */
		class TopologicalOrderIterator implements Iterator<T>{

			Queue<T> queue;
			Map<T, Integer> inDegree;
			
			public TopologicalOrderIterator(){
				queue = new LinkedList<T>();
				inDegree = new HashMap<T, Integer>(in.keySet().size());
				for(T t : in.keySet()){
					inDegree.put(t, in.get(t).size());
					if(in.get(t).size()==0)  queue.offer(t);
				}
				if(queue.isEmpty() && !in.isEmpty()) throw new IllegalArgumentException(); // got a cycle
			}
			
			public boolean hasNext() {
				return !queue.isEmpty();
			}

			public T next() {
				if(!hasNext()) throw new NoSuchElementException();
				T u = queue.poll();
				for(T v : outgoingOf(u)){
					if(inDegree.get(v)==0) throw new IllegalArgumentException(); // got a cycle
					inDegree.put(v, inDegree.get(v) -1);
					if(inDegree.get(v)==0)
						queue.offer(v);
				}
				return u;
			}

			public void remove() {
				throw new RuntimeException("not implemented");
			}
			
		}

	}


}