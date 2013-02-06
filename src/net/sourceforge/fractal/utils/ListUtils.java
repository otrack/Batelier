package net.sourceforge.fractal.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class ListUtils {

	@SuppressWarnings("unchecked")
	public static int compareList(List l0, List l1){
		for(int i = 0; i < l0.size(); i++){
			if(l1.size()==i) return -1;
			if( ! l0.get(i).equals(l1.get(i)) ) throw new ClassCastException();
		}
		if(l1.size() == l0.size()) return 0;
		return 1;
	}

	/**
	 * Everything before the value returned, belongs to the common prefix.
	 * @param l0
	 * @param l1
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static int getCommonPrefixIndex(List l0, List l1){
		int prefix=0; 
		for(; 
		prefix < l0.size()
		&& prefix < l1.size()
		&& l0.get(prefix).equals(l1.get(prefix));
		prefix++){
		}
		return prefix;
	}

	@SuppressWarnings("unchecked")
	public static int getCommonPrefixIndex(Collection<List> set){
		int minIndex = Integer.MAX_VALUE;
		List<?> minList = null;

		if(set.isEmpty()) throw new ClassCastException();

		for(List<?> l : set){
			if(l.size() < minIndex){
				minIndex = l.size();
				minList = l;
			}
		}

		int prefix=0;

		for(;prefix < minIndex; prefix++){
			for(List<?> l : set){
				if( ! l.get(prefix).equals(minList.get(prefix)) ){
					return prefix;
				}
			}
		}
		return prefix;
	}
	
	public static <T> List<T> diff(List<T> list, List<T> list2) {
		List<T> ret = new ArrayList<T>();
		for(int i=list2.size(); i<list.size();i++)
			ret.add(list.get(i));
		return ret;
	}

	public static class DoubleLinkedList<T extends Serializable> implements List<T>, Serializable{		

		private static final long serialVersionUID = 1L;

		Node<T> head;
		Node<T> tail;
		int size;
		int hash;

		public DoubleLinkedList() {
			clear();
		}

		public DoubleLinkedList(DoubleLinkedList<T> l){
			head = l.head;
			tail = l.tail;
			size = l.size;
			hash = l.hash;
		}
		
		public DoubleLinkedList(Collection<T> c){
			clear();
			for(T e : c)
				add(e);
		}

		
		public boolean add(T arg0) {
			Node<T> n = new Node<T>(arg0);
			n.previous = tail; // to keep the meaning of iterator.next().
			if(tail!=null) tail.next = n;
			tail = n;
			size ++;
			hash += arg0.hashCode();
			if(head==null) head = n;
			return true;
		}

		
		public void add(int arg0, T arg1) {
			throw new RuntimeException("NYI");
		}

		
		public boolean addAll(Collection<? extends T> arg0) {
			boolean ret = false;
			for(T e : arg0){
				ret |= add(e);
			}
			return ret;
		}

		
		public boolean addAll(int arg0, Collection<? extends T> arg1) {
			throw new RuntimeException("NYI");
		}

		
		public void clear() {
			tail = head = null;
			size = hash = 0;
		}

		
		public boolean contains(Object arg0) {			
			for(T e : this){
				if(e.equals(arg0)) return true;
			}
			return false;
		}

		
		public boolean containsAll(Collection<?> arg0) {
			throw new RuntimeException("NYI");
		}

		
		public T get(int arg0) {
			if( arg0<0 || arg0>=size || isEmpty() ) throw new IndexOutOfBoundsException();
			Node<T> current= head;
			for(int i = 0; i<arg0; i++){
				assert current.next != null : this + " "+size+" "+arg0;
				current = current.next;
			}
			return current.element;
		}

		public int indexOf(Object arg0) {
			throw new RuntimeException("NYI");
		}

		public boolean isEmpty() {
			return size == 0;
		}

		public Iterator<T> iterator() {
			return new DoubleLinkedListIterator();
		}

		
		public int lastIndexOf(Object arg0) {
			throw new RuntimeException("NYI");
		}

		
		public ListIterator<T> listIterator() {
			throw new RuntimeException("NYI");
		}

		
		public ListIterator<T> listIterator(int arg0) {
			throw new RuntimeException("NYI");
		}

		
		public boolean remove(Object arg0) {
			throw new RuntimeException("NYI");
		}

		
		public T remove(int arg0) {
			if( arg0<0 || arg0>=size ) throw new IndexOutOfBoundsException();

			Node<T> current = head;
			for(int i=0; i<arg0; i++){
				current = current.next;
			}

			if(current==head || current==tail){
				if(current==head){
					head=current.next;
					if(head!=null)
						current.next.previous=null;
				}

				if(current==tail){
					tail=current.previous;
					if(tail!=null)
						current.previous.next = null;
				}
			}else{
				assert current.previous != null && current.next != null;
				current.previous.next = current.next;
				current.next.previous = current.previous;
			}
			
			size--;
			hash -= current.element.hashCode();
			return current.element;
		}

		
		public boolean removeAll(Collection<?> arg0) {
			throw new RuntimeException("NYI");
		}

		
		public boolean retainAll(Collection<?> arg0) {
			throw new RuntimeException("NYI");
		}

		
		public T set(int arg0, T arg1) {
			throw new RuntimeException("NYI");
		}

		
		public int size() {
			return size;
		}

		
		public List<T> subList(int arg0, int arg1) {
			throw new RuntimeException("NYI");
		}

		
		public Object[] toArray() {
			throw new RuntimeException("NYI");
		}

		@SuppressWarnings("hiding")
		
		public <T> T[] toArray(T[] arg0) {
			throw new RuntimeException("NYI");
		}

		public String toString(){
			String ret = "[";
			Iterator<T> it = iterator();
			while(it.hasNext()){
				ret+=it.next().toString();
				if(it.hasNext()) ret+=" ";
			}
			ret += "]";
			return ret;
		}
		
		
		public int hashCode(){
			return hash;
		}

		//
		// Inner classes
		//


		private class DoubleLinkedListIterator implements Iterator<T>{

			Node<T> current;

			DoubleLinkedListIterator(){
				current = head;
			}

			
			public boolean hasNext() {
				if( current == null) return false;
				return true;
			}

			
			public T next() {
				T e = current.element;
				current = current.next;
				return e;
			}

			
			public void remove() {
				throw new RuntimeException("NYI");
			}

		}


		private static class Node<T> implements Serializable{

			private static final long serialVersionUID = 1L;

			T element;
			Node<T> next;
			Node<T> previous;

			Node(T e){
				element = e;
				next = previous = null;
			}
		}


	}
}
