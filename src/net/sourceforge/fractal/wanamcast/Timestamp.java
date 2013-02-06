package net.sourceforge.fractal.wanamcast;

import net.sourceforge.fractal.utils.Pair;
/**   
* @author P. Sutra
* 
*/

@SuppressWarnings("unchecked")
public class Timestamp implements Comparable {

	Integer gclock;
	Integer site;
	Integer lclock;
	
	Timestamp(Pair<Integer,Integer> uid, Integer round){
		this.site = uid.first;
		this.lclock = uid.second;
		this.gclock = round;
	}
	
	public int compareTo(Object o) {
		if(o== null || !(o instanceof Timestamp))
			throw new ClassCastException();
		Timestamp ts = (Timestamp)o;
		if(this.gclock > ts.gclock){
			return 1;
		}else if(this.gclock < ts.gclock){
			return -1;
		}else{
			if(this.site > ts.site){
				return 1;
			}else if(this.site < ts.site){
				return -1;
			}else{
				if(this.lclock > ts.lclock){
					return 1;
				}else if(this.lclock < ts.lclock){
					return -1;
				}else{
					return 0;
				}
			}
		}
	}
	
	public boolean equals(Object o) {
		if(o== null || !(o instanceof Timestamp) )
			throw new ClassCastException();
		Timestamp ts = (Timestamp) o;
		if(
			this.gclock == ts.gclock 
			&& this.site == ts.site
			&& this.lclock == ts.lclock
		){
			return true;
		}
		return false;
	}
	
	public String toString(){
		return gclock+":"+site+":"+lclock;
	}

	public int compareToTs(Pair<Integer, Integer> uidToObject, Integer clock) {	
		if(this.gclock > clock){
			return 1;
		}else if(this.gclock < clock){
			return -1;
		}else{
			if(this.site > uidToObject.first){
				return 1;
			}else if(this.site < uidToObject.first){
				return -1;
			}else{
				if(this.lclock > uidToObject.second){
					return 1;
				}else if(this.lclock < uidToObject.second){
					return -1;
				}else{
					return 0;
				}
			}
		}
	}

	public void setToTs(Pair<Integer, Integer> uidToObject, Integer clock) {
		this.gclock=clock;
		this.site=uidToObject.first;
		this.lclock=uidToObject.second;
		
	}

}
