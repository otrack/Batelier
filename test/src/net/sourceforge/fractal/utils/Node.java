package net.sourceforge.fractal.utils;

public class Node implements Comparable<Node>{

	public Integer id;
	public String ip;
	
	public Node(int i, String a){
		id=i;
		ip=a;
	}
	
	public boolean equals(Node n){
		return this.id==n.id;
	}
	
	public String toString(){
		return "node "+Integer.toString(id);
	}

	public int compareTo(Node n) {
		return this.id.compareTo(n.id);
	}
	
}
