package net.sourceforge.fractal.utils;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

public class MathUtils {
	
	private static Random rand = new Random(System.nanoTime());

	public static double computeStdDev(LinkedList<Long> samples, double average) {
		
		int nbSamples = samples.size();
		
		double var = 0.0;
		
		for (long t : samples) 
			var += Math.pow(((double) t) - average, 2.0);
			
		var /= ((double) nbSamples);
		return Math.sqrt(var);
	}
	
	//Trims nb to keep the first nbDigits of the decimal part
	public static double trim(double nb, int nbDigits) {
		double tmp1 = nb * Math.pow(10.0, ((double) nbDigits));
		long tmp2 = Math.round(tmp1);
		tmp1 = ((double) tmp2) / Math.pow(10.0, ((double) nbDigits));
		return tmp1;
	}
	
	/**
	 * 
	 * @param lo
	 * @param hi
	 * @return a value between lo (incluse) and hi (exclusive) 
	 */
	public static int random_int(int lower, int higher) { 
		return (int) (lower + Math.random() * ( higher - lower) );
	}
	
	public static Set<Set<Integer>> getPinN(Set<Integer> set, int p){
		if( set.size() < p ) return Collections.emptySet();
		
		Set<Set<Integer>> res = new HashSet<Set<Integer>> ();
		Set<Integer> toRemove = new HashSet<Integer>(set);
		
		for(int i : set){
			toRemove.remove(i);
			if(p==1){
				Set<Integer> branch = new HashSet<Integer>();
				branch.add(i);
				res.add(branch);
			}else{
				for(Set<Integer> branch : getPinN(toRemove, p-1)){
					branch.add(i);
					res.add(branch);
					res.add(branch);
				}
			}
		}
		
		return res;
	}
	
    public static long factorial( int n ){
        if( n <= 1 ) 
            return 1;
        else
            return n * factorial( n - 1 );
    }

	public static void main(String[] args){
		Set<Integer> s = new HashSet<Integer>();			
		
		for(int i=1; i<=3; i++){
			s.add(i);
		}
		
		int n = s.size();
		int p = (2*s.size())/3 +1;
		
		System.out.println(s);
		System.out.println("C_"+p+"^"+n+" = " + ( factorial(n) / (factorial(p)*factorial(n-p)) ) );
		
		Set<Set<Integer>> combinaison = getPinN(s,p);
		System.out.print(combinaison.size()+" => ");
		for(Set<Integer> t : combinaison ){
			for(Set<Integer> u : combinaison)
				if( !u.equals(t) )
					assert !u.containsAll(t);
			System.out.print(t+" ");
		}
	}

	public static <T> T random_in_set(Set<T> s) {
		int item = rand.nextInt(s.size());
		int i = 0;
		for(T o: s)
		{
		    if (i == item)
		        return o;
		    i = i + 1;
		}
		return null;
	}
	
}
