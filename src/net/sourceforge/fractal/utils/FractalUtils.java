
package net.sourceforge.fractal.utils;

/**   
 * @author L. Camargos
* @author P. Sutra
* 
*/

public class FractalUtils {
	
    public static boolean inRange(String range, Integer id) {
        if(range.equals("*"))
            return true;
                
        //TODO: properly check the input string.
        String[] ranges = range.split(",");
        for(String s: ranges){
            if(s.contains("-")){
                String[] r = s.split("-");
                if((Integer.parseInt(r[0]) <= id) && (Integer.parseInt(r[1]) >= id))
                    return true;
            }else{
                int n = Integer.parseInt(s);
                if(id == n)
                    return true;
            }
        }
        
        return false;
    }
    
	public static void nap(int to){

		long startTime, waitUntil, currentTime;
		
		startTime = currentTime = System.currentTimeMillis();
		waitUntil = startTime + to;
		do{
			try{
				Thread.sleep(waitUntil - currentTime);
			} catch (InterruptedException e){
				e.printStackTrace();
			}
			currentTime = System.currentTimeMillis();
		} while ( waitUntil > currentTime );
	} 

}
