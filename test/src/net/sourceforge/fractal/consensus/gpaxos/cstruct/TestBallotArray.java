package net.sourceforge.fractal.consensus.gpaxos.cstruct;

import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream.RECOVERY;
/**   
* @author P. Sutra
* 
*/
public class TestBallotArray {

	public static void main(String[] args){
		
		Set<Integer> acceptors = new HashSet<Integer>();
		for(int i=0; i<3; i++){
			acceptors.add(i);
		}
		BallotArray b = new BallotArray(CStructFactory.getInstance("net.sourceforge.fractal.consensus.gpaxos.cstruct.CSched"),acceptors,false,RECOVERY.DEFAULT);
		testBallot2Coordinator(b);
		
	}
	
	private static void testBallot2Coordinator(BallotArray b){
		for(int i=0;i<100;i++ ){
			if(b.isCoordinatorOf(1, i)){
				System.out.println("I coordinate "+i+", next is " + b.nextBallotToCoordinateAfter(1, i));
			}	
		}
	}
	
}
