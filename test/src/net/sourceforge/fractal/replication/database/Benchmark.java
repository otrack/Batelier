package net.sourceforge.fractal.replication.database;

import net.sourceforge.fractal.utils.PerformanceProbe;

/**   
* @author P. Sutra
* 
*/


public class Benchmark {
	
	private final static String dbPath = "/tmp/TpcbDB";
	
	// TODO 
	// use executing task for pstore.applyUpdates
	// batch transactions in genuineTerminationProtocol
	// change classes for read request/reply => use collections 
	
	private static void usage(){
		System.out.println("Usage: Benchmark fractalConfigFile #branches percentageGlobal #groups replicationFactor #clients #transactions algType interGroupDelay");
		System.out.println("where");
		
		System.out.println("\tfractalConfigFile" +
		"\n\t\t Fractal's XML configuration file");

		System.out.println("\t#branches" +
		"\n\t\t number of branches");
		
		System.out.println("\tpercentageGlobal in 0..1" +
		"\n\t\t percentage of global transactions");
		
		System.out.println("\t#groups" +
		"\n\t\t number of groups participating to the experiment");
		
		System.out.println("\treplication factor" +
		"\n\t\t number of times a database partition is replicated"
		+ "\n\t\t a database partition is #branches/#groups branches");		
		
		System.out.println("\t#clients" +
		"\n\t\t number of clients per node");
		
		System.out.println("\t#transactions" +
		"\n\t\t number of transactions sent by a client");
		
		System.out.println("\talgType = O|1|2"
		+"\n\t\t 0 is genuine wan atomic multicast"
	    +"\n\t\t 1 is non-genuine disaster vulnerable atomic multicast"
		+"\n\t\t 2 is non-genuine disaster tolerant atomic multicast");
				
		System.out.println("\tinterGroupDelay " +
		"\n\t\t average two-hops delay between two groups");
		
		System.exit(-1);
	}

	public static void main(String args[]){

		if(args.length!=9){
			System.out.println("Incorrect number of arguments:" + args.length+"\n");
			usage();
		}

		// xml file
		String fractalXMLFile = args[0];
		
		// number of branches
		int nbranches = Integer.parseInt(args[1]);
		
		// percentage of global transactions
		float percentageGlobal = Float.parseFloat(args[2]);
		
		// number of groups 
		int nbGroups = Integer.parseInt(args[3]);

		//replication factor
		int replicationFactor = Integer.parseInt(args[4]);
		
		// number of tpcb clients per node
		int nbClients = Integer.parseInt(args[5]);

		//nb transactions each client executes
		int nbTransactions = Integer.parseInt(args[6]);

		// atomic multicast algorithm type
		// 0 - genuine disaster-vulnerable amcast 
		// 1 - non-genuine disaster-vulnerable amcast
		// 2 - non-genuine disaster-tolerant amcast
		int algType = Integer.parseInt(args[7]);

		// average latency between two groups
		int interGroupDelay = Integer.parseInt(args[8]);

		try {
			TPCB tpcb = new TPCB(fractalXMLFile, dbPath, 
											   nbranches, percentageGlobal, nbGroups, replicationFactor, 
											   nbClients, nbTransactions,
											   algType,  interGroupDelay);
			tpcb.launch();
			tpcb.stop();
			
		} catch (Exception e1) {
			e1.printStackTrace();
			PerformanceProbe.setOutput("/dev/null");
			System.exit(-1);
		}

		System.exit(0);
		

	}
	
}
