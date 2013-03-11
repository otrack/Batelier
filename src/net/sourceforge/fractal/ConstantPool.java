package net.sourceforge.fractal;

/**
 * These are debug level constants. Bigger the DL, more verbose output.
 * 
 * 0 == error messages only.
 * 1 == information about service start/stop.
 * 2 == detailed information
 * 3 ==
 * 4 == debug information
 * 5 == verbose debug information.
 * 10== get ready for a lot of information.
 *   
 * @author L. Camargos
 * @author Pierre Sutra
 * 
 */ 
public class ConstantPool {
	
	public static final int PROTOCOL_MAGIC = 0xCA552DFA;
	
    public static final int MEMBERSHIP_DL=10;  //membership service.
    public static final int PAXOS_RECOVERY_DL = 0;
    
    /**
     *  1 = little information
     *  2 = internals timers for micro-benchmark
     *  3 = messages received
     *  4 = proposed and learned commands
     *  8 = content of messages
     *  >10 = whole state of the stream
     */
    public static final int PAXOS_DL = 0;       //Paxos and GPaxos
    public static final int WAB_DL = 0;
    public static final int ABCAST_DL = 0;      // Atomic broadcast.
    public static final int BROADCAST_DL = 0;      // Reliable broadcast.
    public static final int MULTICAST_DL = 0;      // Reliable multicast.
    public static final int WANAMCAST_DL = 10;   // WAN Atomic multicast
    public static final int PAXOS_COMMIT_DL = 0;// Paxos Commit
    public static final int WANABCAST_DL = 0;   // WAN Atomic broadcast
    public static final int FTWanAMCast_DL = 0; // Fault-Tolerant Wan Atomic Multicast
    public static final int ABC_COMMIT_DL = 0;  // ABCast Commit
    public static final int BDB_DL = 0;         // If BDB is used for log keeping.	
    public static final boolean MESSAGE_STREAM_DEBUG = false;	
    public static final int QUEUES_DL = 0;
    public static final int TEST_DL=0;
    public static final int DUMMY_NET=1;
   
    /**
     * Reliable Broadcast stuff
     */
    public static final boolean RBCAST_UNIFORM=false;
     
    /**
     * Defines the number of threads handling messages in Paxos PER STREAM.
     * TODO Overwrite this in the config file.
     */
    public static final int PAXOS_PROC_THREADS_N = 1;
    
    /**
     * This constant determines if PAXOS will unicast or broadcast proposals to proposers.
     */
    public static final boolean PAXOS_USE_UNICAST = true;
    /**
     * This constant determines the number of timestamps learners pay attention to try to learn.
     */
    public static final int PAXOS_2B_SETS_MAXSIZE = 10;
    

    /**
     * This constant determines if PAXOS Acceptors will really write on disk.
     */
    public static final boolean PAXOS_USE_STABLE_STORAGE = false;
    
    /** Use BDB as log keeper. */
    public static final boolean PAXOS_USE_BDB = false; // FIXME reading from plain log not implemented
    
    /** 
     * Should Paxos reuse old log and recover(true), or should it remove the old log (false).
     * If you set this to true, remember that OLD DECISIONS WILL BE RECOVERED!
     * 
     * This value is just the default value. 
     * You will be able to overload it by passing the right flat on the init
     * method of FractalManager
     */
    public static boolean PAXOS_STABLE_STORAGE_RECOVERY = false;

	public static final boolean PAXOS_PURGE_INSTANCES = true;

    
    /**
     * Purge instances information from memory when the list gets to this size.
     */
    public static final int PAXOS_PURGE_MARK = 5000; // This parameter has a huge influence on memory consumption, tweak wisely.
    
    /**
     * Debug information on recovery
     */
    public static final int PAXOS_COMMIT_RECOVERY_DL = 4;


    /** WAB Based Consensus */
    public static final boolean WAB_USE_STABLE_STORAGE = false;
    public static final boolean WAB_USE_BDB = false;
    public static boolean WAB_STABLE_STORAGE_RECOVERY = false;
    public static final int WAB_PROC_THREADS_N = 1;
    public static final int WAB_SND_SETS_MAXSIZE = 3;

    
    
    /** Atomic Commitment */
    /**
     * Purge instances outcome when instances get this size.
     */
    public static final boolean COMMIT_PURGE_INSTANCES = true; 
    public static final int COMMIT_PURGE_MARK = 1000; 
    

    /**
     * Store transactions outcome on disk?.addA
     */
    public static final boolean ABC_COMMIT_USE_STABLE_STORAGE = true;
    public static final boolean ABC_COMMIT_USE_BDB = true;
    public static boolean ABC_COMMIT_STABLE_STORAGE_RECOVERY = true;
    public static final int ABC_COMMIT_RECOVERY_DL = 6;
    public static final boolean ABC_COMMIT_RECOVERY_STREAM = true;
    
    /**
     * Should multicast messages be grouped?
     */
    public static final boolean MEMBERSHIP_AGGREGATE_MULTICASTS = true;
    public static final long MEMBERSHIP_AGGREGATE_MULTICASTS_DELAY = 1L;
    
    
    /**
     * Defines the size of an abcast message. When abcasting, messages will be filled up to this size if there
     * many messages to be abcast.
     */
    public static final boolean ABCAST_AGGREGATE_ABCASTS = true;
    public static final int ABCAST_PURGE_HIGH_MARK = 120;
    public static final int ABCAST_PURGE_LOW_MARK = 80;
    
    /**
     * Should messages be zipped to fit more?
     */
    public static boolean ABCAST_ZIP = false ;
    
    
    
    
    /**
     * Header size depends on groups' and streams' names.
     * TODO: dynamically find out overhead per group/destination and 
     */
    
    /** Netork parameters */
    private static final int NETWORK_MAX_HEADER_SIZE = 72; //IP = [20-64], UDP = 8.
    /** 1500 is the Ethernet MTU size. If you let IP do fragmentation, you can use 65535. */
    public static final int NETWORK_MAX_DGRAM_SIZE = 65535;
    public static final int NETWORK_MAX_PAYLOAD_SIZE = NETWORK_MAX_DGRAM_SIZE - NETWORK_MAX_HEADER_SIZE; //successfully sent 65491;

    /** Membership 
     * Header size depends on groups' names.
     * 
     * Int(Destination) + Int(Source) + String(GroupName) + Byte(ClassId) + some for stream header.
     * 4 + 1            +  4 + 1        + 10 +2             + 1 + 1
     * 
     * */
//    	public static final int MEMBERSHIP_MAX_GROUPNAME = 10;
    private static final int MEMBERSHIP_MAX_HEADER_SIZE = 35; // + MEMBERSHIP_MAX_GROUPNAME;
    public static final int MEMBERSHIP_PAYLOAD_SIZE = NETWORK_MAX_PAYLOAD_SIZE - MEMBERSHIP_MAX_HEADER_SIZE;
    
    /** Paxos 
     * Header size depends on streams' names.
     * Byte + Intege + String + 3Integers
     * 
     * */
    private static final int PAXOS_MAX_HEADER_SIZE = 35;
    public static final int PAXOS_PAYLOAD_SIZE = MEMBERSHIP_PAYLOAD_SIZE - PAXOS_MAX_HEADER_SIZE;

    /** Abcast
     * Header size depends on streams' names.*/
    private static final int ABCAST_MAX_MULTIHEADER_SIZE = 50;
    public static final int ABCAST_MULTIMESSAGE_MAX_SIZE = PAXOS_PAYLOAD_SIZE - ABCAST_MAX_MULTIHEADER_SIZE; //Eventhough the message sometimes do not go through Paxos...
    private static final int ABCAST_MAX_HEADER_SIZE = 30;
    public static final int ABCAST_MESSAGE_MAX_SIZE = ABCAST_MULTIMESSAGE_MAX_SIZE - ABCAST_MAX_HEADER_SIZE;
    public static final boolean ABCAST_BIGMESSAGE_EXCEPTION = false;

}
