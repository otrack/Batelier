package net.sourceforge.fractal.consensus.paxos;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import net.sourceforge.fractal.ConstantPool;
import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.consensus.Consensus;
import net.sourceforge.fractal.consensus.ConsensusLearner;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.utils.CollectionUtils;
import net.sourceforge.fractal.utils.Pair;

/**
 * 
 * @author Lasaro Camargos
 * @author Pierre Sutra (few modifications, and a couple of bugs' hunts)
 *
 */

public final class PaxosStream implements Consensus, Runnable {

	// Instance management.
	private Map<Integer, InstanceKeeper> instances = Collections
			.synchronizedMap(new LinkedHashMap<Integer, InstanceKeeper>(
					ConstantPool.PAXOS_PURGE_MARK + 1, 0.75f, true) {
				private static final long serialVersionUID = 1L;

				@SuppressWarnings("unchecked")
				protected boolean removeEldestEntry(Map.Entry eldest) {
					if (ConstantPool.PAXOS_PURGE_INSTANCES) {
						if (ConstantPool.PAXOS_DL > 5
								&& size() > ConstantPool.PAXOS_PURGE_MARK)
							debug(" purging size " + size() + " > "
									+ ConstantPool.PAXOS_PURGE_MARK);
						return size() > ConstantPool.PAXOS_PURGE_MARK;
					} else {
						return false;
					}
				}
			});

	
	// Instances' shared vars.
	private Integer sw_id = null; // System wide Id
	private Integer acc_id = null; // my acceptor Id
	private Integer prp_id = null; // my proposer Id
	private Integer lrn_id = null; // my leaner Id

	private int n_acc, n_prp, n_lrn, quorum_size; // , fast_quorum_size;

	private BlockingQueue<Message> delivered;

	String ACC_GRP;
	String PRP_GRP;
	String LRN_GRP;

	private Thread[] myThreads;

	private String streamName;
	private Membership membership;
	
	
	// Local variables now global
	Comparator<PMPhase2B> b2comp;
	List<PMPhase2B> values;

	private BlockingQueue<Pair<Integer, Serializable>> notificationQueue = CollectionUtils
			.newBlockingQueue();

	private Set<ConsensusLearner> learners = CollectionUtils.newSet();

	public PaxosStream(
			String stream_name,
			int swid,
			String coordPolicyName,
			String acc_grp_name,
			String prp_grp_name,
			String lrn_grp_name,
			Membership m
		) {
		
		sw_id = swid;
		streamName = stream_name;
		membership = m;
		ACC_GRP = acc_grp_name;
		PRP_GRP = prp_grp_name;
		LRN_GRP = lrn_grp_name;
		

		delivered = CollectionUtils.newIdBlockingQueue("SWID=" + sw_id);

		if (membership.group(PRP_GRP).contains(sw_id)) {
			prp_id = sw_id;
			membership.group(PRP_GRP).registerQueue(new PMBroadcast().getMessageType(), delivered);
			membership.group(PRP_GRP).registerQueue(new PMPhase1B().getMessageType(), delivered);
			membership.group(PRP_GRP).registerQueue(new PMNACK().getMessageType(), delivered);
		}

		if (membership.group(ACC_GRP).contains(sw_id)) {
			acc_id = sw_id;
			membership.group(ACC_GRP).registerQueue(new PMPhase1A().getMessageType(), delivered);
			membership.group(ACC_GRP).registerQueue(new PMPhase2A().getMessageType(), delivered);
			membership.group(ACC_GRP).registerQueue(new PMLogRequest().getMessageType(), delivered);
		}

		if (membership.group(LRN_GRP).contains(sw_id)) {
			lrn_id = sw_id;
			membership.group(LRN_GRP).registerQueue(new PMPhase2B().getMessageType(), delivered);
			membership.group(LRN_GRP).registerQueue(new PMDecision().getMessageType(), delivered);
		}

		n_acc = membership.group(ACC_GRP).size();
		n_prp = membership.group(PRP_GRP).size();
		n_lrn = membership.group(LRN_GRP).size();
		quorum_size = n_acc / 2 + 1;
		// FIXME: this.fast_quorum_size = ?;

		new Thread(new Runnable() {
			public void run() {
				Pair<Integer, Serializable> nexti = null;
				while (true) {
					try {
						nexti = notificationQueue.take();
						for (ConsensusLearner clearner : learners) {
							clearner.learnConsensusDecision(streamName,
									nexti.first, nexti.second);
						}

					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}

		}, streamName + ":notificationQueue@" + sw_id).start();

		myThreads = new Thread[ConstantPool.PAXOS_PROC_THREADS_N];
		for (int i = 0; i < ConstantPool.PAXOS_PROC_THREADS_N; i++) {
			myThreads[i] = new Thread(this, streamName + ":main@" + sw_id);
			myThreads[i].start();
		}
		
		
		// Local variables now global
		b2comp = new Comparator<PMPhase2B>() {
			public int compare(PMPhase2B arg0, PMPhase2B arg1) {
				if (arg0.serializable.equals(arg1.serializable))
					return 0;
				else {
					int h0 = arg0.serializable.hashCode();
					int h1 = arg0.serializable.hashCode();
					return (h0 < h1 ? -1 : (h0 == h1 ? 0 : 1));
				}
			}
		};
		values = CollectionUtils.newList();
		
	}

	public void propose(Serializable value, int inst) {
		InstanceKeeper ik = getInstance(inst, true);
		if (ik.decided) {
			ik.returnInstance(true);
			return;
		}
		ik.returnInstance(false);

		// Send to the leader.
		if (ConstantPool.PAXOS_USE_UNICAST)
			membership.group(PRP_GRP).unicast(
					membership.group(PRP_GRP).leader(),
					new PMBroadcast(streamName, inst, value, sw_id));
		else
			membership.group(PRP_GRP).broadcast(
					new PMBroadcast(streamName, inst, value, sw_id));
	}

	public void optPropose(Serializable value, int inst) {
		if (ConstantPool.PAXOS_DL > 3)
			debug(":" + inst + " opt proposing v =" + value);
		this.propose(value, inst);
	}

	public Serializable decide(int inst) throws InterruptedException {
		return decide(inst, 0);
	}

	public boolean isDecided(int inst) {
		InstanceKeeper instance = getInstance(inst, true); // got
		return instance.decided;
	}

	public Serializable decide(int inst, long timeout) throws InterruptedException {
		
		InstanceKeeper instance = getInstance(inst, false);
		return instance.waitDecision(timeout);
			
	}

	// returns the correct instance keeper.
	synchronized private InstanceKeeper getInstance(Integer inst, boolean lock) {
		InstanceKeeper ik;
		ik = instances.get(inst);
		if (null == ik) {
			ik = new InstanceKeeper(inst, n_acc, acc_id, n_prp, prp_id,
					n_lrn, lrn_id, sw_id);
			instances.put(inst, ik);
		}

		if (lock)
			ik.lock();

		return ik;
	}

	public Serializable pollDecision(int inst) {
		throw new RuntimeException("not implemented yet");
	}

	public void run() {
		InstanceKeeper instanceKeeper;
		while (true) { // Process events.
			// Get a message from the queue.
			PMessage rcvMsg;

			try {
				if (ConstantPool.PAXOS_DL > 10)
					while (delivered.isEmpty()) {
						debug(delivered.size() + "@" + delivered.hashCode());
						Thread.sleep(1000);
					}
				rcvMsg = null;
				rcvMsg = (PMessage) delivered.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
				continue;
			}

			if (!rcvMsg.tag.equals(this.streamName)) {
				throw new RuntimeException("Unproper tag");
			}

			if (ConstantPool.PAXOS_DL > 3)
				debug(":" + rcvMsg.instance + " received "
						+ rcvMsg.getMessageType() + " with value : " + rcvMsg+" from "+rcvMsg.source);

			// If it is a log request
			if (rcvMsg.type == PMessage.LOGREQUEST) {
				PMLogRequest lrm = (PMLogRequest) rcvMsg;
				if (lrm.usedAcc[acc_id])
					continue;

				// Connect to the requester to be sure I am not wasting my time.
				Socket logSocket = null;

				try {
					logSocket = new Socket(lrm.rmAddress, lrm.port);
					ObjectOutput out = new ObjectOutputStream(
							new BufferedOutputStream(logSocket
									.getOutputStream()));
					out.writeInt(acc_id);

					// FIXME: Send the database image and merge logs in
					// parallel.

					LogEntry[][] vv_entries = lrm.filter.filter(FractalManager.getInstance(), sw_id);

					if (ConstantPool.PAXOS_DL > 2)
						System.out.println("Paxos sending " + vv_entries.length
								+ " sets of entries");

					out.writeInt(vv_entries.length);
					for (int d1 = 0; d1 < vv_entries.length; d1++) {
						out.writeInt(vv_entries[d1].length);
						for (int d2 = 0; d2 < vv_entries[d1].length; d2++) {
							out.writeObject(vv_entries[d1][d2]);
						}
					}

					out.flush();
					out.close();
					logSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				continue;
			}

			// Get the instance keeper.
			instanceKeeper = getInstance(rcvMsg.instance, true);

			if (instanceKeeper.decided) {
				switch (rcvMsg.type) {
				case PMessage.PHASE1_A:
				case PMessage.PHASE2_A:
				case PMessage.BROADCAST: {
					
					// FIXME : Ignored when we have reliable links
					//					loader.membership.group(LRN_GRP).unicastSW(
					//					rcvMsg.source,
					//					new PMDecision(this.streamName, rcvMsg.instance,
					//					instanceKeeper.decision.iterator().next())
					//					);
				}
					;
					break; // switch
				default: {
					if (ConstantPool.PAXOS_DL > 4)
						debug(": instance " + rcvMsg.instance
								+ " was already decided. Ignoring ");
				}
					;
					break; // switch
				}
				instanceKeeper.returnInstance(true);
				continue; // while
			}

			if (ConstantPool.PAXOS_DL > 4)
				debug(": got instanceKeeper for instance " + rcvMsg.instance);

			// Switch the message.
			switch (rcvMsg.type) {
			/**
			 * To be executed by a proposer.
			 */
			case PMessage.BROADCAST: { // Someone wants this value proposed.
				if (null == prp_id) {// I am not a proposer. Don't even
										// bother. I shouldn't even be getting
										// this message.
					if (ConstantPool.PAXOS_DL > 5)
						debug(": I am not a proposer. Forget this message.");
					break; // switch
				}

				PMBroadcast bm = (PMBroadcast) rcvMsg;
				if (membership.group(PRP_GRP).isLeading(membership.myId())) {
									// Am I // FIXME !!!!!
									// the
									// leader?
					if (ConstantPool.PAXOS_DL > 4)
						debug(": I am the leader");

					if (null == instanceKeeper.proposal) {
						instanceKeeper.proposal = bm.serializable;
					}

					// TODO: Execute phase 1 with ticket reservation or jump to
					// phase 2.
					// Can I skip phase 1?
					if (instanceKeeper.nextRound == 0) {// TODO: if(reserved()
														// next round)
						if (ConstantPool.PAXOS_DL > 4)
							debug(": I have 0 reserved");

						instanceKeeper.incRound();

						if (ConstantPool.PAXOS_DL > 3)
							debug(":" + instanceKeeper.instance
									+ " sending phase2a:"
									+ instanceKeeper.currentRound + " ==> "
									+ ACC_GRP);

						PMPhase2A m = new PMPhase2A(this.streamName,
								instanceKeeper.instance,
								instanceKeeper.currentRound,
								instanceKeeper.proposal, sw_id);

//						debug("Sending at instance "
//								+ instanceKeeper.currentRound
//								+ " Phase2A message DIRECTLY : " + m);

						membership.group(ACC_GRP).broadcast(m);
					} else {
						if (ConstantPool.PAXOS_DL > 4)
							debug(": I will execute phase1");

						instanceKeeper.incRound();

						if (ConstantPool.PAXOS_DL > 3)
							debug(":" + instanceKeeper.instance
									+ " sending phase1a:"
									+ instanceKeeper.currentRound + " ==> "
									+ ACC_GRP);

						membership.group(ACC_GRP).broadcast(
								new PMPhase1A(this.streamName,
										instanceKeeper.instance,
										instanceKeeper.currentRound, prp_id, sw_id));
					}
					// FIXME: There is a conflict between round robin leader and
					// ticket reservation.
				} else {
					if (ConstantPool.PAXOS_DL > 4)
						debug(": I am not the leader. Forget this message.");
				}

			}
				;
				break;// switch
			/**
			 * To be executed by acceptors.
			 */
			case PMessage.PHASE1_A: {
				PMPhase1A a1m = (PMPhase1A) rcvMsg;
				if (instanceKeeper.h_ts_promissed <= a1m.timestamp) {

					instanceKeeper.h_ts_promissed = a1m.timestamp;

					try {
						instanceKeeper.logYourself();
					} catch (IOException e) {
						e.printStackTrace();
						throw new RuntimeException(
								"UNABLE TO WRITE TO LOG FILE. CRASHING TO ENSURE SAFETY!!!");
					}

					membership.group(PRP_GRP).unicast(
							a1m.getPrpId(),
							new PMPhase1B(this.streamName,
									instanceKeeper.instance, a1m.timestamp,
									instanceKeeper.h_ts_accepted,
									instanceKeeper.accepted, sw_id));
				} else {
					if (ConstantPool.PAXOS_DL > 3)
						debug(":" + instanceKeeper.instance + " sending NACK"
								+ " ==> " + PRP_GRP + "[" + a1m.getPrpId()
								+ "]");

					membership.group(PRP_GRP).unicast(
							a1m.getPrpId(),
							new PMNACK(this.streamName,
									instanceKeeper.instance, a1m.timestamp,
									instanceKeeper.h_ts_promissed, sw_id));
				}
			}
				;
				break; // switch

			/**
			 * To be executed by THE proposer.
			 */
			case PMessage.PHASE1_B: {
				PMPhase1B b1m = (PMPhase1B) rcvMsg;

				Set<PMPhase1B> b1Set;

				if (null == instanceKeeper.currentRound) {
					debug("I got a phase 1B but my currentRound for instance "
							+ instanceKeeper.instance
							+ " is null. My nextRound is "
							+ instanceKeeper.nextRound
							+ "\nThat mean that someone proposed as if it was me. I am discarding this message.");
					break;
				}

				if (instanceKeeper.currentRound == b1m.timestamp) {
					if (ConstantPool.PAXOS_DL > 3)
						debug(":" + instanceKeeper.instance + " phase1B:"
								+ b1m.timestamp + "=="
								+ instanceKeeper.currentRound);

					b1Set = instanceKeeper.b1Set;
				} else { // if I am getting messages for a round I am not
							// going to send 2a, forget them.
					if (ConstantPool.PAXOS_DL > 3)
						debug(":" + instanceKeeper.instance + " phase1B:"
								+ b1m.timestamp + "!="
								+ instanceKeeper.currentRound);
					break; // switch
				}

				b1Set.add(b1m);

				if (ConstantPool.PAXOS_DL > 3)
					debug(":" + instanceKeeper.instance + " phase1BSet has "
							+ b1Set.size() + " messages");

				if (b1Set.size() == quorum_size) {
					// FIXME: empty all the smaller ones and avoid them being
					// populated again.
					PMPhase1B mostRecent = b1m;
					for (PMPhase1B m : b1Set) {
						if (mostRecent.highestA == null
								|| (m.highestA != null && m.highestA > mostRecent.highestA)) {
							mostRecent = m;
						}
					}

					/*
					 * I don't need to check if I am leader. If I got these
					 * messages, I thought I was.
					 */
					if (ConstantPool.PAXOS_DL > 3)
						debug(":" + instanceKeeper.instance
								+ " sending phase2A "
								+ instanceKeeper.currentRound + " ==> "
								+ ACC_GRP);

					PMPhase2A m = null;

					if (mostRecent.highestA != null) {
						m = new PMPhase2A(this.streamName,
								instanceKeeper.instance, b1m.timestamp,
								mostRecent.serializable, sw_id);
					} else {
						m = new PMPhase2A(this.streamName,
								instanceKeeper.instance, b1m.timestamp,
								instanceKeeper.proposal, sw_id);
					}

					//debug("Sending Phase2A AFTER PHASE 1B message : " + m);

					membership.group(ACC_GRP).broadcast(m);
				}
			}
				;
				break;// switch
			/**
			 * To be executed by acceptors.
			 */
			case PMessage.PHASE2_A: {
				PMPhase2A a2m = (PMPhase2A) rcvMsg;
				//debug("Phase2A message received: " + a2m);
				if (a2m.timestamp >= instanceKeeper.h_ts_promissed) {
					instanceKeeper.h_ts_promissed = a2m.timestamp;
					instanceKeeper.h_ts_accepted = a2m.timestamp;
					instanceKeeper.accepted = a2m.serializable;

					try {
						instanceKeeper.logYourself();
					} catch (IOException e) {
						e.printStackTrace();
						throw new RuntimeException(
								"UNABLE TO WRITE TO LOG FILE. SAFETY VIOLATED!!!");
					}

					if (ConstantPool.PAXOS_DL > 3)
						debug(":" + instanceKeeper.instance
								+ " sending phase2B " + a2m.timestamp + " ==> "
								+ LRN_GRP);

					PMPhase2B m = new PMPhase2B(this.streamName,
							instanceKeeper.instance, a2m.timestamp,
							instanceKeeper.accepted, acc_id, sw_id);
					//debug("Sending Phase2B message : " + m);
					membership.group(LRN_GRP).broadcast(m);
				} else {
					if (ConstantPool.PAXOS_DL > 3)
						debug(":" + instanceKeeper.instance + " sending NACK "
								+ a2m.timestamp + " ==> " + PRP_GRP);

					membership.group(PRP_GRP).broadcast(
							new PMNACK(this.streamName,
									instanceKeeper.instance, a2m.timestamp,
									instanceKeeper.h_ts_promissed, sw_id));
				}

			}
				;
				break; // switch
			/**
			 * To be executed by Learners.
			 */
			case PMessage.PHASE2_B: {
				PMPhase2B b2m = (PMPhase2B) rcvMsg;

				Map<Integer, PMPhase2B> b2Map = CollectionUtils
						.mapGetOrCreateMap(instanceKeeper.b2Maps, b2m.timestamp);
				// if(instanceKeeper.b2Maps.size() >
				// ConstantPool.PAXOS_2B_SETS_MAXSIZE)
				// Collections.sort(new
				// ArrayList<Entry<Integer,Map<Integer,PMPhase2B>>>(instanceKeeper.b2Maps.entrySet()),
				// new Comparator<Entry<Integer,Map<Integer,PMPhase2B>>>(){
				// public int compare(Entry<Integer, Map<Integer, PMPhase2B>>
				// arg0, Entry<Integer, Map<Integer, PMPhase2B>> arg1) {
				// return arg0.getKey().compareTo(arg1.getKey());
				// }});

				b2Map.put(b2m.getAccId(), b2m);

				if (b2Map.size() >= quorum_size) {
					
					values.clear();
					values.addAll(b2Map.values());

					Collections.sort(values, b2comp);

					PMPhase2B pivot = null;
					Boolean decide = false;
					for (int i = 0; !decide
							&& i + quorum_size - 1 < values.size();) {
						pivot = values.get(i);
						if (b2comp.compare(pivot, values.get(i + quorum_size
								- 1)) == 0) {// We got a majority.
							decide = true;
						} else {
							i = values.lastIndexOf(pivot) + 1;
						}
					}

					if (decide) {
						instanceKeeper.decide(pivot.serializable);

						if (ConstantPool.PAXOS_DL > 3)
							debug(": " + instanceKeeper.instance
									+ " added to notification queue");
						
						try {
							instanceKeeper.logYourself();
						} catch (IOException e) {
							e.printStackTrace();
						}
						
					} else {
						if (ConstantPool.PAXOS_DL > 3)
							debug(": " + instanceKeeper.instance
									+ " got enough, but not equal");
					}
				}
			}
				;
				break; // switch
			case PMessage.NACK: {
				// TODO: Handle NACK messages.
			}
				;
				break; // switch
			case PMessage.DECISION: {
				PMDecision decm = (PMDecision) rcvMsg;

				if (ConstantPool.PAXOS_DL > 3)
					debug(": " + instanceKeeper.instance + " learning directly");

				instanceKeeper.decide(decm.serializable);
				if (ConstantPool.PAXOS_DL > 3)
					debug(": " + instanceKeeper.instance
							+ " added to notification queue");
				try {
					instanceKeeper.logYourself();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// FIXME : same reason as above
				//				notificationQueue.add(new Pair<Integer, Serializable>(
				//				instanceKeeper.instance, instanceKeeper.decision
				//				.iterator().next()));

				if (ConstantPool.PAXOS_DL > 3)
					debug(": " + instanceKeeper.instance
							+ " added to notification queue");
			}
				;
				break; // switch
			default:
				System.out
						.println("Uh oh! Your forgot to switch for kind of message!!!");
				break; // switch
			}
			;
			instanceKeeper.returnInstance(true);
		}
	}

	public boolean deliver(PMessage pm) {
		return delivered.add(pm);
	}

	public void registerLearner(ConsensusLearner learner) {
		learners.add(learner);
	}

	public void unregisterLearner(ConsensusLearner learner) {
		learners.remove(learner);
	}

	public String getProposersGroupName() {
		return PRP_GRP;
	}

	public String getAcceptorsGroupName() {
		return ACC_GRP;
	}

	public String getLearnersGroupName() {
		return LRN_GRP;
	}

	private void debug(String string) {
		System.out.println("Paxos( " + sw_id + " , "+ System.currentTimeMillis()+" ) "+ string);
	}

	public void requestLog(String consensusStreamName, InetAddress address,
			int port, LogFilter filter, boolean[] usedAcc) {
		membership.group(getAcceptorsGroupName()).broadcast(
				new PMLogRequest(consensusStreamName, address, port, filter,
						usedAcc, sw_id));
	}

	class InstanceKeeper {
		// General.
		int instance;
		int s_id;
		// private Integer n_acc;
		private Integer n_prp;
		// private Integer n_lrn;
		@SuppressWarnings("unused")
		private Integer a_id;
		private Integer p_id;
		// private Integer l_id;

		// Proposer
		Serializable proposal = null;
		public Set<PMPhase1B> b1Set;

		// Learner
		public Map<Integer, Map<Integer, PMPhase2B>> b2Maps;

		/**
		 * This variable is only used by proposers.
		 */
		Integer nextRound = null, currentRound = null;

		int h_ts_promissed = 0;
		// Acceptor
		Integer h_ts_accepted = null;
		Serializable accepted = null;
		boolean decided = false;
		private Set<Serializable> decision;
		boolean locked = false;

		public InstanceKeeper(int instance, Integer n_acc, Integer a_id,
				Integer n_prp, Integer p_id, Integer n_lrn, Integer l_id,
				int s_id) {
			this.instance = instance;
			// this.s_id = s_id;
			// this.n_acc = n_acc;
			this.n_prp = n_prp;
			// this.n_lrn = n_lrn;
			this.a_id = a_id;
			this.p_id = p_id;
			// this.l_id = l_id;
			decision = new HashSet<Serializable>();

			b2Maps = Collections
					.synchronizedMap(new LinkedHashMap<Integer, Map<Integer, PMPhase2B>>(
							ConstantPool.PAXOS_2B_SETS_MAXSIZE + 1, 0.75f, true) {
						private static final long serialVersionUID = 1L;

						@SuppressWarnings("unchecked")
						protected boolean removeEldestEntry(Map.Entry eldest) {
							if (ConstantPool.PAXOS_DL > 5
									&& size() > ConstantPool.PAXOS_2B_SETS_MAXSIZE)
								debug(" purging 2bsets " + size() + " > "
										+ ConstantPool.PAXOS_2B_SETS_MAXSIZE);
							return size() > ConstantPool.PAXOS_2B_SETS_MAXSIZE;
						}
					});

			if (null != this.p_id) {
				this.nextRound =  0; //n_prp + p_id - (instance%n_prp)
				b1Set = CollectionUtils.newSet();
			}
		}

		public Serializable waitDecision(long timeout) throws InterruptedException {
			
			synchronized (decision) {
				
				if(decided)	return decision.iterator().next();
				
				do{
					decision.wait(timeout);
				}
				while( !decided && timeout==0);
				
				assert timeout!=0 || (decided && !decision.isEmpty());
				
				if(decision.isEmpty())
					return null;
				else
					return decision.iterator().next();

			}

		}

		public void decide(Serializable s) {
			if(decided) return;
			synchronized (decision) {
				decided = true;
				decision.add(s);
				assert !decision.isEmpty();
				decision.notify(); // for those who wait
			}
		}

		public void incRound() {
			// Watch out for need of synchronization for these variables.
			currentRound = nextRound;
			b1Set = CollectionUtils.newSet();
			nextRound += n_prp;
		}

		public InstanceKeeper unlog() throws IOException {
			if (ConstantPool.PAXOS_USE_STABLE_STORAGE) {
				if (FractalManager.getInstance().paxos.paxosUnLogThis(streamName, acc_id, this)) {
					return this;
				} else
					return null;
			} else {
				throw new RuntimeException(
						"Trying to unlog without using stable storage");
			}
		}

		public void logYourself() throws IOException {
			if (ConstantPool.PAXOS_USE_STABLE_STORAGE) {
				FractalManager.getInstance().paxos.paxosLogThis(streamName, acc_id, this);
			}
		}

		synchronized public void lock() {
			while (locked) {
				try {
					wait();
				} catch (InterruptedException e) {}
				locked = true;
			}
		}

		synchronized public void unlock() {
			locked = false;
			notify();
		}

		private void returnInstance(boolean unlock) {
			if (unlock)
				unlock();
		}

	}

}
