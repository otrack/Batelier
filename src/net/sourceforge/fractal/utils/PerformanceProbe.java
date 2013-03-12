package net.sourceforge.fractal.utils;

import static java.lang.Math.*;

import java.io.*;
import java.text.*;
import java.util.*;

/**
 * A probe for measuring performance. This class defines several basic probes:
 * {@link SimpleCounter SimpleCounter}, {@link ValueRecorder ValueRecorder},
 * {@link TimeRecorder TimeRecorder} and {@link VariableSampler VariableSampler}.
 * <p>
 * A probe is identified by a name. The information it collects is automatically
 * output by calling the {@link #toString()} method when the JVM terminates. In
 * addition, this information may be output periodically during the execution of
 * the JVM, as specified by the {@link #setMonitoring} method. The output is by
 * default the standard output of the JVM. It can be redirected to a file
 * through the {@link #setOutput(String)} method.
 * 
 * @author J-M. Busca INRIA/Regal
 * 
 */

public abstract class PerformanceProbe {

	//
	// CONSTANTS
	//
	private static final long NOT_PERIODIC = -1;
	private static final long ALL_RECORDERS = -2;
	private static final DecimalFormat TIME_FORMAT =
		new DecimalFormat("000.000");

	//
	// CLASS FIELDS
	//
	@SuppressWarnings("unused")
	private static Thread probeShutdown = new ProbeShutdown();
	private static Timer probeTimer = new Timer("Probe timer", true);
	private static HashMap<Long, Collection<PerformanceProbe>> _perfProbes =
		new HashMap<Long, Collection<PerformanceProbe>>();

	private static long startTime = System.currentTimeMillis();
	private static PrintStream outputStream = System.out;

	//
	// OBJECT FIELDS
	//
	// probe parameters
	private String probeName;
	private long outputPeriod;
	private boolean doReset;

	//
	// CONSTRUCTORS
	//
	/**
	 * Creates a simple probe with the specified name.
	 * 
	 * @param name
	 *            the name of the probe.
	 */
	public PerformanceProbe(String name) {
		this.probeName = name;
		this.outputPeriod = NOT_PERIODIC;
		this.doReset = false;
		add();
	}

	/**
	 * @inheritDoc
	 */
	public String toString() {
		return "PerfsProbe";
	}

	//
	// OUTPUT CONTROL
	//
	/**
	 * Changes the output of all probes to the specified file. This method may
	 * be called multiple times during the execution of the JVM.
	 * 
	 * @param pathname
	 *            the pathname of the output file.
	 */
	public static void setOutput(String pathname) {

		// open specified file
		PrintStream previous = outputStream;
		try {
			outputStream = new PrintStream(new FileOutputStream(pathname));
		} catch (FileNotFoundException e) {
			// if file cannot be opened, output remains unchanged
			return;
		}

		// close previous output if not standard output
		if (previous != null && !previous.equals(System.out)) {
			previous.close();
		}

		// print date and time
		Format format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		outputStream.println("Starting on " + format.format(new Date()));

	}

	/**
	 * Starts or stops monitoring this probe according to specified parameters.
	 * When monitoring is on, this probe is periodically output at the specified
	 * period. If requested, the probe is reset after each periodical output.
	 * When monitoring if off, this probe is only output when the JVM
	 * terminates.
	 * <p>
	 * This method may be called several times during the execution of the JVM
	 * in order to start, stop, restart, etc. monitoring or to change monitoring
	 * period.
	 * 
	 * @param period
	 *            the monitoring period; specifying {@link #NOT_PERIODIC} stops
	 *            monitoring.
	 * @param doReset
	 *            {@code true} if the {@link #reset()} must be called after each
	 *            output and {@code false} otherwise.
	 */
	public void setMonitoring(long period, boolean doReset) {
		remove();
		this.outputPeriod = period;
		this.doReset = doReset;
		add();
	}

	//
	// PROBE CONTROL
	//
	/**
	 * Discards all information that this probe has collected.
	 */
	public abstract void reset();

	/**
	 * Releases this probe. It will not be output any more, whether periodically
	 * or upon termination.
	 */
	public void release() {
		remove();
	}

	//
	// PROBE TYPES
	//
	/**
	 * This probe is a simple counter incremented and decremented through the
	 * {@link #incr() incr()} and {@link #decr() decr()}. It outputs the
	 * current value of the counter.
	 * 
	 * @author J-M. Busca INRIA/Regal
	 * 
	 */
	public static class SimpleCounter extends PerformanceProbe {

		//
		// Object fields
		//
		private long _counterValue;

		//
		// Constructors
		//
		public SimpleCounter(String name) {
			super(name);
		}

		public String toString() {
			return _counterValue + "";
		}

		//
		// Methods
		//
		/**
		 * Increments the value of this counter.
		 */
		public void incr() {
			_counterValue += 1;
		}

		/**
		 * Decrements the value of this counter.
		 */
		public void decr() {
			_counterValue -= 1;
		}

		public void reset() {
			_counterValue = 0;
		}

	}

	/**
	 * This probe records a set of {@code double} values provided through the
	 * {@link #add(long) add()} method. It outputs the min, max, total, average
	 * and standard deviation of the current set.
	 * 
	 * @author J-M. Busca INRIA/Regal
	 * 
	 */
	public static class FloatValueRecorder extends PerformanceProbe {

		//
		// Constants
		//
		public static final String DEFAULT_FORMAT = "%m/%a/%M/%t [#%c]";

		//
		// Object fields
		//
		// parameters
		private float outputFactor = 1;
		private MessageFormat outputFormat =
			new MessageFormat(format(DEFAULT_FORMAT));

		// stats values
		private double valueCount;
		private double nullCount;
		private double minValue = Long.MAX_VALUE;
		private double maxValue = Long.MIN_VALUE;
		private double totalCount;
		private double totalCount2;
		
		//
		// Constructors
		//
		public FloatValueRecorder(String name) {
			super(name);
		}

		public String toString() {
			return stats();
		}

		//
		// Accessors
		//
		/**
		 * Sets the output format of this value recorder to the specified
		 * string. The string may include the following parameters: %a
		 * (average), %c (count), %d (deviation), %m (min), %M (max) %n (null
		 * count) and %t (total).
		 * 
		 * @param format
		 *            the output format to apply.
		 */
		public void setFormat(String format) {
			outputFormat = new MessageFormat(format(format));
		}

		/**
		 * Sets the conversion factor of this value recorder to the specified
		 * value. The conversion factor is the ratio between the registered
		 * values and the output statistics.
		 * 
		 * @param factor
		 *            the conversion factor to apply.
		 */
		public void setFactor(float factor) {
			outputFactor = factor;
		}

		//
		// Interface
		//
		/**
		 * Adds the specified value this value recorder.
		 * 
		 * @param value
		 *            the value to add.
		 * @return {@code true} if this call updated the min or the max value
		 *         and {@code false} otherwise.
		 */
		public boolean add(double value) {

			// update count
			valueCount += 1;
			if (value == 0) {
				nullCount += 1;
			}

			// update min, max
			boolean updated = false;
			if (value < minValue) {
				updated = (valueCount > 1);
				minValue = value;
			}
			if (maxValue < value) {
				updated = (valueCount > 1);
				maxValue = value;
			}

			// update totals
			totalCount += value;
			totalCount2 += (value / outputFactor) * (value / outputFactor);

			return updated;
		}

		/**
		 * Adds the specified value recorder to this value recorder.
		 * 
		 * @param other
		 *            the recorder to add to this one.
		 * @return {@code true} if this call updated the min or the max value
		 *         and {@code false} otherwise.
		 */
		public boolean add(ValueRecorder other) {

			if (other.outputFactor != outputFactor) {
				throw new RuntimeException(
					"cannot add sample with different factor");
			}

			valueCount += other.valueCount;
			nullCount += other.nullCount;

			boolean updated = false;
			if (other.minValue < minValue) {
				updated = true;
				minValue = other.minValue;
			}
			if (other.maxValue > maxValue) {
				updated = true;
				maxValue = other.maxValue;
			}

			totalCount += other.totalCount;
			totalCount2 += other.totalCount2;

			return updated;
		}

		public void reset() {
			valueCount = 0;
			nullCount = 0;
			minValue = Long.MAX_VALUE;
			maxValue = Long.MIN_VALUE;
			totalCount = 0;
			totalCount2 = 0;
		}

		//
		// Internal methods
		//
		private String format(String format) {
			String result = format;
			result = result.replace("%a", "{0}"); // average
			result = result.replace("%c", "{1}"); // count
			result = result.replace("%d", "{2}"); // deviation
			result = result.replace("%m", "{3}"); // min
			result = result.replace("%M", "{4}"); // max
			result = result.replace("%n", "{5}"); // nulls
			result = result.replace("%t", "{6}"); // total
			return result;
		}

		private String stats() {

			String minString, averageString, maxString, totalString, deviationString;

			// compute string representing stats values
			if (valueCount == 0) {
				minString = "0";
				averageString = "0";
				maxString = "0";
				totalString = "0";
				deviationString = "0";
			} else {
				minString = ((double) (minValue / outputFactor)) + "";
				maxString = ((double) (maxValue / outputFactor)) + "";
				totalString = Double.toString(totalCount / outputFactor);
				if (minValue == maxValue) {
					averageString = minString;
					deviationString = "0";
				} else {
					double average = totalCount / valueCount / outputFactor;
					double deviation =
						sqrt((totalCount2 / valueCount) - average * average);
					averageString = Double.toString(average);
					deviationString = Double.toString(deviation);
				}
			}

			// return stats values according to format
			String[] args =
				new String[] { averageString, valueCount + "", deviationString,
					minString, maxString, nullCount + "", totalString };
			return outputFormat.format(args);

		}
	}

	
	/**
	 * This probe records a set of {@code long} values provided through the
	 * {@link #add(long) add()} method. It outputs the min, max, total, average
	 * and standard deviation of the current set.
	 * 
	 * @author J-M. Busca INRIA/Regal
	 * 
	 */
	public static class ValueRecorder extends PerformanceProbe {

		//
		// Constants
		//
		public static final String DEFAULT_FORMAT = "%m/%a/%M/%t [#%c]";

		//
		// Object fields
		//
		// parameters
		private float outputFactor = 1;
		private MessageFormat outputFormat =
			new MessageFormat(format(DEFAULT_FORMAT));

		// stats values
		private long valueCount;
		private long nullCount;
		private long minValue = Long.MAX_VALUE;
		private long maxValue = Long.MIN_VALUE;
		private long totalCount;
		private long totalCount2;
		
		//
		// Constructors
		//
		public ValueRecorder(String name) {
			super(name);
		}

		public String toString() {
			return stats();
		}

		//
		// Accessors
		//
		/**
		 * Sets the output format of this value recorder to the specified
		 * string. The string may include the following parameters: %a
		 * (average), %c (count), %d (deviation), %m (min), %M (max) %n (null
		 * count) and %t (total).
		 * 
		 * @param format
		 *            the output format to apply.
		 */
		public void setFormat(String format) {
			outputFormat = new MessageFormat(format(format));
		}

		/**
		 * Sets the conversion factor of this value recorder to the specified
		 * value. The conversion factor is the ratio between the registered
		 * values and the output statistics.
		 * 
		 * @param factor
		 *            the conversion factor to apply.
		 */
		public void setFactor(float factor) {
			outputFactor = factor;
		}

		//
		// Interface
		//
		/**
		 * Adds the specified value this value recorder.
		 * 
		 * @param value
		 *            the value to add.
		 * @return {@code true} if this call updated the min or the max value
		 *         and {@code false} otherwise.
		 */
		public boolean add(long value) {

			// update count
			valueCount += 1;
			if (value == 0) {
				nullCount += 1;
			}

			// update min, max
			boolean updated = false;
			if (value < minValue) {
				updated = (valueCount > 1);
				minValue = value;
			}
			if (maxValue < value) {
				updated = (valueCount > 1);
				maxValue = value;
			}

			// update totals
			totalCount += value;
			totalCount2 += (value / outputFactor) * (value / outputFactor);

			return updated;
		}

		/**
		 * Adds the specified value recorder to this value recorder.
		 * 
		 * @param other
		 *            the recorder to add to this one.
		 * @return {@code true} if this call updated the min or the max value
		 *         and {@code false} otherwise.
		 */
		public boolean add(ValueRecorder other) {

			if (other.outputFactor != outputFactor) {
				throw new RuntimeException(
					"cannot add sample with different factor");
			}

			valueCount += other.valueCount;
			nullCount += other.nullCount;

			boolean updated = false;
			if (other.minValue < minValue) {
				updated = true;
				minValue = other.minValue;
			}
			if (other.maxValue > maxValue) {
				updated = true;
				maxValue = other.maxValue;
			}

			totalCount += other.totalCount;
			totalCount2 += other.totalCount2;

			return updated;
		}

		public void reset() {
			valueCount = 0;
			nullCount = 0;
			minValue = Long.MAX_VALUE;
			maxValue = Long.MIN_VALUE;
			totalCount = 0;
			totalCount2 = 0;
		}

		//
		// Internal methods
		//
		private String format(String format) {
			String result = format;
			result = result.replace("%a", "{0}"); // average
			result = result.replace("%c", "{1}"); // count
			result = result.replace("%d", "{2}"); // deviation
			result = result.replace("%m", "{3}"); // min
			result = result.replace("%M", "{4}"); // max
			result = result.replace("%n", "{5}"); // nulls
			result = result.replace("%t", "{6}"); // total
			return result;
		}

		private String stats() {

			String minString, averageString, maxString, totalString, deviationString;

			// compute string representing stats values
			if (valueCount == 0) {
				minString = "0";
				averageString = "0";
				maxString = "0";
				totalString = "0";
				deviationString = "0";
			} else {
				minString = ((long) (minValue / outputFactor)) + "";
				maxString = ((long) (maxValue / outputFactor)) + "";
				totalString =  ((long) (totalCount / outputFactor)) + "";
				if (minValue == maxValue) {
					averageString = minString;
					deviationString = "0";
				} else {
					double average = totalCount / valueCount / outputFactor;
					double deviation =
						sqrt((totalCount2 / valueCount) - average * average);
					averageString = Double.toString(average);
					deviationString = Double.toString(deviation);
				}
			}

			// return stats values according to format
			String[] args =
				new String[] { averageString, valueCount + "", deviationString,
					minString, maxString, nullCount + "", totalString };
			return outputFormat.format(args);

		}
	}

	/**
	 * This probe records a set of execution times delimited by the
	 * {@link #start() start()} and {@link #stop() stop()} methods. It outputs
	 * the min, max, total, average and standard deviation of the current set.
	 * Time is measured in nanosecond with {@link System#nanoTime()} and output
	 * in microsecond.
	 * 
	 * @author J-M. Busca INRIA/Regal
	 * 
	 */
	public static class TimeRecorder extends ValueRecorder {

		//
		// Object fields
		//
		private long startClock;

		//
		// Constructors
		//
		public TimeRecorder(String name) {
			super(name);
			setFactor(1000000);
			setFormat("%m/%a/%Mus [#%c - %tus]");
		}

		//
		// Methods
		//
		/**
		 * Starts execution time measurement.
		 */
		public void start() {
			startClock = System.nanoTime();
		}

		/**
		 * Stops execution time measurement.
		 */
		public void stop() {
			add(System.nanoTime() - startClock);
		}

	}

	/**
	 * This probe automatically samples a variable and records the sampled
	 * values in a {@link ValueRecorder ValueRecorder}. The variable to read is
	 * specified when allocating the probe.
	 */
	public static class VariableSampler extends ValueRecorder {

		//
		// Class fields
		//
		private static HashMap<Long, Collection<SamplingTask>> samplingTasks =
			new HashMap<Long, Collection<SamplingTask>>();

		//
		// Object fields
		//
		private VariableReader valueReader;
		private long samplingPeriod;

		//
		// Constructors
		//
		public VariableSampler(String name, VariableReader reader, long period) {
			super(name);
			this.valueReader = reader;
			this.samplingPeriod = period;
			add();
		}

		//
		// Methods
		//
		private void read() {
			add(valueReader.read());
		}

		public void release() {
			super.release();
			remove();
		}

		//
		// Sampling management
		//
		private void add() {
			Collection<SamplingTask> samplers =
				samplingTasks.get(samplingPeriod);
			if (samplers == null) {
				samplers = new Vector<SamplingTask>();
				samplingTasks.put(samplingPeriod, samplers);
				if (samplingPeriod != NOT_PERIODIC) {
					TimerTask task = new OutputTask(samplingPeriod);
					long next = nextOccurence(samplingPeriod);
					probeTimer.scheduleAtFixedRate(task, next, samplingPeriod);
				}
			}
			SamplingTask sampler = new SamplingTask(this);
			samplers.add(sampler);
		}

		private void remove() {
			Collection<SamplingTask> samplers =
				samplingTasks.get(samplingPeriod);
			if (samplers == null) {
				return;
			}
			samplers.remove(this);
		}

		private static class SamplingTask extends TimerTask {

			// Object fields
			private VariableSampler variableSampler;

			// Constructor
			SamplingTask(VariableSampler sampler) {
				variableSampler = sampler;
			}

			// Runnable interface
			public void run() {
				variableSampler.read();
			}
		}

	}

	/**
	 * An object that reads a variable of type long.
	 * 
	 * @author J-M. Busca INRIA/Regal
	 * 
	 */
	public interface VariableReader {

		public long read();
	}

	/**
	 * This probe reports various indicators on the JVM memory consumption. It
	 * has no specific method.
	 * 
	 * @author J-M. Busca INRIA/Regal
	 * 
	 */
	public static class JVMMemory extends PerformanceProbe {

		//
		// Constants
		//
		private static final long MEGA = 1024 * 1024;

		//
		// Object fields
		//
		private Runtime runtime;
		
		//
		// Constructors
		//
		public JVMMemory(String name) {
			super(name);
			runtime = Runtime.getRuntime();
		}

		public String toString() {
			String result = "f=" + (runtime.freeMemory() / MEGA) + "/";
			result += "t=" + (runtime.totalMemory() / MEGA) + "/";
			result += "m=" + (runtime.maxMemory() / MEGA) + "Mb";
			return result;
		}

		//
		// Methods
		//
		public void reset() {
		}

	}

	//
	// PROBE OUTPUT
	//
	private void output(String type) {
		StringBuffer result = new StringBuffer(128);
		result.append(time()).append(" ");
		result.append(type).append(" ");
		result.append(probeName).append(": ");
		result.append(toString());
		outputStream.println(result.toString());
	}

	private static String time() {
		float now = (float) (System.currentTimeMillis() - startTime) / 1000;
		return TIME_FORMAT.format(now);
	}

	//
	// PROBE MANAGEMENT
	//
	private void add() {
		Collection<PerformanceProbe> probes = _perfProbes.get(outputPeriod);
		if (probes == null) {
			probes = new Vector<PerformanceProbe>();
			_perfProbes.put(outputPeriod, probes);
			if (outputPeriod != NOT_PERIODIC) {
				TimerTask task = new OutputTask(outputPeriod);
				long next = nextOccurence(outputPeriod);
				probeTimer.scheduleAtFixedRate(task, next, outputPeriod);
			}
		}
		probes.add(this);
	}

	private void remove() {
		Collection<PerformanceProbe> probes = _perfProbes.get(outputPeriod);
		if (probes == null) {
			return;
		}
		probes.remove(this);
	}

	private static Collection<PerformanceProbe> list(long period) {
		if (period == ALL_RECORDERS) {
			Collection<PerformanceProbe> result =
				new Vector<PerformanceProbe>();
			for (Collection<PerformanceProbe> probes : _perfProbes.values()) {
				result.addAll(probes);
			}
			return result;
		}

		Collection<PerformanceProbe> result = _perfProbes.get(period);
		if (result == null) {
			return Collections.emptySet();
		}
		return result;
	}

	private static long nextOccurence(long period) {
		return (System.currentTimeMillis() - startTime) % period;
	}
	
	//
	// HELPER CLASSES
	//
	/**
	 * Outputs probes when the JVM exits.
	 */
	private static class ProbeShutdown extends Thread {

		//
		// Constructor
		//
		public ProbeShutdown() {
			super("Probe shutdown");
			setDaemon(true);
			Runtime.getRuntime().addShutdownHook(this);
		}

		//
		// Runnable interface
		//
		public void run() {

			// output all active probes
			for (PerformanceProbe probe : list(ALL_RECORDERS)) {
				probe.output("F");
			}

			// close output if not standard output
			if (!outputStream.equals(System.out)) {
				outputStream.close();
			}

		}

	}

	/**
	 * Outputs probes periodically when the JVM executes.
	 */
	private static class OutputTask extends TimerTask {

		//
		// Object fields
		//
		private long _taskPeriod;

		//
		// Constructor
		//
		public OutputTask(long period) {
			_taskPeriod = period;
		}

		//
		// Runnable interface
		//
		public void run() {

			// output appropriate probes
			for (PerformanceProbe probe : list(_taskPeriod)) {
				probe.output("P");
				if (probe.doReset) {
					probe.reset();
				}
			}

		}

	}

}
