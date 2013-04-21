package nl.clearj.commons.thread;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public abstract class BufferedProcessingThread<K, V> extends Thread {

	private static final Logger LOG = Logger.getLogger(BufferedProcessingThread.class.getName());

	private final long flushBufferMaxIntervalMs;
	private final int processingTriggerSize;
	private final int maxSize;

	/** storage of key-values before processing is triggered */
	private final Map<K, V> bufferToProcess = new ConcurrentHashMap<K, V>();
	private long lastProcessingTime;

	public BufferedProcessingThread(Configuration configuration) {
		super(configuration.threadName);

		checkConfiguration(configuration);

		setDaemon(configuration.isDaemon);
		flushBufferMaxIntervalMs = configuration.flushBufferMaxIntervalMs;
		processingTriggerSize = configuration.processingTriggerSize;
		maxSize = configuration.maxSize;

		registerLastProcessingNow();
		start();
	}

	private static void checkConfiguration(Configuration configuration) {
		if (configuration.processingTriggerSize > configuration.maxSize) {
			throw new IllegalArgumentException("violation of constraint processingTriggerSize <= maxSize");
		}
	}

	private void registerLastProcessingNow() {
		lastProcessingTime = System.currentTimeMillis();
	}

	/**
	 * thread safe method to add new key-value pair
	 */
	public void put(K key, V value) {
		if (bufferToProcess.size() < maxSize) {
			bufferToProcess.put(key, value);
		} else {
			LOG.warning("ignoring value because buffer is too big; try to increase maxSize or improve processing throughput");
		}
		// TODO test this check
		if (isBufferedEnoughToStartProcessing()) {
			synchronized (this) {
				notifyAll();
			}
		}
	}

	private boolean isBufferedEnoughToStartProcessing() {
		return bufferToProcess.size() >= processingTriggerSize;
	}

	@Override
	public void run() {
		while (!isInterrupted()) {
			try {
				waitBeforeNextProcessing();
			} catch (InterruptedException e) {
				// was called interrupt() == shutdown is requested
				return;
			}
			processValues();
		}
	}

	private void waitBeforeNextProcessing() throws InterruptedException {
		long delayBeforeNextRun;
		synchronized (this) {
			while ((!isBufferedEnoughToStartProcessing()) && ((delayBeforeNextRun = getDelayBeforeNextRun()) > 0)) {
				this.wait(delayBeforeNextRun);
			}
		}
	}

	private long getDelayBeforeNextRun() {
		return (lastProcessingTime + flushBufferMaxIntervalMs) - System.currentTimeMillis();
	}

	private void processValues() {
		registerLastProcessingNow();
		initializeProcessing();
		for (V valueProcess : pullValuesToProcess()) {
			if (isInterrupted()) {
				// was called interrupt() == shutdown is requested
				break;
			}
			processValue(valueProcess);
		}
		finalizeProcessing();
	}

	private List<V> pullValuesToProcess() {
		List<V> valuesToProcess = new ArrayList<V>();
		for (K key : bufferToProcess.keySet()) {
			valuesToProcess.add(bufferToProcess.remove(key));
		}
		return valuesToProcess;
	}

	/**
	 * Implementation should check interruption status using isInterrupted() and
	 * exit ASAP on interruption.
	 * <p>
	 * Unhandled RuntimeException terminates processing thread.
	 */
	abstract void initializeProcessing();

	/**
	 * Implementation should check interruption status using isInterrupted() and
	 * exit ASAP on interruption.
	 * <p>
	 * Unhandled RuntimeException terminates processing thread.
	 */
	abstract void processValue(V valueToProcess);

	/**
	 * Implementation should check interruption status using isInterrupted() and
	 * exit ASAP on interruption.
	 * <p>
	 * Unhandled RuntimeException terminates processing thread.
	 */
	abstract void finalizeProcessing();

	public static class Configuration {

		private static final long DEFAULT_FLUSH_INTERVAL_MS = 1L * 60 * 60 * 1000;
		private static final int DEFAULT_TRIGGER_SIZE = 10000;

		private String threadName = BufferedProcessingThread.class.getName();
		private boolean isDaemon = true;
		private long flushBufferMaxIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
		private int processingTriggerSize = DEFAULT_TRIGGER_SIZE;
		private int maxSize = 2 * DEFAULT_TRIGGER_SIZE;

		public void setThreadName(String threadName) {
			this.threadName = threadName;
		}

		/**
		 * The Java Virtual Machine exits when the only threads running are all
		 * daemon threads.
		 */
		public void setDaemon(boolean isDaemon) {
			this.isDaemon = isDaemon;
		}

		/**
		 * @param flushBufferMaxIntervalMs
		 *            all buffered values will be processed after this time if
		 *            the buffer size did not achieve trigger size
		 */
		public void setFlushBufferMaxIntervalMs(long flushBufferMaxIntervalMs) {
			this.flushBufferMaxIntervalMs = flushBufferMaxIntervalMs;
		}

		/**
		 * @param processingTriggerSize
		 *            if buffer size achieved this value this immediately wakes
		 *            up processing thread of this object
		 */
		public void setProcessingTriggerSize(int processingTriggerSize) {
			this.processingTriggerSize = processingTriggerSize;
		}

		/**
		 * @param maxSize
		 *            if processing of values is slower then arrival, then new
		 *            values are ignored if buffer size achieved this size
		 */
		public void setMaxSize(int maxSize) {
			this.maxSize = maxSize;
		}
	}

}
