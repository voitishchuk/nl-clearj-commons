package nl.clearj.commons.thread;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BufferedProcessingThread<K, V> extends Thread {

	private static final long PROCESSING_INTERVAL = 4L * 60 * 60 * 1000;
	private static final int SIZE_TRIGGERS_PROCESSING = 10000;
	private static final int SIZE_TOO_BIG_TO_ACCEPT_NEW_ITEMS = 2 * SIZE_TRIGGERS_PROCESSING;
	/** storage of key-values before processing is triggered */
	private final Map<K, V> bufferToProcess = new ConcurrentHashMap<K, V>();
//	private final Map<K, V> bufferToProcess = Collections.synchronizedMap(new HashMap<K,V>());
	private long nextProcessingTime;

	public BufferedProcessingThread(String threadName, boolean isDaemon) {
		super(threadName);
		setDaemon(isDaemon);
		start();
	}

	/**
	 * thread safe method to add new key-value pair
	 */
	public void put(K key, V value) {
		if (bufferToProcess.size() < SIZE_TOO_BIG_TO_ACCEPT_NEW_ITEMS) {
			bufferToProcess.put(key, value);
		}
		if (isBufferBigEnough()) {
			synchronized (this) {
				notifyAll();
			}
		}
	}

	private boolean isBufferBigEnough() {
		return bufferToProcess.size() > SIZE_TRIGGERS_PROCESSING;
	}

	@Override
	public void run() {
		while (!isInterrupted()) {
			nextProcessingTime = System.currentTimeMillis() + PROCESSING_INTERVAL;
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
			while ((!isBufferBigEnough()) && ((delayBeforeNextRun = getDelayBeforeNextRun()) > 0)) {
				this.wait(delayBeforeNextRun);
			}
		}
	}

	private long getDelayBeforeNextRun() {
		return nextProcessingTime - System.currentTimeMillis();
	}

	private void processValues() {
		initializeProcessing();
		for (V valueProcess : pullValuesToProcess()) {
			if (isInterrupted()) {
				// was called interrupt() == shutdown is requested
				return;
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

	int size() {
		return bufferToProcess.size();
	}

	/**
	 * implementation should check interruption status using isInterrupted() and
	 * exit ASAP on interruption
	 */
	abstract void initializeProcessing();

	/**
	 * implementation should check interruption status using isInterrupted() and
	 * exit ASAP on interruption
	 */
	abstract void processValue(V valueToProcess);

	/**
	 * implementation should check interruption status using isInterrupted() and
	 * exit ASAP on interruption
	 */
	abstract void finalizeProcessing();
}
