package nl.clearj.commons.thread;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BufferedProcessingThread<K, V> extends Thread {

	private long flushBufferIntervalMs = 4L * 60 * 60 * 1000;
	private int processingTriggerSize = 10000;
	private int maxSize = 2 * processingTriggerSize;
	/** storage of key-values before processing is triggered */
	private final Map<K, V> bufferToProcess = new ConcurrentHashMap<K, V>();
	private long nextProcessingTime;

	public BufferedProcessingThread(String threadName, boolean isDaemon) {
		super(threadName);
		setDaemon(isDaemon);
		start();
	}

	public void setFlushBufferIntervalMs(long flushBufferIntervalMs) {
		this.flushBufferIntervalMs = flushBufferIntervalMs;
	}

	public void setProcessingTriggerSize(int processingTriggerSize) {
		this.processingTriggerSize = processingTriggerSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	/**
	 * thread safe method to add new key-value pair
	 */
	public void put(K key, V value) {
		if (bufferToProcess.size() < maxSize) {
			bufferToProcess.put(key, value);
		}
		if (isBufferBigEnough()) {
			synchronized (this) {
				notifyAll();
			}
		}
	}

	private boolean isBufferBigEnough() {
		return bufferToProcess.size() > processingTriggerSize;
	}

	@Override
	public void run() {
		while (!isInterrupted()) {
			nextProcessingTime = System.currentTimeMillis() + flushBufferIntervalMs;
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
