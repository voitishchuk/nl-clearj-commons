package nl.clearj.commons.thread;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BufferedProcessingThreadTest {

	private static final long SLEEP_AFTER_INSERT = 10;
	private int initializeCount;
	private int processCount;
	private Set<Object> processedValues;
	private int finalizeCount;

	@Before
	public void setUp() throws Exception {
		initializeCount = 0;
		processCount = 0;
		processedValues = new HashSet<Object>();
		finalizeCount = 0;
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testBasicProcessing() throws InterruptedException {
		final Object value1 = new Object();
		final Object value2 = new Object();
		BufferedProcessingThread<Object, Object> bufferedProcessingThread = new BufferedProcessingThread<Object, Object>(
				"test thread", true) {

			@Override
			void initializeProcessing() {
				initializeCount++;
				assertEquals(1, initializeCount);
				assertEquals(0, processCount);
				assertEquals(new HashSet<Object>(), processedValues);
				assertEquals(0, finalizeCount);
			}

			@Override
			void processValue(Object valueToProcess) {
				processCount++;
				processedValues.add(valueToProcess);
				assertEquals(1, initializeCount);
				assertEquals(0, finalizeCount);
			}

			@Override
			void finalizeProcessing() {
				finalizeCount++;
				assertEquals(1, initializeCount);
				assertEquals(2, processCount);
				assertEquals(new HashSet<Object>(Arrays.asList(value1, value2)), processedValues);
				assertEquals(1, finalizeCount);
				interrupt();
			}
		};
		bufferedProcessingThread.setProcessingTriggerSize(2);
		bufferedProcessingThread.put(new Object(), value1);
		bufferedProcessingThread.put(new Object(), value2);
		bufferedProcessingThread.join();
		assertEquals(1, initializeCount);
		assertEquals(2, processCount);
		assertEquals(new HashSet<Object>(Arrays.asList(value1, value2)), processedValues);
		assertEquals(1, finalizeCount);
	}

	@Test
	public void testProcessingOfItemsInSeveralRuns() throws InterruptedException {
		final Object value1 = new Object();
		final Object value2 = new Object();
		final Object value3 = new Object();
		final Object value4 = new Object();
		BufferedProcessingThread<Object, Object> bufferedProcessingThread = new BufferedProcessingThread<Object, Object>(
				"test thread", true) {

			@Override
			void initializeProcessing() {
				initializeCount++;
			}

			@Override
			void processValue(Object valueToProcess) {
				processCount++;
				processedValues.add(valueToProcess);
			}

			@Override
			void finalizeProcessing() {
				finalizeCount++;
				if (finalizeCount == 2) {
					interrupt();
				}
			}
		};
		bufferedProcessingThread.setProcessingTriggerSize(2);
		bufferedProcessingThread.put(new Object(), value1);
		Thread.sleep(SLEEP_AFTER_INSERT);
		bufferedProcessingThread.put(new Object(), value2);
		Thread.sleep(SLEEP_AFTER_INSERT);
		bufferedProcessingThread.put(new Object(), value3);
		Thread.sleep(SLEEP_AFTER_INSERT);
		bufferedProcessingThread.put(new Object(), value4);
		Thread.sleep(SLEEP_AFTER_INSERT);
		bufferedProcessingThread.join();
		assertEquals(2, initializeCount);
		assertEquals(4, processCount);
		assertEquals(new HashSet<Object>(Arrays.asList(value1, value2, value3, value4)), processedValues);
		assertEquals(2, finalizeCount);
	}

	@Test
	public void testBufferConcurrentReadWrite() throws InterruptedException {
		final int concurrencyTestSize = 1000;
		BufferedProcessingThread<Object, Object> bufferedProcessingThread = new BufferedProcessingThread<Object, Object>(
				"test thread", true) {

			@Override
			void initializeProcessing() {
			}

			@Override
			void processValue(Object valueToProcess) {
				processCount++;
			}

			@Override
			void finalizeProcessing() {
				if (processCount == concurrencyTestSize) {
					interrupt();
				}
			}
		};
		bufferedProcessingThread.setProcessingTriggerSize(0);
		for (int i = 1; i <= concurrencyTestSize; i++) {
			bufferedProcessingThread.put(new Object(), new Object());
		}
		bufferedProcessingThread.join();
		assertEquals(concurrencyTestSize, processCount);
	}
}
