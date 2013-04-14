package nl.clearj.commons.thread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BufferedProcessingThreadTest {

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
		BufferedProcessingThread<Object, Object> bufferedProcessingThread = new GiveTimeToProcessAfterInsertBufferedProcessingThread<Object, Object>() {

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
		BufferedProcessingThread<Object, Object> bufferedProcessingThread = new GiveTimeToProcessAfterInsertBufferedProcessingThread<Object, Object>() {

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
		bufferedProcessingThread.put(new Object(), value2);
		bufferedProcessingThread.put(new Object(), value3);
		bufferedProcessingThread.put(new Object(), value4);
		bufferedProcessingThread.join();
		assertEquals(2, initializeCount);
		assertEquals(4, processCount);
		assertEquals(new HashSet<Object>(Arrays.asList(value1, value2, value3, value4)), processedValues);
		assertEquals(2, finalizeCount);
	}

	@Test
	public void testProcessingAfterMaxInterval() throws InterruptedException {
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
			}
		};
		bufferedProcessingThread.setFlushBufferMaxIntervalMs(100);
		bufferedProcessingThread.put(new Object(), new Object());
		Thread.sleep(90);
		assertEquals(0, processCount);
		Thread.sleep(20);
		assertEquals(1, processCount);
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

	@Test
	public void testChangeOfFlushBufferMaxIntervalMs() throws InterruptedException {
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
			}
		};
		bufferedProcessingThread.put(new Object(), new Object());
		Thread.sleep(90);
		bufferedProcessingThread.setFlushBufferMaxIntervalMs(100);
		assertEquals(0, processCount);
		Thread.sleep(20);
		assertEquals(1, processCount);
	}

	@Test
	public void testChangeOfProcessingTriggerSize() throws InterruptedException {
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
			}
		};
		bufferedProcessingThread.put(new Object(), new Object());
		Thread.sleep(90);
		bufferedProcessingThread.setProcessingTriggerSize(1);
		assertEquals(0, processCount);
		Thread.sleep(20);
		assertEquals(1, processCount);
	}

	@Test
	public void testProcessingTriggerSizeEqualsMaxSize() throws InterruptedException {
		BufferedProcessingThread<Object, Object> bufferedProcessingThread = new GiveTimeToProcessAfterInsertBufferedProcessingThread<Object, Object>() {

			@Override
			void initializeProcessing() {
			}

			@Override
			void processValue(Object valueToProcess) {
				processCount++;
			}

			@Override
			void finalizeProcessing() {
			}
		};
		bufferedProcessingThread.setProcessingTriggerSize(2);
		bufferedProcessingThread.setMaxSize(2);
		bufferedProcessingThread.put(new Object(), new Object());
		bufferedProcessingThread.put(new Object(), new Object());
		assertEquals(2, processCount);
	}

	@Test
	public void testProcessingTriggerSizeAboveMaxSize() throws InterruptedException {
		BufferedProcessingThread<Object, Object> bufferedProcessingThread = new GiveTimeToProcessAfterInsertBufferedProcessingThread<Object, Object>() {

			@Override
			void initializeProcessing() {
			}

			@Override
			void processValue(Object valueToProcess) {
				processCount++;
			}

			@Override
			void finalizeProcessing() {
			}
		};
		bufferedProcessingThread.setProcessingTriggerSize(2);
		try {
			bufferedProcessingThread.setMaxSize(1);
			fail("ProcessingTriggerSize not be higher than MaxSize");
		} catch (IllegalArgumentException e) {
		}
		bufferedProcessingThread.setMaxSize(2);
		try {
			bufferedProcessingThread.setProcessingTriggerSize(3);
			fail("ProcessingTriggerSize not be higher than MaxSize");
		} catch (IllegalArgumentException e) {
		}
		bufferedProcessingThread.setProcessingTriggerSize(2);
	}

	@Test
	public void testIgnoreItemsAboveLimit() throws InterruptedException {
		BufferedProcessingThread<Object, Object> bufferedProcessingThread = new GiveTimeToProcessAfterInsertBufferedProcessingThread<Object, Object>() {

			@Override
			void initializeProcessing() {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}

			@Override
			void processValue(Object valueToProcess) {
				processCount++;
			}

			@Override
			void finalizeProcessing() {
			}
		};
		bufferedProcessingThread.setProcessingTriggerSize(2);
		bufferedProcessingThread.setMaxSize(2);
		for (int i = 0; i < 10; i++) {
			bufferedProcessingThread.put(new Object(), new Object());
		}
		Thread.sleep(10);
		assertEquals(2, processCount);
	}

	private static abstract class GiveTimeToProcessAfterInsertBufferedProcessingThread<K, V> extends
			BufferedProcessingThread<K, V> {

		private static final long SLEEP_AFTER_INSERT = 10;

		public GiveTimeToProcessAfterInsertBufferedProcessingThread() {
			super("test thread", true);
		}

		@Override
		public void put(K key, V value) {
			super.put(key, value);
			try {
				Thread.sleep(SLEEP_AFTER_INSERT);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
