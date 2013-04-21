package nl.clearj.commons.thread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import nl.clearj.commons.thread.BufferedProcessingThread.Configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BufferedProcessingThreadTest {

	private Configuration configuration;

	private int initializeCount;
	private int processCount;
	private Set<Object> processedValues;
	private int finalizeCount;

	@Before
	public void setUp() throws Exception {
		configuration = new Configuration();
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
		configuration.setProcessingTriggerSize(2);
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
		configuration.setProcessingTriggerSize(2);
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
		configuration.setFlushBufferMaxIntervalMs(100);
		BufferedProcessingThread<Object, Object> bufferedProcessingThread = new BufferedProcessingThread<Object, Object>(
				configuration) {

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
		assertEquals(0, processCount);
		Thread.sleep(20);
		assertEquals(1, processCount);
	}

	@Test
	public void testBufferConcurrentReadWrite() throws InterruptedException {
		configuration.setProcessingTriggerSize(0);
		final int concurrencyTestSize = 1000;
		BufferedProcessingThread<Object, Object> bufferedProcessingThread = new BufferedProcessingThread<Object, Object>(
				configuration) {

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
		for (int i = 1; i <= concurrencyTestSize; i++) {
			bufferedProcessingThread.put(new Object(), new Object());
		}
		bufferedProcessingThread.join();
		assertEquals(concurrencyTestSize, processCount);
	}

	@Test
	public void testProcessingTriggerSizeEqualsMaxSize() throws InterruptedException {
		configuration.setProcessingTriggerSize(2);
		configuration.setMaxSize(2);
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
		bufferedProcessingThread.put(new Object(), new Object());
		bufferedProcessingThread.put(new Object(), new Object());
		assertEquals(2, processCount);
	}

	@Test
	public void testProcessingTriggerSizeAboveMaxSize() throws InterruptedException {
		configuration.setProcessingTriggerSize(1);
		configuration.setMaxSize(1);

		new BufferedProcessingThread<Object, Object>(configuration) {

			@Override
			void initializeProcessing() {
			}

			@Override
			void processValue(Object valueToProcess) {
			}

			@Override
			void finalizeProcessing() {
			}
		};

		configuration.setProcessingTriggerSize(2);
		try {
			new BufferedProcessingThread<Object, Object>(configuration) {

				@Override
				void initializeProcessing() {
				}

				@Override
				void processValue(Object valueToProcess) {
				}

				@Override
				void finalizeProcessing() {
				}
			};
			fail("ProcessingTriggerSize not be higher than MaxSize");
		} catch (IllegalArgumentException e) {
		}
	}

	@Test
	public void testIgnoreItemsAboveLimit() throws InterruptedException {
		configuration.setProcessingTriggerSize(2);
		configuration.setMaxSize(2);
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
		for (int i = 0; i < 10; i++) {
			bufferedProcessingThread.put(new Object(), new Object());
		}
		Thread.sleep(10);
		assertEquals(2, processCount);
	}

	private abstract class GiveTimeToProcessAfterInsertBufferedProcessingThread<K, V> extends
			BufferedProcessingThread<K, V> {

		private static final long SLEEP_AFTER_INSERT = 10;

		public GiveTimeToProcessAfterInsertBufferedProcessingThread() {
			super(configuration);
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
