package nl.clearj.commons.thread;

import static org.junit.Assert.assertEquals;

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
	public void testProcessingOfItemsInOneGo() throws InterruptedException {
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
	public void testBufferConcurrentReadWrite() throws InterruptedException {
		BufferedProcessingThread<Object, Object> bufferedProcessingThread = new BufferedProcessingThread<Object, Object>(
				"test thread", true) {

			@Override
			void initializeProcessing() {
			}

			@Override
			void processValue(Object valueToProcess) {
				synchronized (this) {
					processCount++;
				}
			}

			@Override
			void finalizeProcessing() {
				if (processCount == 100) {
					interrupt();
				}
			}
		};
		bufferedProcessingThread.setProcessingTriggerSize(0);
		for (int i = 1; i <= 100; i++) {
			bufferedProcessingThread.put(new Object(), new Object());
		}
		bufferedProcessingThread.join();
		assertEquals(100, processCount);
	}
}
