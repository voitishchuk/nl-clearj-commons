package nl.clearj.commons.thread;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BufferedProcessingThreadTest {

	int processedCount;

	@Before
	public void setUp() throws Exception {
		processedCount = 0;
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testBufferConcurrentReadWrite() throws InterruptedException {
		BufferedProcessingThread<Object, Object> bufferedProcessingThread = new BufferedProcessingThread<Object, Object>(
				"test thread", true) {

			@Override
			void processValue(Object valueToProcess) {
				synchronized (this) {
					processedCount++;
				}
			}

			@Override
			void initializeProcessing() {
			}

			@Override
			void finalizeProcessing() {
				if (processedCount == 100) {
					interrupt();
				}
			}
		};
		bufferedProcessingThread.setProcessingTriggerSize(0);
		for (int i = 1; i <= 100; i++) {
			bufferedProcessingThread.put(new Object(), new Object());
		}
		bufferedProcessingThread.join();
		assertEquals(100, processedCount);
	}
}
