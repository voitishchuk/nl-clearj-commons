package nl.clearj.commons.thread;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BufferedProcessingThreadTest {

	private BufferedProcessingThread<Object, Object> bufferedProcessingThread;
	private int processedCount;

	@Before
	public void setUp() throws Exception {
		processedCount = 0;
		bufferedProcessingThread = new BufferedProcessingThread<Object, Object>("test thread", true) {

			@Override
			void processValue(Object valueToProcess) {
				Assert.assertNotNull(valueToProcess);
				synchronized (bufferedProcessingThread) {
					processedCount++;
				}
			}

			@Override
			void initializeProcessing() {
			}

			@Override
			void finalizeProcessing() {
			}
		};
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws InterruptedException {
		long start = System.currentTimeMillis();
		for (int i = 1; i < (1000 * 10000); i++) {
			bufferedProcessingThread.put(new Object(), new Object());
		}
		System.out.println(processedCount);
		System.out.println(bufferedProcessingThread.size());
		System.out.println(processedCount + bufferedProcessingThread.size());
		System.out.println((System.currentTimeMillis() - start) + "ms");
	}
}
