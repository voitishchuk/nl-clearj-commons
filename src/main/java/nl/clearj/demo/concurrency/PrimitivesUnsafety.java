package nl.clearj.demo.concurrency;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Demonstrates that concurrent access to primitives is not safe. According to
 * the Java Language Specification, access to primitives that do not fit in
 * native word width is not required to be synchronized. This means access to
 * 64-bits primitives such as long and double is not synchronized on 32-bit CPU.
 * 
 * @author Valeriy Voitishchuk
 */
public class PrimitivesUnsafety {

	public static final Random random = new Random();

	public final Set<Long> pool;

	private volatile long volatLong;
	private long normLong;

	public PrimitivesUnsafety(int poolSize) {
		pool = createPool(poolSize);
		volatLong = normLong = pool.iterator().next();
	}

	private static Set<Long> createPool(int poolSize) {
		System.out.println("generating pool of size " + poolSize);
		Set<Long> result = new HashSet<Long>(poolSize);
		while (result.size() < poolSize) {
			result.add(random.nextLong());
		}
		System.out.println("pool is ready");
		return Collections.unmodifiableSet(result);
	}

	public static void main(String[] args) {
		System.out.println("on 32-bit java you will see unexpected normal values");
		new PrimitivesUnsafety(100).start();
	}

	private void start() {
		new StartedNonDaemonThread() {

			@Override
			public void run() {
				while (true) {
					for (long newValue : pool) {
						volatLong = newValue;
					}
				}
			}
		};

		new StartedNonDaemonThread() {

			@Override
			public void run() {
				while (true) {
					for (long newValue : pool) {
						normLong = newValue;
					}
				}
			}
		};

		new StartedNonDaemonThread() {

			@Override
			public void run() {
				while (true) {
					if (!pool.contains(volatLong)) {
						System.out.println("unexpected volatile value: " + volatLong);
					}
					if (!pool.contains(normLong)) {
						System.out.println("unexpected normal value: " + normLong);
					}
				}
			}
		};
	}

	private static class StartedNonDaemonThread extends Thread {

		public StartedNonDaemonThread() {
			setDaemon(false);
			start();
		}
	}

}
