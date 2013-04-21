package nl.clearj.demo.concurrency;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class PrimitivesUnsafety {

	public static final Random random = new Random();

	public final Set<Long> pool;

	private volatile long volatLong;
	private long norLong;

	public PrimitivesUnsafety(int poolSize) {
		pool = createPool(poolSize);
		volatLong = norLong = pool.iterator().next();
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
						norLong = newValue;
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
					if (!pool.contains(norLong)) {
						System.out.println("unexpected normal value: " + norLong);
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
