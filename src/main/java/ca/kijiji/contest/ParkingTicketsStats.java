package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParkingTicketsStats {

	// 23-bit indices (8M possible entries)
	static final int BITS = 23;
	static final int UNUSED_BITS = 32 - BITS;
	static final int SIZE = 1 << BITS;
	static final int MASK = SIZE - 1;
	static final String[] keys = new String[SIZE];
	static final AtomicIntegerArray vals = new AtomicIntegerArray(SIZE);
	static final byte[] data = new byte[4 * 1024 * 1024];

	static final Pattern namePattern = Pattern.compile("([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)");

	// 4-cores with HyperThreading sets nThreads = 8
	static final int nWorkers = Runtime.getRuntime().availableProcessors();

	// use small blocking queue size to limit read-ahead for higher cache hits
	static final ArrayBlockingQueue<int[]> byteArrayQueue = new ArrayBlockingQueue<int[]>(nWorkers * 3, false);
	static final int[] END_OF_WORK = new int[0];

	static volatile boolean wasrun;

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
    	printInterval("Pre-initialization");

    	if (wasrun) {
    		Arrays.fill(keys, 0);
	    	for (int i = 0; i < SIZE; i++) {
	    		vals.set(i, 0);
	    	}
    	}
    	else {
    		wasrun = true;
    	}

    	try {
			final int available = parkingTicketsStream.available();

			Thread[] workers = new Thread[nWorkers];
			for (int k = 0; k < nWorkers; k++) {
				workers[k] = new Thread("worker"+ k) {
					public void run() {
						worker();
					}
				};
			}

    		for (Thread t : workers) {
	    		t.start();
    		}

        	printInterval("Initialization");

    		int bytes_read = 0;
    		int read_start = 0;
    		int read_end = 0;
    		int block_start = 0;
    		int block_end = 0;
    		for (int read_amount = 1024 * 1024; (read_amount = parkingTicketsStream.read(data, read_start, read_amount)) > 0; ) {
    			bytes_read += read_amount;
    			read_end = read_start + read_amount;
    			block_start = block_end;
    			block_end = read_end;

    			// don't offer the first (header) row
    			if (block_start == 0) {
    				printInterval("First read");
    				while (data[block_start++] != '\n') {}
    			}

    			if (bytes_read < available) {
    				while (data[--block_end] != '\n') {}
        			block_end++;
    			}

    			// subdivide block to minimize latency and improve work balancing
    			int sub_end = block_start;
    			int sub_start;
    			for (int k = 1; k <= nWorkers; k++) {
    				sub_start = sub_end;
    				sub_end = block_start + (block_end - block_start) * k / nWorkers;
    				if (k < nWorkers) {
    					while (data[--sub_end] != '\n') {}
    					sub_end++;
    				}
        			for (;;) {
    					try {
    						byteArrayQueue.put(new int[] {sub_start, sub_end});
    						break;
    					}
    					catch (InterruptedException e) {
    						e.printStackTrace();
    					}
        			}
    			}

    			if (available - bytes_read < read_amount) {
    				read_amount = available - bytes_read;
    			}

    			read_start = read_end;
    			if (read_start + read_amount > data.length) {
    				read_start = read_end = read_end - block_end;
    				System.arraycopy(data, block_end, data, 0, read_start);
    				block_start = block_end = 0;
    			}
    		}

    		for (int t = 0; t < nWorkers; t++) {
    			try {
					byteArrayQueue.put(END_OF_WORK);
				}
    			catch (InterruptedException e) {
					e.printStackTrace();
				}
    		}

	    	for (Thread t: workers) {
	    		try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	    	}
    	}
    	catch (IOException e) {
			e.printStackTrace();
		}

    	final SortedMap<String, Integer> sorted = new TreeMap<String, Integer>(new Comparator<String>() {
			public int compare(String o1, String o2) {
				int c = get(o2) - get(o1);
				if (c != 0) return c;
				return o2.compareTo(o1);
			}});

    	Thread[] threads = new Thread[2];
    	for (int t = 0; t < 2; t++) {
    		final int start = t == 0 ? 0 : SIZE / 2;
    		final int end = t == 0 ? SIZE / 2 : SIZE;

        	threads[t] = new Thread(null, null, "gather"+ t, 2048) {
        		public void run() {
        	    	for (int i = start; i < end; i++) {
        	    		int v = vals.get(i);
        	    		if (v != 0) {
        	    			synchronized (sorted) {
        		    			sorted.put(keys[i], v);
        	    			}
        	    		}
        	    	}
        		}
        	};
        	threads[t].start();
    	}

    	for (Thread thread : threads) {
	    	try { thread.join(); } catch (InterruptedException e) {}
    	}
        return sorted;
    }

    /**
     * worker parallel worker takes blocks of bytes read and processes them
     */
    static final void worker() {
		Matcher nameMatcher = namePattern.matcher("");

		for (;;) {
			int[] block_start_end;
			for (;;) {
				try {
					block_start_end = byteArrayQueue.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
					break;
				}
				catch (InterruptedException e) {
					e.printStackTrace();
					continue;
				}
			}

			if (block_start_end == END_OF_WORK) {
				break;
			}
			final int block_start = block_start_end[0];
			final int block_end = block_start_end[1];

			// process block as fields
			// save fields 4 (set_fine_amount) and 7 (location2)
			int start = block_start;
			int column = 0;
			int fine = 0;
			String location = null;
			// process block
			while (start < block_end) {
				int end = start;
				while (end < block_end && data[end] != ',' && data[end] != '\n') { end++; }

				if (column == 4) {
		    		final String set_fine_amount = new String(data, start, end - start);
		    		try {
			    		fine = Integer.parseInt(set_fine_amount);
		    		}
		    		catch (final NumberFormatException e) {
		    			System.out.print(e.getClass().getSimpleName() +": "+ set_fine_amount);
		    		}
				}
				else if (column == 7) {
					if (fine > 0) {
						location = new String(data, start, end - start);

			    		nameMatcher.reset(location);
			    		if (nameMatcher.find()) {
			    			final String name = nameMatcher.group();
			    			add(name, fine);
		    			}
					}
				}

				column++;
				if (end < block_end && data[end] == '\n') {
					fine = 0;
					location = null;
					column = 0;
				}
				start = end + 1;
			}
		}
    }

	public static int hash(String k) {
		int h = 0;
		for (char c : k.toCharArray()) {
			if (h < 0 || h > MASK) {
				h = (h & MASK) ^ (h >>> BITS);
			}
			int i = (c == ' ') ? 0 : (int)c & 0x00FF - 64;
			h = h * 47 + i;
		}
		return h & MASK;
	}

	public static void add(final String k, final int d) {
		int i = hash(k);

		if (vals.getAndAdd(i, d) == 0) {
			keys[i] = k;
		}
		// use code below instead of if() above to show hash collisions
//		if (vals.getAndAdd(i, d) != 0) {
//			synchronized (keys) {
//				String k0 = keys[i];
//				if (!k.equals(k0)) {
//					println("Key hash clash: first "+ k0 +" and "+ k);
//				}
//			}
//		}
//		else {
//			keys[i] = k;
//		}
	}

	public static int get(final String k) {
		int i = hash(k);
		return vals.get(i);
	}

    static volatile long lastTime = System.currentTimeMillis();

    public static void printInterval(String name) {
    	long time = System.currentTimeMillis();
    	println(time, name +": "+ (time - lastTime) +" ms");
    	lastTime = time;
    }

    public static void printElement(String key, Map<String, Integer> streets) {
    	println(key +": $"+ streets.get(key));
    }

    public static void printProperty(String name) {
		println(name +": "+ System.getProperty(name));
    }

    public static void println(long time, String line) {
    	println(time%10000 +" "+ line);
    }

    public static void println(String line) {
    	System.out.println(line);
    }
}