package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class ParkingTicketsStats {

	// 23-bit indices (4M possible entries)
	static final int BITS = 23;
	static final int UNUSED_BITS = 32 - BITS;
	static final int SIZE = 1 << BITS;
	static final int MASK = SIZE - 1;
	static final AtomicReferenceArray<String> keys = new AtomicReferenceArray<String>(SIZE);
	static final AtomicIntegerArray vals = new AtomicIntegerArray(SIZE);
	static volatile byte[] data;

	static final String name = "([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)";
	static final Pattern namePattern = Pattern.compile(name);

	static volatile int available;

	static final ArrayBlockingQueue<Long> byteArrayQueue = new ArrayBlockingQueue<Long>(1024, true);
	static final AtomicInteger workItems = new AtomicInteger();

    public static SortedMap<String, Integer> sortStreetsByProfitability(final InputStream parkingTicketsStream) {
    	for (int i = 0; i < SIZE; i++) {
	    	keys.set(i, null);
	    	vals.set(i, 0);
    	}

    	printInterval("Pre-entry initialization");
/*
		printProperty("os.arch");
    	println("InputStream is "+ parkingTicketsStream);
    	if (parkingTicketsStream instanceof BufferedInputStream) {
    		BufferedInputStream bis = (BufferedInputStream) parkingTicketsStream;
    	}
*/
    	for (int i = 0; i < SIZE; i++) { keys.set(i, null); vals.set(i, 0); }

    	try {
			available = parkingTicketsStream.available();
    		println(System.currentTimeMillis(), "Bytes available: "+ available);

			final ThreadGroup group = new ThreadGroup("workers");
			final Runnable runnable = new Runnable() {
				public void run() {
					worker();
				}};
			final int n = 4;
			final Thread[] threads = new Thread[n];
			for (int k = 0; k < n; k++) {
				threads[k] = new Thread(group, runnable, Integer.toString(k), 1024);
			}

    		data = new byte[available];

    		workItems.incrementAndGet(); // 1 for first producer task

    		for (final Thread t : threads) {
	    		t.start();
    		}

    		int a = 0;
    		int i = 0;
    		int j = 0;
    		for (int c = 4 * 1024 * 1024; (c = parkingTicketsStream.read(data, a, c)) > 0; ) {
    			a += c;
    			workItems.incrementAndGet();
    			i = j;
    			j = a;
    			if (a < available) {
    				while (data[--j] != '\n') {}
        			j++;
    			}

    			// don't offer the first (header) row
    			if (i == 0) {
    		    	printInterval("Local initialization: read first "+ a +" bytes");

    				while (data[i++] != '\n') {};
    			}

    			final long ij = (long)i << 32 | (long)j & 0x0ffffffffL;
    			try {
					while (!byteArrayQueue.offer(ij, 1, TimeUnit.SECONDS)) {}
				} catch (final InterruptedException e) {
					e.printStackTrace();
				}

    			if (available - a < c) {
    				c = available - a;
    			}
    		}

    		workItems.decrementAndGet();

	    	printInterval("Local initialization: read remaining of "+ a +" total bytes");

	    	for (final Thread t: threads) {
	    		try {
					t.join();
				} catch (final InterruptedException e) {
					e.printStackTrace();
				}
	    	}

	    	printInterval("All worker threads completed");
    	}
    	catch (final IOException e) {
			e.printStackTrace();
		}

    	printInterval("Read and summed");

//    	println("Size: "+ streets.size());

    	final SortedMap<String, Integer> sorted = new TreeMap<String, Integer>(new Comparator<String>() {
			public int compare(final String o1, final String o2) {
				final int c = get(o2) - get(o1);
				if (c != 0) return c;
				return o2.compareTo(o1);
			}});

    	final int B = SIZE / 2;
//    	final int C = B + B + 1;

    	final Thread t0 = new Thread(null, null, "g0", 1024) {
    		public void run() {
    	    	for (int i = 0; i < B; i++) {
    	    		final int v = vals.get(i);
    	    		if (v != 0) {
    	    			synchronized (sorted) {
    		    			sorted.put(keys.get(i), v);
    	    			}
    	    		}
    	    	}
    		}
    	};
    	t0.start();

    	final Thread t1 = new Thread(null, null, "g1", 1024) {
    		public void run() {
    	    	for (int i = B; i < SIZE; i++) {
    	    		final int v = vals.get(i);
    	    		if (v != 0) {
    	    			synchronized (sorted) {
    		    			sorted.put(keys.get(i), v);
    	    			}
    	    		}
    	    	}
    		}
    	};
    	t1.start();

    	try { t0.join(); } catch (final InterruptedException e) {}
    	try { t1.join(); } catch (final InterruptedException e) {}
//    	try { t2.join(); } catch (InterruptedException e) {}

    	printInterval("Populated TreeSet");

        return sorted;
    }

    /**
     * worker parallel worker takes blocks of bytes read and processes them
     */
    static final void worker() {
		try {
		//	String threadName = Thread.currentThread().getName();
			final Matcher nameMatcher = namePattern.matcher("");

			// local access faster than volatile fields
			final byte[] data = ParkingTicketsStats.data;

			final ArrayList<String> parts = new ArrayList<>();

			int work = 0;
			do {
				final Long ij = byteArrayQueue.poll(5, TimeUnit.MILLISECONDS);
				if (ij != null) {
					int i = (int) (ij >>> 32);
					final int j = (int) (long) ij;
				//	println("Thread ["+ threadName +"] processing block("+ i +", "+ j +")");

					// process block
					for (int m; i < j; i = m) {
						// process a line
						m = i;
						while (m < j && data[m++] != (byte)'\n') {}

						parts.clear();
						int k;
						int c = 0;
						do {
							k = i;
							while (k < m && data[k] != ',' && data[k] != '\n') { k++; }
							if (c == 4 || c == 7) {
								parts.add(new String(data, i, k - i));
							} else {
								parts.add(null);
							}
							c++;
							i = k + 1;
						} while (i < m);

			    		try {
	//			    		String tag_number_masked = parts[0];
	//			    		String date_of_infraction = parts[1];
	//			    		String infraction_code = parts[2];
	//			    		String infraction_description = parts[3];
				    		final String sfa = parts.get(4);
				    		Integer set_fine_amount = 0;
				    		try {
					    		set_fine_amount = Integer.parseInt(sfa);
				    		}
				    		catch (final NumberFormatException e) {
				    			System.out.print(e.getClass().getSimpleName() +": "+ sfa);
				    		}
	//			    		String time_of_infraction = parts[5];
	//			    		String location1 = parts[6];
				    		final String location2 = parts.get(7);
	//			    		String location3 = parts[8];
	//			    		String location4 = parts[9];
				    		nameMatcher.reset(location2);
				    		if (nameMatcher.find()) {
				    			final String l = nameMatcher.group();
	//		    			streetMatcher.reset(location2);
	//		    			if (streetMatcher.find()) {
	//		    				String l = streetMatcher.group(2);
			    			/*
					    	//	l = l.replaceAll("[0-9]+", "");
					    		l = l.replaceAll("[^A-Z]+ ", "");
					    		l = l.replaceAll(" (N|NORTH|S|SOUTH|W|WEST|E|EAST)$", "");
					    		l = l.replaceAll(" (AV|AVE|AVENUE|BLVD|CRES|COURT|CRT|DR|RD|ST|STR|STREET|WAY)$", "");
					    	//	l = l.replaceAll("^(A|M) ", "");
					    		l = l.replaceAll("(^| )(PARKING) .*$", "");
					    		l = l.trim();
				    		*/
	//				    		String province = parts[10];
				    			add(l, set_fine_amount);

	//			    			if (!l.equals("KING") && (location2.indexOf(" KING ") >= 0 || location2.endsWith(" KING"))) {
	//			    				println(l +" <- "+ location2);
	//			    			}
			    			}
			    			else {
			    				if (location2.indexOf("KING") >= 0 && location2.indexOf("PARKING") == -1) {
				    				println(""+ location2);
			    				}
			    			}
			    		}
			    		catch (final ArrayIndexOutOfBoundsException e) {
			    			println(e.getClass().getSimpleName() +": "+ parts);
			    			e.printStackTrace();
			    		}
					}
					work = workItems.decrementAndGet();
				}
				else {
					work = workItems.get();
				}
			//	println("Thread ["+ threadName +"] work remaining "+ work +" queued="+ byteArrayQueue.size());
			} while (work > 0);

		//	println(System.currentTimeMillis(), "Thread ["+ threadName +"] ending normally");
		}
		catch (final InterruptedException e) {
			e.printStackTrace();
		}
    }

    public static int hashfact = 47;
    public static AtomicInteger clashes = new AtomicInteger(0);

    public static volatile HashFunction murmur3 = Hashing.murmur3_32();
    public static void setHashSeed(int seed) {
    	murmur3 = Hashing.murmur3_32(seed);
    }

	public static int hash(final String k) {
		int h = 0;
		try {
			h = murmur3.hashBytes(k.replace(" ", "").getBytes("UTF-8")).asInt();
		} catch (UnsupportedEncodingException e) {}
/*
		int h = 0;
		try {
			for (final byte b : k.getBytes("UTF-8")) {
				if (b != ' ') {
					h <<= 4;
					final int c = (b == ' ') ? 0 : (int)b & 0x00FF - 65;
					h += c;
					h = (h ^ (h >>> BITS)) & MASK;
				}
			}
		}
		catch (final UnsupportedEncodingException e) {}
*/
		return h & MASK;
	}

	public static int murmurHash2(byte[] data, int seed) {
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        int m = 0x5bd1e995;
        int r = 24;

        // Initialize the hash to a 'random' value
        int len = data.length;
        int h = seed ^ len;

        int i = 0;
        while (len  >= 4) {
            int k = data[i + 0] & 0xFF;
            k |= (data[i + 1] & 0xFF) << 8;
            k |= (data[i + 2] & 0xFF) << 16;
            k |= (data[i + 3] & 0xFF) << 24;

            k *= m;
            k ^= k >>> r;
            k *= m;

            h *= m;
            h ^= k;

            i += 4;
            len -= 4;
        }

        switch (len) {
        case 3: h ^= (data[i + 2] & 0xFF) << 16;
        case 2: h ^= (data[i + 1] & 0xFF) << 8;
        case 1: h ^= (data[i + 0] & 0xFF);
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

	public static void add(final String k, final int d) {
		final int i = hash(k);
		vals.addAndGet(i, d);

//		keys.compareAndSet(i, null, k);
		final String k0 = keys.getAndSet(i, k);
		if (k0 != null && !k0.replace(" ", "").equals(k.replace(" ", ""))) {
//			println("Key hash clash: first "+ k0 +" and "+ k);
			clashes.incrementAndGet();
		}
	}
	public static int get(final String k) {
		final int i = hash(k);
		return vals.get(i);
	}

    static volatile long lastTime = System.currentTimeMillis();

    public static void printInterval(final String name) {
    	final long time = System.currentTimeMillis();
    	println(time, name +": "+ (time - lastTime) +" ms");
    	lastTime = time;
    }

    public static void printElement(final String key, final Map<String, Integer> streets) {
    	println(key +": $"+ streets.get(key));
    }

    public static void printProperty(final String name) {
		println(name +": "+ System.getProperty(name));
    }

    public static void println(final long time, final String line) {
    	println(time%10000 +" "+ line);
    }

    public static void println(final String line) {
    	System.out.println(line);
    }
}