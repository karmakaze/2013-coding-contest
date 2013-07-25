package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * Pipeline decomposition:
 * - InputStream
 * - byte[] blocks
 * - lines
 * - fields (e.g. fine, location)
 * - parse fine, location
 * - hash location
 *
 */
public class ParkingTicketsStats {

	// 24-bit indices (16M possible entries)
	static final int BITS = 24;
	static final int UNUSED_BITS = 32 - BITS;
	static final int SIZE = 1 << BITS;
	static final int MASK = SIZE - 1;

	static final AtomicReferenceArray<String> keys = new AtomicReferenceArray<String>(SIZE);
	static final AtomicIntegerArray vals = new AtomicIntegerArray(SIZE);

	static volatile byte[] data;

	static final String name = "([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)";
	static final Pattern namePattern = Pattern.compile(name);

    public static SortedMap<String, Integer> sortStreetsByProfitability(final InputStream parkingTicketsStream) {
    	printInterval("Pre-entry initialization");
    	//printProperty("os.arch");

    	try {
			final int available = parkingTicketsStream.available();
    		println(System.currentTimeMillis(), "Bytes available: "+ available);

	        final ExecutorService exec = Executors.newCachedThreadPool();

	        final int nDisruptors = 8;
	        final Disruptor<StartEndEvent> blockDisruptors[] = new Disruptor[nDisruptors];
	        final BlockEventHandler blockHandlers[] = new BlockEventHandler[nDisruptors];
	        final RingBuffer<StartEndEvent> blockRingBuffers[] = new RingBuffer[nDisruptors];

	        for (int d = 0; d < nDisruptors; d++) {
		        blockDisruptors[d]= new Disruptor<StartEndEvent>(StartEndEvent.EVENT_FACTORY, 128, exec);
		        blockHandlers[d] = new BlockEventHandler();
		        blockDisruptors[d].handleEventsWith(blockHandlers[d]);
		        blockRingBuffers[d] = blockDisruptors[d].start();
	        }

    		data = new byte[available];
    		final byte[] data = ParkingTicketsStats.data;

    		int a = 0;
    		int i = 0;
    		int j = 0;
    		int disruptor = 0;
    		for (int c = 1 * 1024 * 1024; (c = parkingTicketsStream.read(data, a, c)) > 0; ) {
    			a += c;
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

    			int start;
    			int end = i;
    			for (int k = 1; k <= 64; k++) {
    				start = end;
    				if (k == 64) {
    					end = j;
    				}
    				else {
    					end = i + (j - i) * k / 64;
        				while (data[--end] != '\n') {}
    				}
		            // Two phase commit. Grab one of the slots
		            final long seq = blockRingBuffers[disruptor].next();
		            final StartEndEvent valueEvent = blockRingBuffers[disruptor].get(seq);
		            valueEvent.start = start;
		            valueEvent.end = end;
		            blockRingBuffers[disruptor].publish(seq);
    			}

    			if (available - a < c) {
    				c = available - a;
    			}

    			disruptor = (disruptor + 1) % nDisruptors;
    		}

	    	printInterval("Local initialization: read remaining of "+ a +" total bytes");

	        for (int d = 0; d < nDisruptors; d++) {
		        blockDisruptors[d].shutdown();
	        }
	        exec.shutdown();

	    	printInterval("All worker threads completed");
    	}
    	catch (final IOException e) {
			e.printStackTrace();
		}

/*
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

    	printInterval("Populated TreeSet");
*/
    	final SortedMap<String, Integer> sorted = new SelectSortedMap();

        return sorted;
    }

    /**
     * worker parallel worker takes blocks of bytes read and processes them
     */
    static final void worker(final ArrayBlockingQueue<Long> byteArrayReadQueue) {
    }

	public static int hash(final String k) {
		int h = 0;
		try {
			for (final byte b : k.getBytes("UTF-8")) {
				final int c = (b == ' ') ? 0 : (int)b & 0x00FF - 64;
				h = h * 71 + c;
				h = (h ^ (h >>> BITS)) & MASK;
			}
		}
		catch (final UnsupportedEncodingException e) {}

		return h;
	}

	public static void add(final String k, final int d) {
		final int i = hash(k);

		vals.addAndGet(i, d);

		if (keys.get(i) == null) {
			keys.set(i, k.intern());
		}

//		String k0 = keys[i];
//		if (k0 != null && !k0.equals(k)) {
//			println("Key hash clash: first "+ k0 +" and "+ k);
//		}
//		else {
//			keys.set(i, k);
//		}
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

    public final static class BlockEventHandler implements EventHandler<StartEndEvent> {
		private final Matcher nameMatcher = namePattern.matcher("");

        public void onEvent(final StartEndEvent event, final long sequence, final boolean endOfBatch) throws Exception {

			// local access faster than volatile fields
			final byte[] data = ParkingTicketsStats.data;

			int i = event.start;
			final int j = event.end;

			// process block
			for (int m; i < j; i = m) {
				// process a line
				m = i;
				while (m < j && data[m++] != (byte)'\n') {}

				// split out fields 4 (set_fine_amount) and 7 (location2)
	    		String set_fine_amount = "0";
	    		String location2 = "";

				int k;
				int c = 0;
				do {
					k = i;
					while (k < m && data[k] != ',' && data[k] != '\n') { k++; }
					if (c == 4) {
						set_fine_amount = new String(data, i, k - i);
					}
					else if (c == 7) {
			    		location2 = new String(data, i, k - i);
					}
					c++;
					i = k + 1;
				} while (i < m);

    			// parse fine
	    		int amount = 0;
	    		try {
		    		amount = Integer.parseInt(set_fine_amount);
	    		}
	    		catch (final NumberFormatException e) {
	    			System.out.print(e.getClass().getSimpleName() +": "+ set_fine_amount);
	    		}

	    		// parse location2
	    		nameMatcher.reset(location2);
	    		if (nameMatcher.find()) {
	    			add(nameMatcher.group(), amount);
				}
				else {
					// name could not be parsed, print out select subset of these errors
					if (location2.indexOf("KING") >= 0 && location2.indexOf("PARKING") == -1) {
	    				println(""+ location2);
					}
				}
			}
        }
    }

    /**
     * WARNING: This is a mutable object which will be recycled by the RingBuffer.
     * You must take a copy of data it holds before the framework recycles it.
     */
    public final static class StartEndEvent {
        int start;
        int end;

        public final static EventFactory<StartEndEvent> EVENT_FACTORY = new EventFactory<StartEndEvent>() {
            public StartEndEvent newInstance() {
                return new StartEndEvent();
            }
        };
    }

    protected final static class SelectSortedMap implements SortedMap<String, Integer> {
    	private final Comparator<String> comparator = new Comparator<String>() {
			public int compare(final String a, final String b) {
				final int c = get(b) - get(a);
				if (c != 0) return c;
				return a.compareTo(b);
			}};

		public Comparator<? super String> comparator() {
			return comparator;
		}

		public SortedMap<String, Integer> subMap(String fromKey, String toKey) {
			return null;
		}

		public SortedMap<String, Integer> headMap(String toKey) {
			return null;
		}

		public SortedMap<String, Integer> tailMap(String fromKey) {
			return null;
		}

		public String firstKey() {
			String maxKey = null;
			int maxVal = Integer.MIN_VALUE;
	    	for (int i = 0; i < SIZE; i++) {
	    		final int v = vals.get(i);
	    		if (v != 0 && v >= maxVal) {
	    			maxVal = v;
	    			maxKey = keys.get(i);
	    		}
	    	}
			return maxKey;
		}

		public String lastKey() {
			String minKey = null;
			int minVal = Integer.MAX_VALUE;
	    	for (int i = 0; i < SIZE; i++) {
	    		final int v = vals.get(i);
	    		if (v != 0 && v <= minVal) {
	    			minVal = v;
	    			minKey = keys.get(i);
	    		}
	    	}
			return minKey;
		}

		public Set<java.util.Map.Entry<String, Integer>> entrySet() {
			Set<java.util.Map.Entry<String, Integer>> entries = new TreeSet<Entry<String, Integer>>();
	    	for (int i = 0; i < SIZE; i++) {
	    		final int v = vals.get(i);
	    		if (v != 0) {
	    			entries.add(new AbstractMap.SimpleEntry<String, Integer>(keys.get(i), v));
	    		}
	    	}
			return entries;
		}

		public int size() {
			return 0;
		}

		public boolean isEmpty() {
			return false;
		}

		public boolean containsKey(Object key) {
			return false;
		}

		public boolean containsValue(Object value) {
			return false;
		}

		public Integer get(Object key) {
			if (key instanceof String) {
				String k = ((String) key).intern();

		    	for (int i = 0; i < SIZE; i++) {
		    		if (k == keys.get(i)) {
		    			return vals.get(i);
		    		}
		    	}
			}
			return null;
		}

		public Integer put(String key, Integer value) {
			return null;
		}

		public Integer remove(Object key) {
			return null;
		}

		public void putAll(Map<? extends String, ? extends Integer> m) {
		}

		public void clear() {
		}

		public Set<String> keySet() {
			return null;
		}

		public Collection<Integer> values() {
			return null;
		}
    }
}
