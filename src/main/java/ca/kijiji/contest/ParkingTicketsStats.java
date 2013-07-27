package ca.kijiji.contest;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pipeline decomposition:
 * - InputStream
 * - byte[] blocks
 * - Strings (set_fine_amount, location2)
 * - parse fine, location
 * - hash location
 */
public class ParkingTicketsStats {

	// 24-bit indices (16M possible entries)
	static final int BITS = 24;
	static final int UNUSED_BITS = 32 - BITS;
	static final int SIZE = 1 << BITS;
	static final int MASK = SIZE - 1;

	static volatile byte[] data;

	static final Pattern namePattern = Pattern.compile("([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)");

    static final int nWorkers = 4;
	static final TObjectIntHashMap<String>[] maps = new TObjectIntHashMap[nWorkers];
//	static final TObjectIntHashMap<String> themap = new TObjectIntHashMap(20000);

    public static SortedMap<String, Integer> sortStreetsByProfitability(final InputStream parkingTicketsStream) {
    	printInterval("Pre-entry initialization");
    	//printProperty("os.arch");

    	try {
			final int available = parkingTicketsStream.available();
    		println(System.currentTimeMillis(), "Bytes available: "+ available);

    		data = new byte[available];
    		final byte[] data = ParkingTicketsStats.data;

	        final ArrayBlockingQueue<Long> queues[] = new ArrayBlockingQueue[nWorkers];
	        final Worker workers[] = new Worker[nWorkers];

	        for (int t = 0; t < nWorkers; t++) {
	        	queues[t] = new ArrayBlockingQueue<>(512);
	        	maps[t] = new TObjectIntHashMap(15000);
	        	workers[t] = new Worker(queues[t], maps[t]);
	        	workers[t].start();
	        }

    		int a = 0;
    		int i = 0;
    		int j = 0;
    		int t = 0;
    		for (int c = 4 * 1024 * 1024; (c = parkingTicketsStream.read(data, a, c)) > 0; ) {
    			a += c;
    			i = j;
    			j = a;

    			// don't offer the first (header) row
    			if (i == 0) {
    		    	printInterval("Local initialization: read first "+ a +" bytes");

    				while (data[i++] != '\n') {};
    			}

    			if (a < available) {
    				while (data[--j] != '\n') {}
        			j++;
    			}

				final long ij = (long)i << 32 | (long)j & 0x00ffffffff;
				try {
					queues[t].put(ij);
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
    			t = (++t) % nWorkers;

    			if (available - a < c) {
    				c = available - a;
    			}
    		}

	    	printInterval("Local initialization: read remaining of "+ a +" total bytes");

	        for (t = 0; t < nWorkers; t++) {
				try {
					queues[t].put(0L);
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
	        }

	        for (t = 0; t < nWorkers; t++) {
		        try {
					workers[t].join();
				}
		        catch (final InterruptedException e) {
					e.printStackTrace();
				}
	        }

	    	printInterval("All worker threads completed");
    	}
    	catch (final IOException e) {
			e.printStackTrace();
		}

    	final SortedMap<String, Integer> sorted = new MergeMap();

    	printInterval("Maps merged");

        return sorted;
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

    public final static class Worker extends Thread {
    	public volatile long pad7, pad6, pad5, pad4, pad3, pad2, pad1;
		private final ArrayBlockingQueue<Long> queue;

		private final TObjectIntHashMap<String> map;
		private final Matcher nameMatcher = namePattern.matcher("");
		public volatile long Pad1, Pad2, Pad3, Pad4, Pad5, Pad6, Pad7;

		public Worker(final ArrayBlockingQueue<Long> queue, final TObjectIntHashMap<String> map) {
			this.queue = queue;
			this.map = map;
			pad7 = pad6 = pad5 = pad4 = pad3 = pad2 = pad1 = 7;
			Pad1 = Pad2 = Pad3 = Pad4 = Pad5 = Pad6 = Pad7 = 7;
		}

        public final void run() {
			// local access faster than volatile fields
			final byte[] data = ParkingTicketsStats.data;
			final TIntArrayList fines = new TIntArrayList(819200);
			final ArrayList<String> locations = new ArrayList<>(819200);

			for (;;) {
				final long block_start_end;
				try {
					block_start_end = queue.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
					continue;
				}

				if (block_start_end == 0) {
					break;
				}

				final int block_start = (int)(block_start_end >>> 32);
    			final int block_end = (int)block_start_end;

				// process block as fields
				// save fields 4 (set_fine_amount) and 7 (location2)
    			int start = block_start;
				int column = 0;
	    		int fine = 0;
	    		String location = null;
				while (start < block_end) {
					// find a field data[start, end)
					int end = start;
					while (end < block_end && data[end] != ',' && data[end] != '\n') { end++; }

					if (column == 4) {
			    		final String set_fine_amount = new String(data, start, end - start);

		    			// parse fine
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
				    		fines.add(fine);
				    		locations.add(location);
						}
					}
					column++;
					if (end < block_end & data[end] == '\n') {
						column = 0;
					}
					start = end + 1;
				}
			}

			println("fines/locations: size="+ fines.size() +" "+ locations.size());

			int i = 0;
			for (final String location : locations) {
	    		// parse location
	    		nameMatcher.reset(location);
	    		if (nameMatcher.find()) {
	    			final String name = nameMatcher.group();
    				final int fine = fines.get(i);

	    			map.adjustOrPutValue(name, fine, fine);
				}
	    		i++;
			}
        }
    }

	@SuppressWarnings("serial")
	public static final class MergeMap extends TreeMap<String, Integer> {
		public MergeMap() {
			super(new Comparator<String>() {
				// order by value descending, name ascending
				public int compare(final String a, final String b) {
					final int c = getMerged(b) - getMerged(a);
					if (c != 0) return c;
					return b.compareTo(a);
				}
			});

			for (final TObjectIntHashMap<String> map : maps) {
				map.forEachEntry(new TObjectIntProcedure<String>() {
					public boolean execute(final String k, final int v) {
						final Integer i = get(k);
						if (i == null) {
							put(k, v);
						} else {
							put(k,  i+v);
						}
						return true;
					}});
			}
		}

		private static int getMerged(final Object key) {
			int v = 0;
//			v = themap.get(key);
			for (final TObjectIntHashMap<String> map : maps) {
				v += map.get(key);
			}
			return v;
		}
	}
}