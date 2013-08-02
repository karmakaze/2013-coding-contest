package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compact open-addressing hash table with mechanical sympathy.
 *
 * @author Keith Kim
 */
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
public class ParkingTicketsStats3 {

	static volatile byte[] data;

	static final Pattern namePattern = Pattern.compile("([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)");

    static final int nWorkers = 4;
	static final ArrayBlockingQueue<Long> thequeue = new ArrayBlockingQueue<>(2 * nWorkers, false);
    static final OpenStringIntHashMap themap = new OpenStringIntHashMap(12800); // 8770

    public static SortedMap<String, Integer> sortStreetsByProfitability(final InputStream parkingTicketsStream) {
    	printInterval("Pre-entry initialization");
    	//printProperty("os.arch");

    	try {
			final int available = parkingTicketsStream.available();
    		println(System.currentTimeMillis(), "Bytes available: "+ available);

    		data = new byte[available];
    		final byte[] data = ParkingTicketsStats3.data;

	        final Worker workers[] = new Worker[nWorkers];

	        for (int t = 0; t < nWorkers; t++) {
	//        	maps[t] = new TObjectIntHashMap(15000);
	        	workers[t] = new Worker(thequeue, themap);
	        	workers[t].start();
	        }

    		int read_end = 0;
    		int block_start = 0;
    		int block_end = 0;
    		for (int read_amount = 64 * 1024; (read_amount = parkingTicketsStream.read(data, read_end, read_amount)) > 0; ) {
    			read_end += read_amount;
    			block_start = block_end;
    			block_end = read_end;
    			if (read_end < available) {
    				while (data[--block_end] != '\n') {}
        			block_end++;
    			}

    			// don't offer the first (header) row
    			if (block_start == 0) {
    		    	printInterval("Local initialization: read first "+ read_end +" bytes");

    				while (data[block_start++] != '\n') {};
    			}

    			int sub_end = block_start;
    			for (int k = 0; k < nWorkers; k++) {
    				int sub_start = sub_end;
    				sub_end = block_start + (block_end - block_start) * (k + 1) / nWorkers;
        			if (k < nWorkers - 1 || read_end < available) {
        				while (data[--sub_end] != '\n') {}
            			sub_end++;
        			}

    				final long ij = (long)sub_start << 32 | (long)sub_end & 0x00ffffffff;
    				try {
    					thequeue.put(ij);
    				}
    				catch (final InterruptedException e) {
    					e.printStackTrace();
    				}
    			}

    			if (available - read_end < read_amount) {
    				read_amount = available - read_end;
    			}
    		}

	    	printInterval("Local initialization: read remaining of "+ read_end +" total bytes");

	        for (int t = 0; t < nWorkers; t++) {
				try {
					thequeue.put(0L);
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
	        }

	        for (int t = 0; t < nWorkers; t++) {
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
		private final OpenStringIntHashMap map;
		private final Matcher nameMatcher = namePattern.matcher("");
		public volatile long Pad1, Pad2, Pad3, Pad4, Pad5, Pad6, Pad7;

		public Worker(final ArrayBlockingQueue<Long> queue, final OpenStringIntHashMap map) {
			this.queue = queue;
			this.map = map;
			pad7 = pad6 = pad5 = pad4 = pad3 = pad2 = pad1 = 7;
			Pad1 = Pad2 = Pad3 = Pad4 = Pad5 = Pad6 = Pad7 = 7;
		}

        public final void run() {
			// local access faster than volatile fields
			final byte[] data = ParkingTicketsStats3.data;
			final char[] keybuf = new char[256];

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

				final int block_start = (int) (block_start_end >>> 32);
    			final int block_end = (int) block_start_end;

    			// process block as fields
    			// save fields 4 (set_fine_amount) and 7 (location2)
    			int start = block_start;
    			int column = 0;
    			int fine = 0;
    			String location = null;
    			// process block
    			while (start < block_end) {
    				// find a field data[start, end)
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
				    			map.adjustOrPutValue(name, fine, keybuf);
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
		   	printInterval("Worker completed");
        }
    }

	@SuppressWarnings("serial")
	public static final class MergeMap extends TreeMap<String, Integer> {
		public MergeMap() {
			super(new Comparator<String>() {
				public int compare(final String o1, final String o2) {
					final int c = getMerged(o2) - getMerged(o1);
					if (c != 0) return c;
					return o2.compareTo(o1);
				}
			});

			themap.putAllTo(this);
		}

		private static Integer getMerged(final Object key) {
			int v = 0;
			v = themap.get((String) key);
//			for (final TObjectIntHashMap<String> map : maps) {
//				v += map.get(key);
//			}
			return v;
		}
	}
}