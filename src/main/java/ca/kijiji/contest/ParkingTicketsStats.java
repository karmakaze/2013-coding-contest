package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParkingTicketsStats {

	static final byte[] data = new byte[2 * 1024 * 1024];

	static final Pattern namePattern = Pattern.compile("([A-Z][A-Z][A-Z]+|ST [A-Z][A-Z][A-Z]+)");

	// 4-cores with HyperThreading sets nThreads = 8
	static final int nWorkers = 4; // Runtime.getRuntime().availableProcessors();

	// use small blocking queue size to limit read-ahead for higher cache hits
	static final ArrayBlockingQueue<int[]> byteArrayQueue = new ArrayBlockingQueue<int[]>(2 * nWorkers - 1, false);
	static final int[] END_OF_WORK = new int[0];

	static final ConcurrentHashMap<String, AtomicInteger> themap = new ConcurrentHashMap<>(12800, 0.8f, 8);

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
    	printInterval("Pre-initialization");

		Worker[] workers = new Worker[nWorkers];

    	try {
			final int available = parkingTicketsStream.available();

			for (int k = 0; k < nWorkers; k++) {
				workers[k] = new Worker("worker"+ k, byteArrayQueue, END_OF_WORK);
				workers[k].start();
			}

        	printInterval("Initialization");

    		int bytes_read = 0;
    		int read_end = 0;
    		int block_start = 0;
    		int block_end = 0;
    		for (int read_amount = 256 * 1024; (read_amount = parkingTicketsStream.read(data, read_end, read_amount)) > 0; ) {
    			bytes_read += read_amount;
    			read_end += read_amount;
    			block_start = block_end;
    			block_end = read_end;

    			// don't offer the first (header) row
    			if (bytes_read == read_amount) {
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
    				if (k < nWorkers) {
        				sub_end = block_start + (block_end - block_start) * k / nWorkers;
    					while (data[--sub_end] != '\n') {}
    					sub_end++;
    				}
    				else {
        				sub_end = block_end;
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

    			if (bytes_read == available) {
    				break;
    			}
    			else if (available - bytes_read < read_amount) {
    				read_amount = available - bytes_read;
    			}

    			if (read_end + read_amount > data.length) {
    				System.arraycopy(data, block_end, data, 0, read_end - block_end);
    				read_end -= block_end;
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

	    	for (Worker w: workers) {
	    		try {
					w.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	    	}
    	}
    	catch (IOException e) {
			e.printStackTrace();
		}

    	printInterval("Workers done");

    	List<Map.Entry<String, Integer>> list = new ArrayList<>(12800);

    	for (Map.Entry<String, AtomicInteger> me : themap.entrySet()) {
    		list.add(new SimpleEntry<String, Integer>(me.getKey(), me.getValue().intValue()));
    	}

    	final SortedMap<String, Integer> sorted = new ValueKeySortedMap<String, Integer>(list);

    	printInterval("Ordered");

        return sorted;
    }

    static class Worker extends Thread {
    	private final int[] END_OF_WORK;
    	private final BlockingQueue<int[]> queue;

    	Worker(String name, BlockingQueue<int[]> queue, int[] END_OF_WORK) {
    		this.queue = queue;
    		this.END_OF_WORK = END_OF_WORK;
    	}

        /**
         * worker parallel worker takes blocks of bytes read and processes them
         */
    	public final void run() {
    		final Matcher nameMatcher = namePattern.matcher("");
    		final StringBuilder location = new StringBuilder();

    		AtomicInteger v = new AtomicInteger();

    		for (;;) {
    			int[] block_start_end;
    			for (;;) {
    				try {
    					block_start_end = queue.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
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
    			// process block
    			while (start < block_end) {
    				int end = start;

    				if (column == 4) {
    					fine = 0;
    					while (start < block_end && (char)data[start] >= '0' && (char)data[start] <= '9') {
    						fine = fine * 10 + (char)data[start++] - '0';
    					}
    					end = start;
    					while (end < block_end && data[end] != ',' && data[end] != '\n') { end++; }
    				}
    				else if (column == 7) {
    					while (end < block_end && data[end] != ',' && data[end] != '\n') { end++; }

    					if (fine > 0) {
    						for (location.setLength(0); start < end; ) {
    							location.append((char) data[start++]);
    						}

    			    		nameMatcher.reset(location);
    			    		if (nameMatcher.find()) {
    			    			final String name = nameMatcher.group();
    			    			AtomicInteger v0 = themap.putIfAbsent(name, v);
    			    			if (v0 == null) {
    			    				v.addAndGet(fine);
    			    				v = new AtomicInteger();
    			    			} else {
				    				v0.addAndGet(fine);
    			    			}
    		    			}
    					}
    				}
    				else {
    					while (end < block_end && data[end] != ',' && data[end] != '\n') { end++; }
    				}

    				column++;
    				if (end < block_end && data[end] == '\n') {
    					column = 0;
    				}
    				start = end + 1;
    			}
    		}
        }
    }

    public static class ValueKeySortedMap<K extends Comparable<K>, V extends Comparable<V>>
					    extends LinkedHashMap<K, V> implements SortedMap<K, V> {

    	final ArrayList<Map.Entry<K, V>> list;

    	final Comparator<Map.Entry<K, V>> comparator = new Comparator<Map.Entry<K, V>>() {
				public int compare(Entry<K, V> e1, Entry<K, V> e2) {
					int c = e2.getValue().compareTo(e1.getValue());
					if (c != 0) return c;
					return e1.getKey().compareTo(e2.getKey());
			}};

		public ValueKeySortedMap(Collection<Map.Entry<K, V>> entries) {
			list = new ArrayList<Map.Entry<K, V>>(entries);

	    	Collections.sort(list, comparator);

    		for (Map.Entry<? extends K, ? extends V> me : entries) {
    			super.put(me.getKey(), me.getValue());
    		}
    	}
		public Comparator<? super K> comparator() {
			return new Comparator<K>() {
				public int compare(K k1, K k2) {
					int c = get(k2).compareTo(get(k1));
					if (c != 0) return c;
					return k1.compareTo(k2);
				}};
		}
		public SortedMap<K, V> subMap(K fromKey, K toKey) {
			int fromIndex = Collections.<Map.Entry<K, V>>binarySearch(list, new SimpleEntry(fromKey, get(fromKey)), comparator);
			int toIndex = Collections.<Map.Entry<K, V>>binarySearch(list, new SimpleEntry(toKey, get(toKey)), comparator);
			return new ValueKeySortedMap<>(list.subList(fromIndex, toIndex));
		}
		public SortedMap<K, V> headMap(K toKey) {
			return subMap(null, toKey);
		}
		public SortedMap<K, V> tailMap(K fromKey) {
			return subMap(fromKey, null);
		}
		public K firstKey() {
			return list.get(0).getKey();
		}
		public K lastKey() {
			return list.get(list.size() - 1).getKey();
		}
		private static final long serialVersionUID = -7794035165242839160L;
	};

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
