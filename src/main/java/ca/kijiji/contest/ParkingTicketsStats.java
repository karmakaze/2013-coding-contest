package ca.kijiji.contest;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ParkingTicketsStats {

	static final byte[] data = new byte[1 * 1024 * 1024];

	// 4-cores with HyperThreading sets nThreads = 8
	static final int nWorkers = 4; // Runtime.getRuntime().availableProcessors();

	// use small blocking queue size to limit read-ahead for higher cache hits
	static final ArrayBlockingQueue<int[]> byteArrayQueue = new ArrayBlockingQueue<int[]>(2 * nWorkers - 1, false);
	static final int SIZE = 20 * 1024;
	static final int[] END_OF_WORK = new int[0];

    public static SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
    	printInterval("Pre-initialization");

    	Thread reader = new Reader(parkingTicketsStream, byteArrayQueue);
    	reader.start();

		final Worker[] workers = new Worker[nWorkers];
		for (int k = 0; k < nWorkers; k++) {
			workers[k] = new Worker("worker"+ k, SIZE, byteArrayQueue, END_OF_WORK);
			workers[k].start();
		}

    	printInterval("Initialization");

		try {
			reader.join();

	    	for (Worker w: workers) {
				w.join();
	    	}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

    	printInterval("Workers done");

    	// merge results in pyramid manner
    	ExecutorService executor = Executors.newFixedThreadPool(4);
    	for (int step = 1; step < nWorkers; step *= 2) {
    		ArrayList<Future<?>> futures = new ArrayList<>();
    		for (int j = 0; j < nWorkers; j += step*2) {
				final Worker wj = workers[j];
				if (j + step < nWorkers) {
					final Worker w = workers[j + step];
//					println("Merging worker "+ (j + step) +" into worker "+ j);
					futures.add(executor.submit(new Runnable() { public void run() {
	    	        		w.mergeTo(wj);
						}}));
				}
    		}
    		for (Future<?> f : futures) {
    			try {
					f.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
    		}
    	}

    	printInterval("Parallel merged");

    	final OpenStringIntHashMap map0 = workers[0].map;

    	final SortedMap<String, Integer> sorted = new ConcurrentSkipListMap<String, Integer>(new Comparator<String>() {
			public final int compare(String k1, String k2) {
				int c = map0.get(k2) - map0.get(k1);
				if (c != 0) return c;
				return k1.compareTo(k2);
			}});

    	Thread[] threads = new Thread[nWorkers];
    	for (int t = 0; t < nWorkers; t++) {
    		final int start = SIZE * t / nWorkers;
    		final int end = SIZE * (t+1) / nWorkers;

        	threads[t] = new Thread(null, null, "gather"+ t, 2048) {
        		public void run() {
    				map0.putRangeTo(start, end, sorted);
        		}
        	};
        	threads[t].start();
    	}

    	for (Thread thread : threads) {
	    	try { thread.join(); } catch (InterruptedException e) {}
    	}

    	printInterval("Parallel ordered");

        return sorted;
    }

    static class Reader extends Thread {
    	final InputStream parkingTicketsStream;
    	final ArrayBlockingQueue<int[]> byteArrayQueue;

    	Reader(InputStream parkingTicketsStream, ArrayBlockingQueue<int[]> byteArrayQueue) {
    		this.parkingTicketsStream = parkingTicketsStream;
    		this.byteArrayQueue = byteArrayQueue;
    	}

    	public void run() {
        	try {
    			final int available = parkingTicketsStream.available();

        		int bytes_read = 0;
        		int read_end = 0;
        		int block_start = 0;
        		int block_end = 0;
        		for (int read_amount = 128 * 1024; (read_amount = parkingTicketsStream.read(data, read_end, read_amount)) > 0; ) {
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
        	}
        	catch (IOException e) {
    			e.printStackTrace();
    		}
    	}
    }

    static class Worker extends Thread {
    	private final int[] END_OF_WORK;
    	private final BlockingQueue<int[]> queue;
    	private final OpenStringIntHashMap map;

    	Worker(String name, int capacity, BlockingQueue<int[]> queue, int[] END_OF_WORK) {
    		this.queue = queue;
    		this.END_OF_WORK = END_OF_WORK;
    		map = new OpenStringIntHashMap(capacity);
    	}

        /**
         * worker parallel worker takes blocks of bytes read and processes them
         */
    	public final void run() {
    		final StringBuilder nameBuffer = new StringBuilder(256);

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
    			int start = block_start + 18; // skip first 2 columns 8+1+8+1
    			int column = 2;
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
    						if (extractStreetName(data, start, end, nameBuffer)) {
    			    			map.adjustOrPutValue(nameBuffer, fine);
    						}
    					}
    				}
    				else {
    					while (end < block_end && data[end] != ',' && data[end] != '\n') { end++; }
    				}

    				column++;
    				if (end < block_end && data[end] == '\n') {
    					column = 2;
    					end += 18; // skip first 2 columns 8+1+8+1
    				}
    				start = end + 1;
    			}
    		}
        }

    	public final void mergeTo(Worker dest) {
    		map.mergeTo(dest.map);
    	}
    }

    static final boolean extractStreetName(final byte[] data, final int start, final int end, final StringBuilder output) {
    	output.setLength(0);
    	int count = 0;
    	boolean letter = false;
		for (int i = start; i < end; i++) {
			char c = (char) data[i];
			if (c >= 'A' && c <= 'Z') {
				output.append((char) c);
				letter = true;
				count++;
			}
			else if (c >= '0' && c <= '9') {
				output.append((char) c);
				count++;
			}
			else if (c == ' ') {
				if (count > 3) {
					if (letter) {
						break;
					}
					output.setLength(0);
					count = 0;
					letter = false;
				}
				else if (count == 2 && output.charAt(0) == 'S' && output.charAt(1) == 'T'
					  || count == 3 && output.charAt(0) == 'T' && output.charAt(1) == 'H' && output.charAt(2) == 'E') {
					output.append((char) c);
					count++;
				}
				else if (count == 3 && letter) {
					break;
				}
				else {
					output.setLength(0);
					count = 0;
					letter = false;
				}
			}
		}
		return count > 2;
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
//    	System.out.println(line);
    }
}
