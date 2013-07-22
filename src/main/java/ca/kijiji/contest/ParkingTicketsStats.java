package ca.kijiji.contest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.regex.Matcher;

import com.lmax.disruptor.ClaimStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

public class ParkingTicketsStats {

	// 24-bit indices (16M possible entries)
	static final int BITS = 24;
	static final int UNUSED_BITS = 32 - BITS;
	static final int SIZE = 1 << BITS;
	static final int MASK = SIZE - 1;

	static final String[] keys = new String[SIZE];
	static final AtomicIntegerArray vals = new AtomicIntegerArray(SIZE);

	public static final int hash(final String k) {
		int h = 0;
		try {
			for (byte b : k.getBytes("UTF-8")) {
				int c = (b == ' ') ? 0 : (int)b & 0x00FF - 64;
				h = h * 71 + c;
				h = (h ^ (h >>> BITS)) & MASK;
			}
		}
		catch (UnsupportedEncodingException e) {}

		return h;
	}

	public static final void add(final String k, final int d) {
		int i = hash(k);
		vals.addAndGet(i, d);

// Uncomment block to enable hashing output
//		String k0 = keys[i];
//		if (k0 != null && !k0.equals(k)) {
//			System.err.println("Key hash clash: first "+ k0 +" and "+ k);
//		}
//		else {
			keys[i] = k;
//		}
	}

	public static final int get(final String k) {
		int i = hash(k);
		return vals.get(i);
	}

    @SuppressWarnings("unchecked")
	public static final SortedMap<String, Integer> sortStreetsByProfitability(InputStream parkingTicketsStream) {
    	printInterval("Pre-entry initialization");

		printProperty("os.arch");
    	System.out.println("InputStream is "+ parkingTicketsStream);

        ExecutorService exec = Executors.newCachedThreadPool();

        // Preallocate RingBuffer with 1024 ValueEvents
        ClaimStrategy claimStrategy = new SingleThreadedClaimStrategy(256);
        WaitStrategy waitStrategy = new YieldingWaitStrategy();
        Disruptor<ValueEvent> disruptor = new Disruptor<ValueEvent>(ValueEvent.EVENT_FACTORY, exec, claimStrategy, waitStrategy);

        final EventHandler<ValueEvent> handler = new EventHandler<ValueEvent>() {
            // event will eventually be recycled by the Disruptor after it wraps
            public void onEvent(final ValueEvent event, final long sequence, final boolean endOfBatch) throws Exception {
            	System.out.println("Thread: "+ Thread.currentThread().getId());
            	String line = event.getValue();
	    		String[] parts = line.split(",");
//		    	String tag_number_masked = parts[0];
//		    	String date_of_infraction = parts[1];
//		    	String infraction_code = parts[2];
//		    	String infraction_description = parts[3];
	    		String sfa = parts[4];
	    		Integer set_fine_amount = 0;
	    		try {
		    		set_fine_amount = Integer.parseInt(sfa);
	    		}
	    		catch (NumberFormatException e) {
	    			System.out.print(e.getClass().getSimpleName() +": "+ sfa);
	    		}
//		    	String time_of_infraction = parts[5];
//		    	String location1 = parts[6];
	    		String location2 = parts[7];
//		    	String location3 = parts[8];
//		    	String location4 = parts[9];

	    		Matcher nameMatcher = event.nameMatcher;
	    		nameMatcher.reset(location2);
	    		if (nameMatcher.find()) {
	    			String l = nameMatcher.group();
//	    			streetMatcher.reset(location2);
//	    			if (streetMatcher.find()) {
//	    				String l = streetMatcher.group(2);
	    			/*
		    	//	l = l.replaceAll("[0-9]+", "");
		    		l = l.replaceAll("[^A-Z]+ ", "");
		    		l = l.replaceAll(" (N|NORTH|S|SOUTH|W|WEST|E|EAST)$", "");
		    		l = l.replaceAll(" (AV|AVE|AVENUE|BLVD|CRES|COURT|CRT|DR|RD|ST|STR|STREET|WAY)$", "");
		    	//	l = l.replaceAll("^(A|M) ", "");
		    		l = l.replaceAll("(^| )(PARKING) .*$", "");
		    		l = l.trim();
    			 	*/
//			    	String province = parts[10];
	    			add(l, set_fine_amount);

//		    		if (!l.equals("KING") && (location2.indexOf(" KING ") >= 0 || location2.endsWith(" KING"))) {
//		    			System.out.println(l +" <- "+ location2);
//		    		}
    			}
    			else {
    				if (location2.indexOf("KING") >= 0 && location2.indexOf("PARKING") == -1) {
	    				System.out.println(""+ location2);
    				}
    			}
            //	System.out.println("Sequence: " + sequence);
            //	System.out.println("ValueEvent: " + line);
            }
        };

        // Build dependency graph
        disruptor.handleEventsWith(handler);
        RingBuffer<ValueEvent> ringBuffer = disruptor.start();

    	try {
	    	BufferedReader r = new BufferedReader(new InputStreamReader(parkingTicketsStream));
	    	r.readLine();	// discard header row
	    	for (String line; (line = r.readLine()) != null; ) {
	            // Two phase commit. Grab one of the slots
	            long seq = ringBuffer.next();
	            ValueEvent valueEvent = ringBuffer.get(seq);
	            valueEvent.setValue(line);
	            ringBuffer.publish(seq);
	    	}
    	}
    	catch (IOException e) {
			e.printStackTrace();
		}

        disruptor.shutdown();
        exec.shutdown();

    	printInterval("Read and summed");

//    	System.out.println("Size: "+ streets.size());

    	SortedMap<String, Integer> sorted = new TreeMap<String, Integer>(new Comparator<String>() {
			public int compare(String o1, String o2) {
				int c = get(o2) - get(o1);
				if (c != 0) return c;
				return o2.compareTo(o1);
			}});

    	for (int i = 0; i < SIZE; i++) {
    		int v;
    		if ((v = vals.get(i)) != 0) {
    			sorted.put(keys[i], v);
    		}
    	}

    	printInterval("Populated TreeSet");

        return sorted;
    }

    static volatile long lastTime = System.currentTimeMillis();

    public static void printInterval(String name) {
    	long time = System.currentTimeMillis();
    	System.out.println(time +" "+ name +": "+ (time - lastTime) +" ms");
    	lastTime = time;
    }

    public static void printElement(String key, Map<String, Integer> streets) {
    	System.out.println(key +": $"+ streets.get(key));
    }

    public static void printProperty(String name) {
		System.out.println(name +": "+ System.getProperty(name));
    }
}