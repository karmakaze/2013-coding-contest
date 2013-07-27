package ca.kijiji.contest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

import java.io.InputStream;
import java.util.SortedMap;

import org.hamcrest.Matcher;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParkingTicketsStatsTest {

    private static final Logger LOG = LoggerFactory.getLogger(ParkingTicketsStatsTest.class);

    // Download the file from the following URL and extract into src/test/resources
    // http://www1.toronto.ca/City_Of_Toronto/Information_&_Technology/Open_Data/Data_Sets/Assets/Files/parking_tickets_data_2012.zip
    private static final String PARKING_TAGS_DATA_2012_CSV_PATH = "/Parking_Tags_Data_2012.csv";

    @Test
    public void testSortStreetsByProfitability() throws Exception {
        final long startTime = System.currentTimeMillis();

        final InputStream parkingTicketsStream = this.getClass().getResourceAsStream(PARKING_TAGS_DATA_2012_CSV_PATH);
        parkingTicketsStream.mark(256 * 1024 * 1024);
        for (final int p : new int[] {29, 31, 37, 41, 43, 47,53,59, 61, 67, 71,
        73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173,
        179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281,
        283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409,
        419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541,
        547, 557, 563, 569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659,
        661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809,
        811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937, 941,
        947, 953, 967, 971, 977, 983, 991, 997}) {
        parkingTicketsStream.reset();
       	ParkingTicketsStats.hashfact = p;
       	ParkingTicketsStats.setHashSeed(p * 997 + p);
       	ParkingTicketsStats.clashes.set(0);
        final SortedMap<String, Integer> streets = ParkingTicketsStats.sortStreetsByProfitability(parkingTicketsStream);

        final long duration = System.currentTimeMillis() - startTime;
        LOG.info("Duration of computation = {} ms", duration);

        // Watch out for some nasty business in the data!
        // Luckily, there is a 5% margin of error on each number asserted below, so don't bother finding the
        // most accurate solution. Just build the best implementation which solves the problem reasonably well and
        // satisfies the test case.
        //
        // Generally, addresses follow the format: NUMBER NAME SUFFIX DIRECTION
        // with
        // NUMBER (optional) = digits, ranges of digit (e.g. 1531-1535), letters, characters like ! or ? or % or /
        // NAME (required) = the name you need to extract, mostly uppercase letters, sometimes spaces (e.g. ST CLAIR), rarely numbers (e.g. 16TH)
        // SUFFIX (optional) = the type of street such as ST, STREET, AV, AVE, COURT, CRT, CT, RD ...
        // DIRECTION (optional) = one of EAST, WEST, E, W, N, S
        //
        // NOTE: the street name should be extracted from the field location2 only.

        assertThat(streets.get("KING"), closeTo(2570710));
        assertThat(streets.get("ST CLAIR"), closeTo(1871510));
        assertThat(streets.get(streets.firstKey()), closeTo(3781095));
        ParkingTicketsStats.println("\nHash p="+ p +" clashes="+ ParkingTicketsStats.clashes.get());
        if (ParkingTicketsStats.clashes.get() == 0) {
        	break;
        }
        }
    }

    private Matcher<Integer> closeTo(final int num) {
        final int low = (int) (num * 0.95);
        final int high = (int) (num * 1.05);
        return allOf(greaterThan(low), lessThan(high));
    }
}