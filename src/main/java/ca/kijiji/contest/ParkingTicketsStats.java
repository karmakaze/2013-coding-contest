package ca.kijiji.contest;

import java.io.InputStream;
import java.util.SortedMap;

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
	public final static int algorithm = 4;

    public static SortedMap<String, Integer> sortStreetsByProfitability(final InputStream parkingTicketsStream) {
    	switch (algorithm) {
    	case 1: return ParkingTicketsStats1.sortStreetsByProfitability(parkingTicketsStream);

    	case 2: return ParkingTicketsStats2.sortStreetsByProfitability(parkingTicketsStream);

    	case 3: return ParkingTicketsStats3.sortStreetsByProfitability(parkingTicketsStream);

    	case 4: return ParkingTicketsStats4.sortStreetsByProfitability(parkingTicketsStream);

    	case 5: return ParkingTicketsStats5.sortStreetsByProfitability(parkingTicketsStream);

        default: throw new IllegalStateException("Selected algorithm "+ algorithm +" has no implementation.");
    	}
    }
}