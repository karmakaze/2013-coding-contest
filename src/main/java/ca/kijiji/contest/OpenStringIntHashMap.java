package ca.kijiji.contest;

import java.util.Arrays;
import java.util.Map;

public class OpenStringIntHashMap {
	public volatile long pad7, pad6, pad5, pad4, pad3, pad2, pad1;

	public final int NO_ELEMENT_VALUE = 0;

	private final int capacity;
	private final String[] keys;
	private final long[] valueHashes;

	public volatile long Pad1, Pad2, Pad3, Pad4, Pad5, Pad6, Pad7;

	public OpenStringIntHashMap(int capacity) {
		this.capacity = capacity;
		keys = new String[capacity];
		valueHashes = new long[capacity];
		pad7 = pad6 = pad5 = pad4 = pad3 = pad2 = pad1 = 7;
		Pad1 = Pad2 = Pad3 = Pad4 = Pad5 = Pad6 = Pad7 = 7;
	}

	public void clear() {
		Arrays.fill(keys, 0);
		Arrays.fill(valueHashes, 0);
	}

	public int get(String key) {
		int hash = hash(key);
		int cur = hash % capacity;
		if (cur < 0) cur += capacity;

		int v = scanValueHash(key, hash, cur, capacity);
		if (v == NO_ELEMENT_VALUE) {
			v = scanValueHash(key, hash, 0, cur);
		}
		return v;
	}

	public void put(String key, int value) {
		int hash = hash(key);
		int cur = hash % capacity;
		if (cur < 0) cur += capacity;

		if (!put(key, hash, value, cur, capacity)) {
			if (!put(key, hash, value, 0, cur)) {
				throw new IllegalStateException("Exceeded capacity "+ capacity);
			}
		}
	}

	public void adjustOrPutValue(CharSequence key, int value) {
		int hash = hash(key);
		int cur = hash % capacity;
		if (cur < 0) cur += capacity;

		if (!adjustOrPutValue(key, hash, value, cur, capacity)) {
			if (!adjustOrPutValue(key, hash, value, 0, cur)) {
				throw new IllegalStateException("Exceeded capacity "+ capacity);
			}
		}
	}

	public void mergeTo(OpenStringIntHashMap mergeTo) {
		String key;
		for (int cur = 0; cur < capacity; cur++) {
			if ((key = keys[cur]) != null) {
				int v = (int) (valueHashes[cur] >>> 32);
				mergeTo.adjustOrPutValue(key, v);
			}
		}
	}

	/**
	 * @param hash
	 * @param cur
	 * @param end
	 * @return the found value with hash, otherwise NO_ELEMENT_VALUE
	 */
	private int scanValueHash(String key, int hash, int cur, int end) {
		long vh;
		do {
			vh = valueHashes[cur];
			int h = (int) vh;
			if (h == hash) {
				return (int) (vh >>> 32);
			}
		} while (vh != 0 && ++cur < end);

		return NO_ELEMENT_VALUE;
	}

	private boolean put(String key, int hash, int value, int cur, int end) {
		do {
			long vh = valueHashes[cur];
			int h = (int) vh;
			if (h == hash) {
				valueHashes[cur] = (long)value << 32 | (long)hash & 0x00ffffffffL;
				return true;
			}
			else if (h == 0) {
				valueHashes[cur] = (long)value << 32 | (long)hash & 0x00ffffffffL;
				keys[cur] = key;
				return true;
			}
		} while (++cur < end);

		return false;
	}

	private final boolean adjustOrPutValue(CharSequence key, int hash, int value, int cur, int end) {
		do {
			long vh = valueHashes[cur];
			int h = (int) vh;
			if (h == hash) {
				valueHashes[cur] += (long)value << 32;
				return true;
			}
			else if (h == 0) {
				valueHashes[cur] = (long)value << 32 | (long)hash & 0x00ffffffffL;
				keys[cur] = key.toString();
				return true;
			}
		} while (++cur < end);

		return false;
	}

	public void putAllTo(Map<String, Integer> dest) {
		putRangeTo(0, capacity, dest);
	}

	protected void putRangeTo(int cur, int end, Map<String, Integer> dest) {
		for (String key; cur < end; cur++) {
			if ((key = keys[cur]) != null) {
				int v = (int) (valueHashes[cur] >>> 32);
				dest.put(key, v);
			}
		}
	}

	private int hash(CharSequence key) {
		int hash = 0;

		int l = key.length();
		for (int i = 0; i < l; i++) {
            hash = (16777619 * hash) ^ key.charAt(i);
 		}
		return hash;
	}
}
