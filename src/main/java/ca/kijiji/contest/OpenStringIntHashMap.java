package ca.kijiji.contest;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class OpenStringIntHashMap {
	public volatile long pad7, pad6, pad5, pad4, pad3, pad2, pad1;

	public final int NO_ELEMENT_VALUE = Integer.MIN_VALUE;

	private final int capacity;
	private final String[] keys;
	private final long[] hashAndValues; // each long is value << 32 | hash

	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
	private final ReadLock readLock = readWriteLock.readLock();
	private final WriteLock writeLock = readWriteLock.writeLock();
	public volatile long Pad1, Pad2, Pad3, Pad4, Pad5, Pad6, Pad7;

	public OpenStringIntHashMap(int capacity) {
		this.capacity = capacity;
		keys = new String[capacity];
		hashAndValues = new long[capacity];
		pad7 = pad6 = pad5 = pad4 = pad3 = pad2 = pad1 = 7;
		Pad1 = Pad2 = Pad3 = Pad4 = Pad5 = Pad6 = Pad7 = 7;
	}

	public void clear() {
		writeLock.lock(); try
		{
			Arrays.fill(keys, 0);
			Arrays.fill(hashAndValues, 0);
		}
		finally { writeLock.unlock(); }
	}

	public int get(String key) {
		return get(key, new char[256]);
	}

	public int get(String key, char[] keybuf) {
		int hash = hash(key, keybuf);
		int cur = hash % capacity;
		if (cur < 0) cur += capacity;

		int v = scanValueHash(hash, cur, capacity);
		if (v == NO_ELEMENT_VALUE) {
			v = scanValueHash(hash, 0, cur);
		}
		return v;
	}

	public void put(String key, int value) {
		put(key, value, new char[256]);
	}

	public void put(String key, int value, char[] keybuf) {
		int hash = hash(key, keybuf);
		int cur = hash % capacity;
		if (cur < 0) cur += capacity;

		if (!put(key, hash, value, cur, capacity)) {
			if (!put(key, hash, value, 0, cur)) {
				throw new IllegalStateException("Exceeded capacity "+ capacity);
			}
		}
	}

	public void adjustOrPutValue(String key, int value, char[] keybuf) {
		int hash = hash(key, keybuf);
		int cur = hash % capacity;
		if (cur < 0) cur += capacity;

		if (!adjustOrPutValue(key, hash, value, cur, capacity)) {
			if (!adjustOrPutValue(key, hash, value, 0, cur)) {
				throw new IllegalStateException("Exceeded capacity "+ capacity);
			}
		}
	}

	/**
	 * @param hash
	 * @param cur
	 * @param end
	 * @return the found value with hash, otherwise NO_ELEMENT_VALUE
	 */
	private int scanValueHash(int hash, int cur, int end) {
		for (; cur < end; cur++) {
			long vh = hashAndValues[cur];
			int h = (int) vh;
			if (h == hash) {
				readLock.lock(); try
				{
					return (int) (hashAndValues[cur] >>> 32);
				}
				finally { readLock.unlock(); }
			}
			else if (h == 0) {
				readLock.lock(); try
				{
					do {
						vh = hashAndValues[cur];
						h = (int) vh;
						if (h == hash) {
							return (int) (vh >>> 32);
						}
					} while (h != 0 && ++cur < end);

					return NO_ELEMENT_VALUE;
				}
				finally { readLock.unlock(); }
			}
		}
		return NO_ELEMENT_VALUE;
	}

	private boolean put(String key, int hash, int value, int cur, int end) {
		for (; cur < end; cur++) {
			long vh = hashAndValues[cur];
			int h = (int) vh;
			if (h == hash) {
				writeLock.lock(); try
				{
					hashAndValues[cur] = (long)value << 32 | (long)hash & 0x00FFFFFFFF;
					return true;
				}
				finally { writeLock.unlock(); }
			}
			else if (h == 0) {
				readLock.lock();
				do {
					vh = hashAndValues[cur];
					h = (int) vh;
					if (h == hash) {
						readLock.unlock();
						writeLock.lock(); try
						{
							hashAndValues[cur] = (long)value << 32 | (long)hash & 0x00FFFFFFFF;
							return true;
						}
						finally { writeLock.unlock(); }
					}
					else if (h == 0) {
						readLock.unlock();
						writeLock.lock(); try
						{
							hashAndValues[cur] = (long)value << 32 | (long)hash & 0x00FFFFFFFF;
							keys[cur] = key;
							return true;
						}
						finally { writeLock.unlock(); }
					}
				} while (++cur < end);

				readLock.unlock();
				return false;
			}
		}
		return false;
	}

	private boolean adjustOrPutValue(String key, int hash, int value, int cur, int end) {
		for (; cur < end; cur++) {
			long vh = hashAndValues[cur];
			int h = (int) vh;
			if (h == hash) {
				writeLock.lock(); try
				{
					hashAndValues[cur] += (long)value << 32;
					return true;
				}
				finally { writeLock.unlock(); }
			}
			else if (h == 0) {
				readLock.lock();
				do {
					vh = hashAndValues[cur];
					h = (int) vh;
					if (h == hash) {
						readLock.unlock();
						writeLock.lock(); try
						{
							hashAndValues[cur] += (long)value << 32;
							return true;
						}
						finally { writeLock.unlock(); }
					}
					else if (h == 0) {
						readLock.unlock();
						writeLock.lock(); try
						{
							hashAndValues[cur] = (long)value << 32 | (long)hash & 0x00FFFFFFFF;
							keys[cur] = key;
							return true;
						}
						finally {  writeLock.unlock(); }
					}
				} while (++cur < end);

				readLock.unlock();
				return false;
			}
		}
		return false;
	}

	public void putAllTo(Map<String, Integer> dest) {
		putRangeTo(0, capacity, dest);
	}

	protected void putRangeTo(int cur, int end, Map<String, Integer> dest) {
		readLock.lock(); try
		{
			String key;
			for (; cur < end; cur++) {
				if ((key = keys[cur]) != null) {
					synchronized (dest) {
						dest.put(key, (int) (hashAndValues[cur] >> 32));
					}
				}
			}
		}
		finally { readLock.unlock(); }
	}

	private int hash(String key, char[] keybuf) {
		int h = 0;
		int l = key.length();
		key.getChars(0, l, keybuf, 0);
		for (; --l >= 0;) {
			char c = keybuf[l];
			int i = (c == ' ') ? 0 : (int)c & 0x00FF - 64;
			h = h * 47 + i;
		}
		return h;
	}
}
