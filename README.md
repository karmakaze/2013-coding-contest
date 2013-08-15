Repository Notes
================
- The 'master' branch has this updated README.md file and my favourite implementation 'closed-L2' merged in. After replacing the regex and using a CharSequence buffer rather than always making String objects for street names runs in ~240ms. Using the 'warm-up' test harness average times ~160ms. Parallel pyramid merge, sort with ConcurrentSkipListMap ~150ms. Skipping first 2 columns (18 bytes) ~125ms. With hyperthreading (7 workers) on Linux 3.2 64-bit kernel ~118-124ms. (YMMV: highly tuned to my particular system Ivy Bridge i7 3.4GHz)

- The ['submit'](https://github.com/karmakaze/2013-coding-contest/tree/submit) branch is what I actually submitted. It uses a large hash table with a 'perfect hash' function (but not a minimal perfect hash). The table is implemented with a String[] for the keys using lock-free writes and an AtomicIntegerArray for the values. Runtime ~513-540ms on Linux Core i7-3770 3.40GHz (~435ms with warm-up).

- The ['closed-atomic'](https://github.com/karmakaze/2013-coding-contest/tree/closed-atomic) branch is what I wanted to finish but didn't get done before the deadline. It runs ~398-420ms on my machine. It uses 8 threads (HTT) and tries to keep all 'working' data and the closed hash table within 8 MB of L3 shared cache memory. Instead of comparing entire keys, 32-bit hash values are compared using optimistic lock-free reads and a memory barrier for re-reads/writes. The values are in an AtomicIntegerArray.

- The ['closed-L2'](https://github.com/karmakaze/2013-coding-contest/tree/closed-L2) branch uses private arrays: long[] for packed value|hash, and String[] for keys. These are later merged (sequential merge ~7ms). Uses only 4 worker threads to use L2 effectively rather than 8 threads' registers. Runtime ~414ms (where 41ms is final ordering which can certainly be improved).

- The ['chm'](https://github.com/karmakaze/2013-coding-contest/tree/chm) branch uses a ConcurrentHashMap<String, AtomicInteger> and is an attempt to be the simplest datastructure with good performance. This is perhaps better done with the key as a StringBuilder/StringBuffer/CharBuffer which is reused until it is added then a new buffer allocated.

Cheers,
-Keith
