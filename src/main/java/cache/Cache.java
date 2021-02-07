package cache;

/**
 * Interface for Cache.
 *
 * @param <K> The key.
 * @param <V> The value.
 */
public interface Cache<K, V> {

    /**
     * Add a key value pair to the cache.
     *
     * @param key   The key.
     * @param value The value.
     */
    void put(K key, V value);

    /**
     * Retrieve the value stored corresponding to the key in the cache.
     * Return null if key is not present.
     *
     * @param key The key.
     * @return Value stored for the input key, null if key is not present.
     */
    V get(K key);

}
