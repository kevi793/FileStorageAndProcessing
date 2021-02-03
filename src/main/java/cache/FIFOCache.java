package cache;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class FIFOCache<K, V> implements Cache<K, V> {

    private final int cacheSize;
    private final HashMap<K, Integer> map;
    private final List<Node<K, V>> list;

    public FIFOCache(int cacheSize) {
        this.cacheSize = cacheSize;
        this.map = new HashMap<>();
        this.list = new ArrayList<>();
    }

    @Override
    public synchronized void put(K key, V value) {
        if (this.map.containsKey(key)) {
            log.debug("Updating value for key: {}", key.toString());
            this.list.set(this.map.get(key), new Node<>(key, value));
        } else {

            if (this.list.size() == this.cacheSize) {
                log.debug("Cache is full. Need to evict: {}", this.list.get(0).key.toString());
                this.map.remove(this.list.get(0).key);
                this.list.remove(0);
            }

            this.list.add(new Node<>(key, value));
            this.map.put(key, this.list.size() - 1);
            log.debug("Key added in cache: {}", key.toString());
        }
    }

    @Override
    public V get(K key) {
        if (!this.map.containsKey(key)) {
            log.debug("Cache Miss for key: {}", key);
            return null;
        }

        log.debug("Cache Hit for key: {}", key.toString());
        return this.list.get(this.map.get(key)).value;
    }

    private static class Node<K, V> {
        private final K key;
        private final V value;


        private Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

}


