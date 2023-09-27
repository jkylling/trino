/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;

public class SortedEvictableCache<K extends Comparable<K>, V>
        extends EvictableCache<K, V>
{
    private final ConcurrentNavigableMap<K, Token<K>> navigableTokens;

    SortedEvictableCache(CacheBuilder<? super Token<K>, ? super V> cacheBuilder, CacheLoader<? super K, V> cacheLoader)
    {
        super(cacheBuilder, cacheLoader, new ConcurrentSkipListMap<>());
        this.navigableTokens = (ConcurrentNavigableMap<K, Token<K>>) this.tokens;
    }

    /**
     * Return the greatest entry less than or equal to key, if it has not been evicted from the cache.
     * Potentially there can be entries in the cache less than key. Ideally the cache should be used such
     * that entries with larger keys are expected to be evicted after entries with smaller keys.
     */
    public Map.Entry<K, V> floorEntryIfNotEvicted(K key)
            throws ExecutionException
    {
        Map.Entry<K, Token<K>> entry = navigableTokens.floorEntry(key);
        if (entry == null) {
            return null;
        }
        V value = dataCache.getIfPresent(entry.getValue());
        if (value == null) {
            return null;
        }
        return Map.entry(entry.getKey(), value);
    }
}
