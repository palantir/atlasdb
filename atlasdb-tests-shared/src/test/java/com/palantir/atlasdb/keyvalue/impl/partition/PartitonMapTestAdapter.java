package com.palantir.atlasdb.keyvalue.impl.partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.mockito.cglib.core.CollectionUtils;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.util.Pair;

/**
 * This is to extract the actual underlying maps
 * from the callback API.
 * 
 * @author htarasiuk
 *
 */
public class PartitonMapTestAdapter <K, V> {

	final Multimap<K,V> entries = Multimaps.<K, V>newListMultimap(Maps.<K, Collection<V>>newLinkedHashMap(), new Supplier<List<V>>() {
		@Override
		public List<V> get() {
			return new ArrayList<>();
		}
	});
	
	Iterable<Entry<K, V>> getEntries() {
		return entries.entries();
	}

	void registerEntry(Pair<K, V> entry) {
		entries.put(entry.lhSide, entry.rhSide);
	}
	
	final Function<Pair<K, V>, Void> task = new Function<Pair<K, V>, Void>() {
		@Override
		public Void apply(@Nullable Pair<K, V> input) {
			registerEntry(input);
			return null;
		}
	}; 
	
	public static void testRowsRead(DynamicPartitionMap map, String tableName, Iterable<byte[]> rows, Map<KeyValueService, Iterable<byte[]>> expected) {
		PartitonMapTestAdapter<KeyValueService, Iterable<byte[]>> adapter = new PartitonMapTestAdapter<>();
		map.runForRowsRead(tableName, rows, adapter.task);
		Assert.assertTrue(expected.equals(adapter.entries));
	}
	
	private static <T> boolean equalCollections(Collection<T> c1, Collection<T> c2) {
		if (c1.size() != c2.size()) {
			return false;
		}
		for (T el : c1) {
			if (!c2.contains(el)) {
				return false;
			}
		}
		return true;
	}
	
	private static <T> boolean equalIterables(Iterable<T> i1, Iterable<T> i2) {
		Set<T> s1 = Sets.newHashSet(i1);
		Set<T> s2 = Sets.newHashSet(i2);
		return s1.equals(s2);
	}
	
}
