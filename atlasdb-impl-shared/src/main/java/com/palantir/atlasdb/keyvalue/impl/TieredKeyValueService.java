/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.collect.IteratorUtils;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.AssertUtils;
import com.palantir.util.Pair;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class TieredKeyValueService implements KeyValueService {
    /**
     * We keep track of and report only the tables we're actually tiering
     * so the TieredKvsMover can know what tables need to be moved between
     * tiers.
     */
    private final Set<String> tieredTables;
    private final KeyValueService primary;
    private final KeyValueService secondary;
    private final ExecutorService executor;

    public static TieredKeyValueService create(Set<String> tieredTables,
                                               KeyValueService primary,
                                               KeyValueService secondary) {
        return create(tieredTables, primary, secondary,
                PTExecutors.newCachedThreadPool(PTExecutors.newNamedThreadFactory()));
    }

    public static TieredKeyValueService create(Set<String> tieredTables,
                                               KeyValueService primary,
                                               KeyValueService secondary,
                                               ExecutorService executor) {
        return new TieredKeyValueService(tieredTables, primary, secondary, executor);
    }

    private TieredKeyValueService(Set<String> tieredTables,
                                  KeyValueService primary,
                                  KeyValueService secondary,
                                  ExecutorService executor) {
        Set<String> badTables = Sets.intersection(AtlasDbConstants.hiddenTables, tieredTables);
        Preconditions.checkArgument(badTables.isEmpty(), "The hidden tables %s cannot be tiered.", badTables);
        this.tieredTables = ImmutableSet.copyOf(tieredTables);
        this.primary = primary;
        this.secondary = secondary;
        this.executor = executor;
    }

    /**
     * Returns the primary key value service.
     * <p>
     * All new writes and all operations on tables that are not tiered (including system tables)
     * go only to the primary tier.
     */
    public KeyValueService getPrimaryTier() {
        return primary;
    }


    /**
     * Returns the secondary key value service.
     * <p>
     * The secondary tier is only written to by TieredKvsMover during batch operations
     * that move the contents of tiered tables from the primary tier to the secondary tier.
     */
    public KeyValueService getSecondaryTier() {
        return secondary;
    }

    private boolean isNotTiered(String tableName) {
        if (tieredTables.isEmpty()) {
            return AtlasDbConstants.hiddenTables.contains(tableName);
        }
        return !tieredTables.contains(tableName);
    }

    @Override
    public void initializeFromFreshInstance() {
        primary.initializeFromFreshInstance();
        secondary.initializeFromFreshInstance();
    }

    @Override
    public void close() {
        primary.close();
        secondary.close();
        executor.shutdown();
    }

    @Override
    public void teardown() {
        primary.teardown();
        secondary.teardown();
        executor.shutdown();
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of(primary, secondary);
    }

    public Set<String> getTieredTablenames() {
        if (tieredTables.isEmpty()) {
            return Sets.difference(getAllTableNames(), AtlasDbConstants.hiddenTables);
        } else {
            return tieredTables;
        }
    }

    @Override
    public Map<Cell, Value> getRows(final String tableName,
                                    final Iterable<byte[]> rows,
                                    final ColumnSelection columnSelection,
                                    final long timestamp) {
        if (isNotTiered(tableName)) {
            return primary.getRows(tableName, rows, columnSelection, timestamp);
        }
        Map<Cell, Value> primaryResults = primary.getRows(tableName, rows, columnSelection, timestamp);
        Map<Cell, Value> results = Maps.newHashMap(secondary.getRows(tableName, rows, columnSelection, timestamp));
        results.putAll(primaryResults);
        return results;
    }

    @Override
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        if (isNotTiered(tableName)) {
            return primary.get(tableName, timestampByCell);
        }
        Map<Cell, Value> results;
        Map<Cell, Value> primaryResults = primary.get(tableName, timestampByCell);
        if (primaryResults.size() == timestampByCell.size()) {
            results = primaryResults;
        } else {
            Map<Cell, Long> missingCells = Maps.newHashMapWithExpectedSize(timestampByCell.size() - primaryResults.size());
            results = Maps.newHashMapWithExpectedSize(timestampByCell.size());
            for (Entry<Cell, Long> entry : timestampByCell.entrySet()) {
                Value value = primaryResults.get(entry.getKey());
                if (value == null) {
                    missingCells.put(entry.getKey(), entry.getValue());
                } else {
                    results.put(entry.getKey(), value);
                }
            }
            results.putAll(secondary.get(tableName, missingCells));
        }
        return results;
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> timestampByCell) {
        if (isNotTiered(tableName)) {
            return primary.getLatestTimestamps(tableName, timestampByCell);
        }
        Map<Cell, Long> results;
        Map<Cell, Long> primaryResults = primary.getLatestTimestamps(tableName, timestampByCell);
        if (primaryResults.size() == timestampByCell.size()) {
            results = primaryResults;
        } else {
            Map<Cell, Long> missingCells = Maps.newHashMapWithExpectedSize(timestampByCell.size() - primaryResults.size());
            results = Maps.newHashMapWithExpectedSize(timestampByCell.size());
            for (Entry<Cell, Long> entry : timestampByCell.entrySet()) {
                Long timestamp = primaryResults.get(entry.getKey());
                if (timestamp == null) {
                    missingCells.put(entry.getKey(), entry.getValue());
                } else {
                    results.put(entry.getKey(), timestamp);
                }
            }
            results.putAll(secondary.getLatestTimestamps(tableName, missingCells));
        }
        return results;
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(final String tableName,
                                                 final Set<Cell> cells,
                                                 final long timestamp) {
        if (isNotTiered(tableName)) {
            return primary.getAllTimestamps(tableName, cells, timestamp);
        }
        Multimap<Cell, Long> primaryResults = primary.getAllTimestamps(tableName, cells, timestamp);
        Multimap<Cell, Long> results = HashMultimap.create(secondary.getAllTimestamps(tableName, cells, timestamp));
        results.putAll(primaryResults);
        return results;
    }

    @Override
    public void truncateTable(final String tableName) {
        if (isNotTiered(tableName)) {
            primary.truncateTable(tableName);
            return;
        }
        Future<?> primaryFuture = executor.submit(new Runnable() {
            @Override
            public void run() {
                primary.truncateTable(tableName);
            }
        });
        secondary.truncateTable(tableName);
        Futures.getUnchecked(primaryFuture);
    }

    @Override
    public void truncateTables(final Set<String> tableNames) {
        final Set<String> truncateOnPrimary = Sets.newHashSet();
        final Set<String> truncateOnSecondary = Sets.newHashSet();

        for (String tableName : tableNames) {
            if (isNotTiered(tableName)) {
                truncateOnPrimary.add(tableName);
            } else {
                truncateOnPrimary.add(tableName);
                truncateOnSecondary.add(tableName);
            }

            Future<?> primaryFuture = executor.submit(new Runnable() {
                @Override
                public void run() {
                    primary.truncateTables(truncateOnPrimary);
                }
            });
            secondary.truncateTables(truncateOnSecondary);
            Futures.getUnchecked(primaryFuture);
        }
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        primary.put(tableName, values, timestamp);
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        primary.multiPut(valuesByTable, timestamp);
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values) {
        primary.putWithTimestamps(tableName, values);
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values) {
        if (isNotTiered(tableName)) {
            primary.putUnlessExists(tableName, values);
            return;
        }
        throw new UnsupportedOperationException("TieredKeyValueService does not " +
                "support putUnlessExists on tiered tables. tableName=" + tableName + ".");
    }

    @Override
    public void delete(final String tableName, final Multimap<Cell, Long> keys) {
        if (isNotTiered(tableName)) {
            primary.delete(tableName, keys);
            return;
        }
        Future<?> primaryFuture = executor.submit(new Runnable() {
            @Override
            public void run() {
                primary.delete(tableName, keys);
            }
        });
        secondary.delete(tableName, keys);
        Futures.getUnchecked(primaryFuture);
    }

    @Override
    public ClosableIterator<RowResult<Value>> getRange(final String tableName,
                                                       final RangeRequest rangeRequest,
                                                       final long timestamp) {
        if (isNotTiered(tableName)) {
            return primary.getRange(tableName, rangeRequest, timestamp);
        }
        ClosableIterator<RowResult<Value>> primaryIter = primary.getRange(tableName, rangeRequest, timestamp);
        return new ClosableMergedIterator<Value>(rangeRequest, primaryIter,
                new Function<RangeRequest, ClosableIterator<RowResult<Value>>>() {
                    @Override
                    public ClosableIterator<RowResult<Value>> apply(RangeRequest request) {
                        return secondary.getRange(tableName, request, timestamp);
                    }});
    }

    @Override
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(final String tableName,
                                                                       final RangeRequest rangeRequest,
                                                                       final long timestamp) {
        if (isNotTiered(tableName)) {
            return primary.getRangeOfTimestamps(tableName, rangeRequest, timestamp);
        }
        ClosableIterator<RowResult<Set<Long>>> primaryIter = primary.getRangeOfTimestamps(tableName, rangeRequest, timestamp);
        return new ClosableMergedIterator<Set<Long>>(rangeRequest, primaryIter,
                new Function<RangeRequest, ClosableIterator<RowResult<Set<Long>>>>() {
                    @Override
                    public ClosableIterator<RowResult<Set<Long>>> apply(RangeRequest request) {
                        return secondary.getRangeOfTimestamps(tableName, request, timestamp);
                    }});
    }

    @Override
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(final String tableName,
                                                                       final RangeRequest rangeRequest,
                                                                       final long timestamp) {
        if (isNotTiered(tableName)) {
            return primary.getRangeWithHistory(tableName, rangeRequest, timestamp);
        }
        ClosableIterator<RowResult<Set<Value>>> primaryIter = primary.getRangeWithHistory(tableName, rangeRequest, timestamp);
        return new ClosableMergedIterator<Set<Value>>(rangeRequest, primaryIter,
                new Function<RangeRequest, ClosableIterator<RowResult<Set<Value>>>>() {
                    @Override
                    public ClosableIterator<RowResult<Set<Value>>> apply(RangeRequest request) {
                        return secondary.getRangeWithHistory(tableName, request, timestamp);
                    }});
    }

    @Override
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>
            getFirstBatchForRanges(final String tableName,
                                   final Iterable<RangeRequest> rangeRequests,
                                   final long timestamp) {
        if (isNotTiered(tableName)) {
            return primary.getFirstBatchForRanges(tableName, rangeRequests, timestamp);
        }
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> primaryResults =
                primary.getFirstBatchForRanges(tableName, rangeRequests, timestamp);
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> secondaryResults =
                secondary.getFirstBatchForRanges(tableName, rangeRequests, timestamp);
        Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> results =
                Maps.newHashMapWithExpectedSize(primaryResults.size());
        for (Entry<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> entry : primaryResults.entrySet()) {
            RangeRequest rangeRequest = entry.getKey();
            boolean isReversed = rangeRequest.isReverse();
            TokenBackedBasicResultsPage<RowResult<Value>, byte[]> primaryPage = entry.getValue();
            TokenBackedBasicResultsPage<RowResult<Value>, byte[]> secondaryPage = secondaryResults.get(rangeRequest);
            boolean moreAvailable = primaryPage.moreResultsAvailable() || secondaryPage.moreResultsAvailable();
            byte[] pageToken = getNextPageToken(primaryPage, secondaryPage, isReversed);
            Predicate<RowResult<Value>> limiter = getLimitingPredicate(pageToken, isReversed, moreAvailable);
            Iterable<RowResult<Value>> pageResults = getMergedPageResults(primaryPage, secondaryPage, limiter, isReversed);
            results.put(rangeRequest, SimpleTokenBackedResultsPage.create(pageToken, pageResults, moreAvailable));
        }
        return results;
    }

    /**
     * Returns the smaller of the page tokens from the given pages (or greater if isReversed).
     */
    private byte[] getNextPageToken(TokenBackedBasicResultsPage<RowResult<Value>, byte[]> primaryPage,
                                    TokenBackedBasicResultsPage<RowResult<Value>, byte[]> secondaryPage,
                                    boolean isReversed) {
        if (!primaryPage.moreResultsAvailable()) {
            return secondaryPage.getTokenForNextPage();
        } else if (!secondaryPage.moreResultsAvailable()) {
            return primaryPage.getTokenForNextPage();
        } else {
            if (compare(primaryPage.getTokenForNextPage(), secondaryPage.getTokenForNextPage(), isReversed) < 0) {
                return primaryPage.getTokenForNextPage();
            } else {
                return secondaryPage.getTokenForNextPage();
            }
        }
    }

    /**
     * Returns a predicate accepting only row results with row names less than or equal to the given
     * page token (or greater than or equal if isReversed).
     */
    private Predicate<RowResult<Value>> getLimitingPredicate(final byte[] pageToken,
                                                             final boolean isReversed,
                                                             boolean moreAvailable) {
        if (!moreAvailable) {
            return Predicates.alwaysTrue();
        }
        return new Predicate<RowResult<Value>>() {
            @Override
            public boolean apply(RowResult<Value> rowResult) {
                return compare(rowResult.getRowName(), pageToken, isReversed) <= 0;
            }
        };
    }

    /**
     * Return a lexicographical comparison of the byte arrays, reversing the results if
     * reverseOrdering is true.
     */
    private static int compare(byte[] first, byte[] second, boolean reverseOrdering) {
        int comparison = UnsignedBytes.lexicographicalComparator().compare(first, second);
        if (reverseOrdering) {
            return -comparison;
        } else {
            return comparison;
        }
    }

    /**
     * Returns the merged page results from both pages in the proper order, limited by the given predicate.
     */
    private Iterable<RowResult<Value>> getMergedPageResults(
            final TokenBackedBasicResultsPage<RowResult<Value>, byte[]> primaryPage,
            final TokenBackedBasicResultsPage<RowResult<Value>, byte[]> secondaryPage,
            final Predicate<RowResult<Value>> limiter,
            final boolean isReversed) {
        return new Iterable<RowResult<Value>>() {
            @Override
            public Iterator<RowResult<Value>> iterator() {
                return mergeIterators(takeWhile(primaryPage.getResults().iterator(), limiter),
                        takeWhile(secondaryPage.getResults().iterator(), limiter), isReversed);
            }
        };
    }

    private static <T> Iterator<RowResult<T>> mergeIterators(Iterator<RowResult<T>> primaryIter,
                                                             Iterator<RowResult<T>> secondaryIter,
                                                             boolean isReversed) {
        Ordering<RowResult<T>> comparator = RowResult.getOrderingByRowName();
        if (isReversed) {
            comparator = comparator.reverse();
        }
        return IteratorUtils.mergeIterators(
                primaryIter,
                secondaryIter,
                comparator,
                new Function<Pair<RowResult<T>, RowResult<T>>, RowResult<T>>() {
                    @Override
                    public RowResult<T> apply(Pair<RowResult<T>, RowResult<T>> rows) {
                        RowResult<T> primaryResult = rows.getLhSide();
                        RowResult<T> secondaryResult = rows.getRhSide();
                        for (Entry<byte[], T> entry : primaryResult.getColumns().entrySet()) {
                            T secondaryValue = secondaryResult.getColumns().get(entry.getKey());
                            if (secondaryValue instanceof Value) {
                                long primaryTimestamp = ((Value)entry.getValue()).getTimestamp();
                                long secondaryTimestamp = ((Value)secondaryValue).getTimestamp();
                                AssertUtils.assertAndLog(primaryTimestamp >= secondaryTimestamp,
                                        "The secondary kvs has a row with timestamp %s, while the " +
                                        "same row in the primary has timestamp %s. This should be " +
                                        "extremely uncommon.",
                                        secondaryTimestamp, primaryTimestamp);
                            }
                        }
                        // Order is important here, overwrite results from the
                        // secondary tier with results from the primary tier.
                        return RowResults.merge(secondaryResult, primaryResult);
                    }
                });
    }

    /**
     * Returns an iterator that returns elements from the delegate until the predicate returns false.
     */
    private static <T> Iterator<T> takeWhile(final Iterator<T> delegate, final Predicate<T> predicate) {
        return new AbstractIterator<T>() {
            @Override
            protected T computeNext() {
                if (!delegate.hasNext()) {
                    return endOfData();
                }
                T next = delegate.next();
                if (predicate.apply(next)) {
                    return next;
                }
                return endOfData();
            }
        };
    }

    @Override
    public void dropTable(final String tableName) {
        dropTables(ImmutableSet.of(tableName));
    }

    @Override
    public void dropTables(Set<String> tableNames) {
        Map<KeyValueService, Set<String>> tableNamesPerDelegate = Maps.newHashMapWithExpectedSize(2);
        for (String tableName : tableNames) {
            Set<String> splitTableNames;

            // always place in primary
            if (tableNamesPerDelegate.containsKey(primary)) {
                splitTableNames = tableNamesPerDelegate.get(primary);
            } else {
                splitTableNames = Sets.newHashSet();
            }
            splitTableNames.add(tableName);
            tableNamesPerDelegate.put(primary, splitTableNames);

            if (!isNotTiered(tableName)) { // if tiered also place in secondary
                if (tableNamesPerDelegate.containsKey(secondary)) {
                    splitTableNames = tableNamesPerDelegate.get(secondary);
                } else {
                    splitTableNames = Sets.newHashSet();
                }
                splitTableNames.add(tableName);
                tableNamesPerDelegate.put(secondary, splitTableNames);
            }
        }

        List<Future<?>> futures = Lists.newArrayListWithExpectedSize(2);
        for (final Entry<KeyValueService, Set<String>> tableNamesPerKVS : tableNamesPerDelegate.entrySet()) {
            futures.add(executor.submit(new Runnable() {
                @Override
                public void run() {
                    tableNamesPerKVS.getKey().dropTables(tableNamesPerKVS.getValue());
                }
            }));
        }

        for (Future<?> future : futures) {
            Futures.getUnchecked(future);
        }
    }

    @Override
    public void createTable(final String tableName, final byte[] tableMetadata) {
        if (isNotTiered(tableName)) {
            primary.createTable(tableName, tableMetadata);
            return;
        }
        primary.createTable(tableName, tableMetadata);
        secondary.createTable(tableName, tableMetadata);
    }

    @Override
    public void createTables(Map<String, byte[]> tableNameToTableMetadata) {
        Map<KeyValueService, Map<String, byte[]>> delegateToTableMetadata = Maps.newHashMapWithExpectedSize(2);
        for (Entry<String, byte[]> tableEntry : tableNameToTableMetadata.entrySet()) {
            String tableName = tableEntry.getKey();
            byte[] metadata = tableEntry.getValue();
            Map<String, byte[]> splitTableToMetadata = ImmutableMap.of();

            // always place in primary
            if (delegateToTableMetadata.containsKey(primary)) {
                splitTableToMetadata = delegateToTableMetadata.get(primary);
            } else {
                splitTableToMetadata = Maps.newHashMap();
            }
            splitTableToMetadata.put(tableName, metadata);
            delegateToTableMetadata.put(primary, splitTableToMetadata);

            if (!isNotTiered(tableName)) { // if tiered also place in secondary
                if (delegateToTableMetadata.containsKey(secondary)) {
                    splitTableToMetadata = delegateToTableMetadata.get(secondary);
                } else {
                    splitTableToMetadata = Maps.newHashMap();
                }
                splitTableToMetadata.put(tableName, metadata);
                delegateToTableMetadata.put(secondary, splitTableToMetadata);
            }
        }

        for (KeyValueService kvs : delegateToTableMetadata.keySet()) {
            kvs.createTables(delegateToTableMetadata.get(kvs));
        }
    }

    @Override
    public Set<String> getAllTableNames() {
        return primary.getAllTableNames();
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        return primary.getMetadataForTable(tableName);
    }

    @Override
    public Map<String, byte[]> getMetadataForTables() {
        return primary.getMetadataForTables();
    }

    @Override
    public void putMetadataForTable(final String tableName, final byte[] metadata) {
        if (isNotTiered(tableName)) {
            primary.putMetadataForTable(tableName, metadata);
            return;
        }
        primary.putMetadataForTable(tableName, metadata);
        secondary.putMetadataForTable(tableName, metadata);
    }

    @Override
    public void putMetadataForTables(Map<String, byte[]> tableNameToMetadata) {
        Map<KeyValueService, Map<String, byte[]>> delegateToTablenameToMetadata = Maps.newHashMapWithExpectedSize(2);
        for (Entry<String, byte[]> tableEntry : tableNameToMetadata.entrySet()) {
            String tableName = tableEntry.getKey();
            byte[] metadata = tableEntry.getValue();
            Map<String, byte[]> splitTableToMetadata = ImmutableMap.of();

            // always place in primary
            if (delegateToTablenameToMetadata.containsKey(primary)) {
                splitTableToMetadata = delegateToTablenameToMetadata.get(primary);
            } else {
                splitTableToMetadata = Maps.newHashMap();
            }
            splitTableToMetadata.put(tableName, metadata);
            delegateToTablenameToMetadata.put(primary, splitTableToMetadata);

            if (!isNotTiered(tableName)) {
                if (delegateToTablenameToMetadata.containsKey(secondary)) {
                    splitTableToMetadata = delegateToTablenameToMetadata.get(secondary);
                } else {
                    splitTableToMetadata = Maps.newHashMap();
                }
                splitTableToMetadata.put(tableName, metadata);
                delegateToTablenameToMetadata.put(secondary, splitTableToMetadata);
            }
        }
        for (KeyValueService kvs : delegateToTablenameToMetadata.keySet()) {
            kvs.putMetadataForTables(delegateToTablenameToMetadata.get(kvs));
        }
    }

    @Override
    public void addGarbageCollectionSentinelValues(final String tableName, final Set<Cell> cells) {
        secondary.addGarbageCollectionSentinelValues(tableName, cells);
    }

    public static Collection<TieredKeyValueService> getTieredServices(KeyValueService kvs) {
        List<TieredKeyValueService> tieredServices = Lists.newArrayList();
        if (kvs instanceof TieredKeyValueService) {
            tieredServices.add((TieredKeyValueService) kvs);
        }
        for (KeyValueService delegate : kvs.getDelegates()) {
            tieredServices.addAll(getTieredServices(delegate));
        }
        return tieredServices;
    }

    @Override
    public void compactInternally(String tableName) {
        throw new UnsupportedOperationException();
    }
}
