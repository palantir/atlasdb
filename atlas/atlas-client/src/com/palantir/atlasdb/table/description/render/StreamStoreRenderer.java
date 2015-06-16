// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.table.description.render;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ExpirationStrategy;
import com.palantir.atlasdb.stream.ExpiringStreamStore;
import com.palantir.atlasdb.stream.PersistentStreamStore;
import com.palantir.atlasdb.stream.TransactionBackedRunner;
import com.palantir.atlasdb.stream.TransactionManagerBackedRunner;
import com.palantir.atlasdb.stream.TransactionRunner;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.base.Throwables;
import com.palantir.common.io.ConcatenatedInputStream;
import com.palantir.util.AssertUtils;
import com.palantir.util.ByteArrayIOStream;
import com.palantir.util.crypto.Sha256Hash;
import com.palantir.util.file.DeleteOnCloseFileInputStream;
import com.palantir.util.file.FileUtils;

public class StreamStoreRenderer {
    private final String name;
    private final ValueType streamIdType;
    private final String packageName;
    private final String schemaName;
    private final int inMemoryThreshold;
    private final ExpirationStrategy expirationStrategy;

    public StreamStoreRenderer(String name, ValueType streamIdType, String packageName, String schemaName, int inMemoryThreshold, ExpirationStrategy expirationStrategy) {
        this.name = name;
        this.streamIdType = streamIdType;
        this.packageName = packageName;
        this.schemaName = schemaName;
        this.inMemoryThreshold = inMemoryThreshold;
        this.expirationStrategy = expirationStrategy;
    }

    public String getPackageName() {
        return packageName;
    }

    public String getStreamStoreClassName() {
        return name + "StreamStore";
    }

    public String getIndexCleanupTaskClassName() {
        return name + "IndexCleanupTask";
    }

    public String getMetadataCleanupTaskClassName() {
        return name + "MetadataCleanupTask";
    }

    public String renderStreamStore() {
        final String StreamStore = name + "StreamStore";

        final String StreamValueTable = name + "StreamValueTable";
        final String StreamValueRow = StreamValueTable + "." + name + "StreamValueRow";

        final String StreamMetadataTable = name + "StreamMetadataTable";
        final String StreamMetadataRow = StreamMetadataTable + "." + name + "StreamMetadataRow";

        final String StreamHashAidxTable = name + "StreamHashAidxTable";
        final String StreamHashAidxRow = StreamHashAidxTable + "." + name + "StreamHashAidxRow";
        final String StreamHashAidxColumn = StreamHashAidxTable + "." + name + "StreamHashAidxColumn";
        final String StreamHashAidxColumnValue = StreamHashAidxTable + "." + name + "StreamHashAidxColumnValue";

        final String StreamIdxTable = name + "StreamIdxTable";
        final String StreamIdxRow = StreamIdxTable + "." + name + "StreamIdxRow";
        final String StreamIdxColumn = StreamIdxTable + "." + name + "StreamIdxColumn";
        final String StreamIdxColumnValue = StreamIdxTable + "." + name + "StreamIdxColumnValue";

        final String TableFactory = schemaName + "TableFactory";
        final String StreamId = streamIdType.getJavaObjectClassName();

        return new Renderer() {
            @Override
            protected void run() {
                ImportRenderer importRenderer = new ImportRenderer(this,
                        Arrays.asList(IMPORTS));
                _("package ", packageName, ";");
                _();
                importRenderer.renderImports();
                _();
                _("public class ", StreamStore, " implements ", (isExpiring() ? "ExpiringStreamStore" : "PersistentStreamStore"), "<", StreamId, "> {"); {
                    fields();
                    _();
                    constructors();
                    _();
                    _("// =========================================================================");
                    _("// General Storing Helpers");
                    _("// =========================================================================");
                    _();
                    storeBlocksAndGetFinalMetadata();
                    _();
                    _("// General Storing Helpers: Storing blocks");
                    _("// =========================================================================");
                    _();
                    storeBlocksFromStream();
                    _();
                    storeBlock();
                    _();
                    touchMetadataWhileStoringForConflicts();
                    _();
                    _("// General Storing Helpers: Storing Metadata");
                    _("// =========================================================================");
                    _();
                    storeMetadataAndIndex();
                    _();
                    putMetadataAndHashIndexTask();
                    _();
                    _("// General Storing Helpers");
                    _("// =========================================================================");
                    _();
                    getNumberOfBlocksFromMetadata();
                    _();
                    _("// =========================================================================");
                    _("// Store Streams");
                    _("// =========================================================================");
                    _();
                    storeStream();
                    _();
                    storeStreams();
                    _();
                    getEmptyMetadata();
                    _();
                    _("// =========================================================================");
                    _("// Load Streams");
                    _("// =========================================================================");
                    _();
                    loadStreams();
                    _();
                    loadStream();
                    _();
                    loadStreamAsFile();
                    _();
                    loadToNewTempFile();
                    _();
                    _("// Load Streams: checking metadata");
                    _("// =========================================================================");
                    _();
                    checkStreamStored();
                    _();
                    _("// Load Streams: File writing methods");
                    _("// =========================================================================");
                    _();
                    createTempFile();
                    _();
                    writeStreamToFile();
                    _();
                    tryWriteStreamToFile();
                    _();
                    writeSingleBlockToOutputStream();
                    _();
                    _("// Load Streams: Getting blocks");
                    _("// =========================================================================");
                    _();
                    getBlockTask();
                    _();
                    _("// Load Streams: Getting metadata");
                    _("// =========================================================================");
                    _();
                    getMetadata();
                    _();
                    getMetadataTask();
                    _();
                    getMetadataTasks();
                    _();
                    _("// =========================================================================");
                    _("// Stuff related to hash indexing");
                    _("// =========================================================================");
                    _();
                    lookupStreamsByHash();
                    _();
                    getHashIndexRowsForHashes();
                    _();
                    getMetadataRowsForIds();
                    _();
                    putHashIndexTask();
                    _();
                    deleteStreams();
                    if (!isExpiring()) {
                        _();
                        markStreamsAsUsed();
                        _();
                        removeStreamsAsUsed();
                        _();
                        touchMetadataWhileMarkingUsedForConflicts();
                    }
                    _();
                    importRenderer.renderImportJavaDoc();
                    _("static final int dummy = 0;");
                } _("}");
            }

            private void fields() {
                _("public static final int BLOCK_SIZE_IN_BYTES = 1000000; // 1MB. DO NOT CHANGE THIS WITHOUT AN UPGRADE TASK");
                _("public static final int IN_MEMORY_THRESHOLD = ", String.valueOf(inMemoryThreshold), "; // streams under this size are kept in memory when loaded");
                _("public static final String STREAM_FILE_PREFIX = \"", name, "_stream_\";");
                _("public static final String STREAM_FILE_SUFFIX = \".tmp\";");
                _();
                _("private static final Logger log = LoggerFactory.getLogger(", StreamStore, ".class);");
                _();
                _("private final TransactionRunner txRunner;");
                _("private final ", TableFactory, " tables;");
            }

            private void constructors() {
                _("private ", StreamStore, "(TransactionManager txManager, ", TableFactory, " tables) {"); {
                    _("this.txRunner = new TransactionManagerBackedRunner(txManager);");
                    _("this.tables = tables;");
                } _("}");
                _();
                _("private ", StreamStore, "(Transaction t, ", TableFactory, " tables) {"); {
                    _("this.txRunner = new TransactionBackedRunner(t);");
                    _("this.tables = tables;");
                } _("}");
                _();
                _("public static ", StreamStore, " of(TransactionManager txManager, ", TableFactory, " tables) {"); {
                    _("return new ", StreamStore, "(txManager, tables);");
                } _("}");
                _();
                _("/**");
                _(" * This should only be used by test code or as a performance optimization.");
                _(" */");
                _("public static ", StreamStore, " of(final Transaction t, ", TableFactory, " tables) {"); {
                    _("return new ", StreamStore, "(t, tables);");
                } _("}");
            }

            private void storeBlocksAndGetFinalMetadata() {
                String params = isExpiring() ? ", long duration, TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                _("/**");
                _(" * @return metadata, the final metadata that should be stored into the database if the stream hash is correct and unique");
                _(" */");
                _("private StreamMetadata storeBlocksAndGetFinalMetadata(", StreamId, " id, InputStream stream", params, ") {"); {
                    _("// Set up for finding hash and length");
                    _("MessageDigest digest = Sha256Hash.getMessageDigest();");
                    _("stream = new DigestInputStream(stream, digest);");
                    _("CountingInputStream countingStream = new CountingInputStream(stream);");
                    _();
                    _("// Try to store the bytes to the stream and get length");
                    _("try {"); {
                        _("storeBlocksFromStream(id, countingStream", args, ");");
                    } _("} catch (IOException e) {"); {
                        _("long length = countingStream.getCount();");
                        _("StreamMetadata metadata = StreamMetadata.newBuilder()");
                        _("    .setStatus(Status.FAILED)");
                        _("    .setLength(length)");
                        _("    .setHash(com.google.protobuf.ByteString.EMPTY)");
                        _("    .build();");
                        _("storeMetadataAndIndex(id, metadata", args, ");");
                        _("log.error(\"Could not store stream \" + id + \". Failed after \" + length + \" bytes.\", e);");
                        _("throw Throwables.rewrapAndThrowUncheckedException(\"Failed to store stream.\", e);");
                    } _("}");
                    _();
                    _("// Get hash and length");
                    _("ByteString hashByteString = ByteString.copyFrom(digest.digest());");
                    _("long length = countingStream.getCount();");
                    _();
                    _("// Return the final metadata.");
                    _("StreamMetadata metadata = StreamMetadata.newBuilder()");
                    _("    .setStatus(Status.STORED)");
                    _("    .setLength(length)");
                    _("    .setHash(hashByteString)");
                    _("    .build();");
                    _("return metadata;");
                } _("}");
            }

            private void storeBlocksFromStream() {
                String params = isExpiring() ? ", long duration, TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                _("private void storeBlocksFromStream(", StreamId, " id, InputStream stream", params, ") throws IOException {"); {
                    _("// We need to use a buffered stream here because we assume each read will fill the whole buffer.");
                    _("stream = new BufferedInputStream(stream);");
                    _("byte[] bytesToStore = new byte[BLOCK_SIZE_IN_BYTES];");
                    _("long blockNumber = 0;");
                    _();
                    _("// Read initial block.");
                    _("int length = stream.read(bytesToStore);");
                    _();
                    _("// Stops when there is nothing left to read.");
                    _("while (length != -1) {"); {
                        _("// Store only relevant data if it only filled a partial block");
                        _("if (length < BLOCK_SIZE_IN_BYTES) {"); {
                            _("bytesToStore = Arrays.copyOf(bytesToStore, length);");
                        } _("}");
                        _();
                        _("// Store block to database.");
                        _("storeBlock(", StreamValueRow, ".of(id, blockNumber), bytesToStore", args, ");");
                        _();
                        _("// Get a new block.");
                        _("blockNumber++;");
                        _("bytesToStore = new byte[BLOCK_SIZE_IN_BYTES];");
                        _("length = stream.read(bytesToStore);");
                    } _("}");
                } _("}");
            }

            private void storeBlock() {
                String params = isExpiring() ? ", final long duration, final TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                _("private void storeBlock(final ", StreamValueRow, " row, final byte[] block", params, ") {"); {
                    _("Preconditions.checkArgument(block.length <= BLOCK_SIZE_IN_BYTES, \"Block to store in DB must be less than BLOCK_SIZE_IN_BYTES\");");
                    _();
                    _("try {"); {
                        _("txRunner.run(new TransactionTask<Void, RuntimeException>() {"); {
                            _("@Override");
                            _("public Void execute(Transaction t) {"); {
                                _("// Do a touch operation on this table to ensure we get a conflict if someone cleans it up.");
                                _("touchMetadataWhileStoringForConflicts(t, row.getId(), row.getBlockId()", args, ");");
                                _("tables.get", StreamValueTable, "(t).putValue(row, block", args, ");");
                                _("return null;");
                            } _("}");
                        } _("});");
                    } _("} catch (RuntimeException e) {"); {
                        _("log.error(\"Error storing block \" + row.getBlockId() + \" for stream id \" + row.getId(), e);");
                        _("throw e;");
                    } _("}");
                } _("}");
            }

            private void touchMetadataWhileStoringForConflicts() {
                String params = isExpiring() ? ", long duration, TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                _("private void touchMetadataWhileStoringForConflicts(Transaction t, ", StreamId, " id, long blockNumber", params, ") {"); {
                    _(StreamMetadataTable, " metaTable = tables.get", StreamMetadataTable, "(t);");
                    _(StreamMetadataRow, " row = ", StreamMetadataRow, ".of(id);");
                    _("StreamMetadata metadata = metaTable.getMetadatas(ImmutableSet.of(row)).values().iterator().next();");
                    _("Preconditions.checkState(metadata.getStatus() == Status.STORING, \"This stream is being cleaned up while storing blocks: \" + id);");
                    _("Builder builder = StreamMetadata.newBuilder(metadata);");
                    _("builder.setLength(blockNumber * BLOCK_SIZE_IN_BYTES + 1);");
                    _("metaTable.putMetadata(row, builder.build()", args, ");");
                } _("}");
            }

            private void storeMetadataAndIndex() {
                String params = isExpiring() ? ", final long duration, final TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                _("private void storeMetadataAndIndex(final ", StreamId, " streamId, final StreamMetadata metadata", params, ") {"); {
                    _("txRunner.run(new TransactionTask<Void, RuntimeException>() {"); {
                        _("@Override");
                        _("public Void execute(Transaction t) {"); {
                            _(StreamMetadataRow, " row = ", StreamMetadataRow, ".of(streamId);");
                            _("putMetadataAndHashIndexTask(t, row, metadata", args, ");");
                            _("return null;");
                        } _("}");
                    } _("});");
                } _("}");
            }

            private void putMetadataAndHashIndexTask() {
                String params = isExpiring() ? ", long duration, TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                _("private void putMetadataAndHashIndexTask(Transaction t, ", StreamMetadataRow, " row, StreamMetadata metadata", params, ") {"); {
                    _(StreamMetadataTable, " mdTable = tables.get", StreamMetadataTable, "(t);");
                    _("if (metadata.getStatus() == Status.STORED) {"); {
                        _("StreamMetadata prevMetadata = getMetadataTask(t, row);");
                        _("if (prevMetadata == null || prevMetadata.getStatus() != Status.STORING) {"); {
                            _("// This can happen if we cleanup old streams.");
                            _("throw new IllegalStateException(\"Cannot mark a stream as stored that isn't currently storing: \" + prevMetadata);");
                        } _("}");
                        _("putHashIndexTask(t, row, metadata", args, ");");
                    } _("} else if (metadata.getStatus() == Status.STORING) {"); {
                        _("StreamMetadata prevMetadata = getMetadataTask(t, row);");
                        _("// This will prevent two users trying to store the same id.");
                        _("if (prevMetadata != null) {"); {
                            _("throw new IllegalStateException(\"Cannot reuse the same stream id: \" + row.getId());");
                        } _("}");
                    } _("}");
                    _();
                    _("mdTable.putMetadata(row, metadata", args, ");");
                } _("}");
            }

            private void getNumberOfBlocksFromMetadata() {
                _("private long getNumberOfBlocksFromMetadata(StreamMetadata metadata) {"); {
                    _("return (metadata.getLength() + BLOCK_SIZE_IN_BYTES - 1) / BLOCK_SIZE_IN_BYTES;");
                } _("}");
            }

            private void storeStream() {
                String params = isExpiring() ? ", long duration, TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                _("@Override");
                _("public Sha256Hash storeStream(", StreamId, " id, InputStream stream", params, ") {"); {
                    _("// Store empty metadata before doing anything --- this is will be useful");
                    _("// once we implement stream garbage collection.");
                    _("storeMetadataAndIndex(id, getEmptyMetadata()", args, ");");
                    _();
                    _("StreamMetadata metadata = storeBlocksAndGetFinalMetadata(id, stream", args, ");");
                    _("storeMetadataAndIndex(id, metadata", args, ");");
                    _("return new Sha256Hash(metadata.getHash().toByteArray());");
                } _("}");
            }

            private void storeStreams() {
                String params = isExpiring() ? ", final long duration, final TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                _("@Override");
                _("public Map<", StreamId, ", Sha256Hash> storeStreams(final Map<", StreamId, ", InputStream> streams", params, ") {"); {
                    _("if (streams.isEmpty()) {"); {
                        _("return ImmutableMap.of();");
                    } _("}");
                    _("txRunner.run(new TransactionTask<Void, RuntimeException>() {"); {
                        _("@Override");
                        _("public Void execute(Transaction t) {"); {
                            _(StreamMetadataTable, " table = tables.get", StreamMetadataTable, "(t);");
                            _("Set<", StreamMetadataRow, "> rows = getMetadataRowsForIds(streams.keySet());");
                            _("Map<", StreamMetadataRow, ", StreamMetadata> existingMetadata = table.getMetadatas(rows);");
                            _("if (!existingMetadata.isEmpty()) {"); {
                                _("throw new IllegalStateException(\"Cannot reuse existing stream ids: \" + existingMetadata.keySet());");
                            } _("}");
                            _("Map<", StreamMetadataRow, ", ", StreamMetadataTable, ".Metadata> metadata = Maps.newHashMap();");
                            _("StreamMetadata emptyMetadata = getEmptyMetadata();");
                            _("for (", StreamMetadataRow, " row : rows) {"); {
                                _("metadata.put(row, ", StreamMetadataTable, ".Metadata.of(emptyMetadata));");
                            } _("}");
                            _("table.put(Multimaps.forMap(metadata)", args, ");");
                            _("return null;");
                        } _("}");
                    } _("});");
                    _("Map<", StreamId, ", Sha256Hash> hashes = Maps.newHashMap();");
                    _("for (Entry<", StreamId, ", InputStream> entry : streams.entrySet()) {"); {
                        _(StreamId, " id = entry.getKey();");
                        _("InputStream stream = entry.getValue();");
                        _("StreamMetadata metadata = storeBlocksAndGetFinalMetadata(id, stream", args, ");");
                        _("storeMetadataAndIndex(id, metadata", args, ");");
                        _("hashes.put(id, new Sha256Hash(metadata.getHash().toByteArray()));");
                    } _("}");
                    _("return hashes;");
                } _("}");
            }

            private void getEmptyMetadata() {
                _("private StreamMetadata getEmptyMetadata() {"); {
                    _("return StreamMetadata.newBuilder()");
                    _("    .setStatus(Status.STORING)");
                    _("    .setLength(0L)");
                    _("    .setHash(com.google.protobuf.ByteString.EMPTY)");
                    _("    .build();");
                } _("}");
            }

            private void loadStreams() {
                _("@Override");
                _("public Map<", StreamId, ", InputStream> loadStreams(final Set<", StreamId, "> ids) {"); {
                    _("if (ids.isEmpty()) {"); {
                        _("return ImmutableMap.of();");
                    } _("}");
                    _("Map<", StreamId, ", StreamMetadata> metadata = txRunner.run(");
                    _("        new TransactionTask<Map<", StreamId, ", StreamMetadata>, RuntimeException>() {"); {
                        _("@Override");
                        _("public Map<", StreamId, ", StreamMetadata> execute(Transaction t) {"); {
                            _("return getMetadataTask(t, ids);");
                        } _("}");
                    } _("});");
                    _("final Collection<", StreamValueRow, "> rows = Lists.newArrayListWithExpectedSize(metadata.size());");
                    _("for (Entry<", StreamId, ", StreamMetadata> entry : metadata.entrySet()) {"); {
                        _(StreamId, " id = entry.getKey();");
                        _("StreamMetadata m = checkStreamStored(id, entry.getValue());");
                        _("long numBlocks = getNumberOfBlocksFromMetadata(m);");
                        _("for (int blockId = 0; blockId < numBlocks; blockId++) {"); {
                            _("rows.add(", StreamValueRow, ".of(id, blockId));");
                        } _("}");
                    } _("}");
                    _("Map<", StreamValueRow, ", byte[]> values = txRunner.run(");
                    _("        new TransactionTask<Map<", StreamValueRow, ", byte[]>, RuntimeException>() {"); {
                        _("@Override");
                        _("public Map<", StreamValueRow, ", byte[]> execute(Transaction t) {"); {
                            _(StreamValueTable, " valueTable = tables.get", StreamValueTable, "(t);");
                            _("return valueTable.getValues(rows);");
                        } _("}");
                    } _("});");
                    _("Map<", StreamId, ", InputStream> streams = Maps.newHashMapWithExpectedSize(ids.size());");
                    _("for (Entry<", StreamId, ", StreamMetadata> entry : metadata.entrySet()) {"); {
                        _(StreamId, " id = entry.getKey();");
                        _("int numBlocks = (int) getNumberOfBlocksFromMetadata(entry.getValue());");
                        _("Collection<InputStream> fragments = Lists.newArrayListWithCapacity(numBlocks);");
                        _("for (int blockId = 0; blockId < numBlocks; blockId++) {"); {
                            _("fragments.add(new ByteArrayInputStream(values.get(", StreamValueRow, ".of(id, blockId))));");
                        } _("}");
                        _("streams.put(id, new ConcatenatedInputStream(fragments));");
                    } _("}");
                    _("return streams;");
                } _("}");
            }

            private void loadStream() {
                _("@Override");
                _("public InputStream loadStream(final ", StreamId, " id) {"); {
                    _("try {"); {
                        _("StreamMetadata metadata = checkStreamStored(id, getMetadata(", StreamMetadataRow, ".of(id)));");
                        _("if (metadata.getLength() == 0) {"); {
                            _("return new ByteArrayInputStream(new byte[0]);");
                        } _("} else if (metadata.getLength() <= Math.min(IN_MEMORY_THRESHOLD, BLOCK_SIZE_IN_BYTES)) {"); {
                            _("ByteArrayIOStream ios = new ByteArrayIOStream(Ints.saturatedCast(metadata.getLength()));");
                            _("writeSingleBlockToOutputStream(", StreamValueRow, ".of(id, 0), ios);");
                            _("return ios.getInputStream();");
                        } _("} else {"); {
                            _("File file = loadToNewTempFile(id, metadata);");
                            _("return new DeleteOnCloseFileInputStream(file);");
                        } _("}");
                    } _("} catch (FileNotFoundException e) {"); {
                        _("log.error(\"Error opening temp file for stream \" + id, e);");
                        _("throw Throwables.rewrapAndThrowUncheckedException(\"Could not open temp file to create stream.\", e);");
                    } _("}");
                } _("}");
            }

            private void loadStreamAsFile() {
                _("@Override");
                _("public File loadStreamAsFile(", StreamId, " id) {"); {
                    _("StreamMetadata metadata = checkStreamStored(id, getMetadata(", StreamMetadataRow, ".of(id)));");
                    _("return loadToNewTempFile(id, metadata);");
                } _("}");
            }

            private void loadToNewTempFile() {
                _("private File loadToNewTempFile(", StreamId, " id, StreamMetadata metadata) {"); {
                    _("try {"); {
                        _("File file = createTempFile(id);");
                        _("writeStreamToFile(id, metadata, file);");
                        _("return file;");
                    } _("} catch (IOException e) {"); {
                        _("log.error(\"Could not create temp file for stream id \" + id, e);");
                        _("throw Throwables.rewrapAndThrowUncheckedException(\"Could not create file to create stream.\", e);");
                    } _("}");
                } _("}");
            }

            private void checkStreamStored() {
                _("private StreamMetadata checkStreamStored(", StreamId, " id, StreamMetadata metadata) {"); {
                    _("if (metadata == null) {"); {
                        _("log.error(\"Error loading stream \" + id + \" because it was never stored.\");");
                        _("throw new IllegalArgumentException(\"Unable to load stream \" + id + \" because it was never stored.\");");
                    } _("} else if (metadata.getStatus() != Status.STORED) {"); {
                        _("log.error(\"Error loading stream \" + id + \" because it has status \" + metadata.getStatus());");
                        _("throw new IllegalArgumentException(\"Could not get stream because it was not fully stored.\");");
                    } _("}");
                    _("return metadata;");
                } _("}");
            }

            private void createTempFile() {
                _("private File createTempFile(", StreamId, " id) throws IOException {"); {
                    _("File file = FileUtils.createTempFile(STREAM_FILE_PREFIX + id, STREAM_FILE_SUFFIX);");
                    _("file.deleteOnExit();");
                    _("return file;");
                } _("}");
            }

            private void writeStreamToFile() {
                _("private void writeStreamToFile(", StreamId, " id, StreamMetadata metadata, File file) throws FileNotFoundException {"); {
                    _("FileOutputStream fos = new FileOutputStream(file);");
                    _("try {"); {
                        _("tryWriteStreamToFile(id, metadata, fos);");
                    } _("} catch (IOException e) {"); {
                        _("log.error(\"Could not finish streaming blocks to file for stream \" + id, e);");
                        _("throw Throwables.rewrapAndThrowUncheckedException(\"Error writing blocks while opening a stream.\", e);");
                    } _("} finally {"); {
                        _("IOUtils.closeQuietly(fos);");
                    } _("}");
                } _("}");
            }

            private void tryWriteStreamToFile() {
                _("private void tryWriteStreamToFile(", StreamId, " id, StreamMetadata metadata, FileOutputStream fos) throws IOException {"); {
                    _("long numBlocks = getNumberOfBlocksFromMetadata(metadata);");
                    _("for (long i = 0; i < numBlocks; i++) {"); {
                        _("writeSingleBlockToOutputStream(", StreamValueRow, ".of(id, i), fos);");
                    } _("}");
                    _("fos.close();");
                } _("}");
            }

            private void writeSingleBlockToOutputStream() {
                _("private void writeSingleBlockToOutputStream(final ", StreamValueRow, " row, OutputStream os) {"); {
                    _("try {"); {
                        _("// Get block");
                        _("byte[] block = txRunner.run(new TransactionTask<byte[], RuntimeException>() {"); {
                            _("@Override");
                            _("public byte[] execute(Transaction t) {"); {
                                _("return getBlockTask(t, row);");
                            } _("}");
                        } _("});");
                        _();
                        _("os.write(block);");
                    } _("} catch (RuntimeException e) {"); {
                        _("log.error(\"Error getting block \" + row.getBlockId() + \" of stream \" + row.getId(), e);");
                        _("throw e;");
                    } _("} catch (IOException e) {"); {
                        _("log.error(\"Error writing block \" + row.getBlockId() + \" to file when getting stream id \" + row.getId(), e);");
                        _("throw Throwables.rewrapAndThrowUncheckedException(\"Error writing blocks to file when creating stream.\", e);");
                    } _("}");
                } _("}");
            }

            private void getBlockTask() {
                _("private byte[] getBlockTask(Transaction t, ", StreamValueRow, " row) {"); {
                    _(StreamValueTable, " valueTable = tables.get", StreamValueTable, "(t);");
                    _("byte[] block = valueTable.getValues(ImmutableSet.of(row)).get(row);");
                    _("return block;");
                } _("}");
            }

            private void getMetadata() {
                _("private StreamMetadata getMetadata(final ", StreamMetadataRow, " row) {"); {
                    _("try {"); {
                        _("return txRunner.run(new TransactionTask<StreamMetadata, RuntimeException>() {"); {
                            _("@Override");
                            _("public StreamMetadata execute(Transaction t) {"); {
                                _("return getMetadataTask(t, row);");
                            } _("}");
                        } _("});");
                    } _("} catch (RuntimeException e) {"); {
                        _("log.error(\"Error getting metadata for stream id \" + row.getId(), e);");
                        _("throw e;");
                    } _("}");
                } _("}");
            }

            private void getMetadataTask() {
                _("private StreamMetadata getMetadataTask(Transaction t, ", StreamMetadataRow, " row) {"); {
                    _(StreamMetadataTable, " table = tables.get", StreamMetadataTable, "(t);");
                    _("StreamMetadata metadata = table.getMetadatas(ImmutableSet.of(row)).get(row);");
                    _("return metadata;");
                } _("}");
            }

            private void getMetadataTasks() {
                _("private Map<", StreamId, ", StreamMetadata> getMetadataTask(Transaction t, Set<", StreamId, "> streamIds) {"); {
                    _("if (streamIds.isEmpty()) {"); {
                        _("return ImmutableMap.of();");
                    } _("}");
                    _(StreamMetadataTable, " table = tables.get", StreamMetadataTable, "(t);");
                    _("Map<", StreamMetadataRow, ", StreamMetadata> metadatas = table.getMetadatas(getMetadataRowsForIds(streamIds));");
                    _("Map<", StreamId, ", StreamMetadata> ret = Maps.newHashMap();");
                    _("for (Map.Entry<", StreamMetadataRow, ", StreamMetadata> e : metadatas.entrySet()) {"); {
                        _("ret.put(e.getKey().getId(), e.getValue());");
                    } _("}");
                    _("return ret;");
                } _("}");
            }

            private void lookupStreamsByHash() {
                _("@Override");
                _("public Map<Sha256Hash, ", StreamId, "> lookupStreamIdsByHash(final Set<Sha256Hash> hashes) {"); {
                    _("if (hashes.isEmpty()) {"); {
                        _("return ImmutableMap.of();");
                    } _("}");
                    _("return txRunner.run(new TransactionTask<Map<Sha256Hash, ", StreamId, ">, RuntimeException>() {"); {
                        _("@Override");
                        _("public Map<Sha256Hash, ", StreamId, "> execute(Transaction t) {"); {
                            _(StreamHashAidxTable, " idx = tables.get", StreamHashAidxTable, "(t);");
                            _("Set<", StreamHashAidxRow, "> rows = getHashIndexRowsForHashes(hashes);");
                            _();
                            _("Multimap<", StreamHashAidxRow, ", ", StreamHashAidxColumnValue, "> m = idx.getRowsMultimap(rows);");
                            _("Map<", StreamId, ", Sha256Hash> hashForStreams = Maps.newHashMap();");
                            _("for (", StreamHashAidxRow, " r : m.keySet()) {"); {
                                _("for (", StreamHashAidxColumnValue, " v : m.get(r)) {"); {
                                    _(StreamId, " streamId = v.getColumnName().getStreamId();");
                                    _("Sha256Hash hash = r.getHash();");
                                    _("if (hashForStreams.containsKey(streamId)) {"); {
                                        _("AssertUtils.assertAndLog(hashForStreams.get(streamId).equals(hash), \"(BUG) Stream ID has 2 different hashes: \" + streamId);");
                                    } _("}");
                                    _("hashForStreams.put(streamId, hash);");
                                } _("}");
                            } _("}");
                            _("Map<", StreamId, ", StreamMetadata> metadata = getMetadataTask(t, hashForStreams.keySet());");
                            _();
                            _("Map<Sha256Hash, ", StreamId, "> ret = Maps.newHashMap();");
                            _("for (Map.Entry<", StreamId, ", StreamMetadata> e : metadata.entrySet()) {"); {
                                _("if (e.getValue().getStatus() != Status.STORED) {"); {
                                    _("continue;");
                                } _("}");
                                _("Sha256Hash hash = hashForStreams.get(e.getKey());");
                                _("ret.put(hash, e.getKey());");
                            } _("}");
                            _();
                            _("return ret;");
                        } _("}");
                    } _("});");
                } _("}");
            }

            private void getHashIndexRowsForHashes() {
                _("private Set<", StreamHashAidxRow, "> getHashIndexRowsForHashes(final Set<Sha256Hash> hashes) {"); {
                    _("Set<", StreamHashAidxRow, "> rows = Sets.newHashSet();");
                    _("for (Sha256Hash h : hashes) {"); {
                        _("rows.add(", StreamHashAidxRow, ".of(h));");
                    } _("}");
                    _("return rows;");
                } _("}");
            }

            private void getMetadataRowsForIds() {
                _("private Set<", StreamMetadataRow, "> getMetadataRowsForIds(final Iterable<", StreamId, "> ids) {"); {
                    _("Set<", StreamMetadataRow, "> rows = Sets.newHashSet();");
                    _("for (", StreamId, " id : ids) {"); {
                        _("rows.add(", StreamMetadataRow, ".of(id));");
                    } _("}");
                    _("return rows;");
                } _("}");
            }

            private void putHashIndexTask() {
                String params = isExpiring() ? ", long duration, TimeUnit unit" : "";
                String args = isExpiring() ? "duration, unit, " : "";
                _("private void putHashIndexTask(Transaction t, ", StreamMetadataRow, " row, StreamMetadata metadata", params, ") {"); {
                    _("Preconditions.checkArgument(");
                    _("        metadata.getStatus() == Status.STORED,");
                    _("        \"Should only index successfully stored streams.\");");
                    _();
                    _("Sha256Hash hash = Sha256Hash.EMPTY;");
                    _("if (metadata.getHash() != com.google.protobuf.ByteString.EMPTY) {"); {
                        _("hash = new Sha256Hash(metadata.getHash().toByteArray());");
                    } _("}");
                    _(StreamHashAidxRow, " hashRow = ", StreamHashAidxRow, ".of(hash);");
                    _(StreamHashAidxColumn, " column = ", StreamHashAidxColumn, ".of(row.getId());");
                    _(StreamHashAidxColumnValue, " columnValue = ", StreamHashAidxColumnValue, ".of(column, 0L);");
                    _(StreamHashAidxTable, " hiTable = tables.get", StreamHashAidxTable, "(t);");
                    _("hiTable.put(", args, "hashRow, columnValue);");
                } _("}");
            }

            private void deleteStreams() {
                _("public void deleteStreams(final Set<", StreamId, "> streamIds) {"); {
                    _("if (streamIds.isEmpty()) {"); {
                        _("return;");
                    } _("}");
                    _("txRunner.run(new TransactionTask<Void, RuntimeException>() {"); {
                        _("@Override");
                        _("public Void execute(Transaction t) {"); {
                            _("Set<", StreamMetadataRow, "> smRows = Sets.newHashSet();");
                            _("Multimap<", StreamHashAidxRow, ", ", StreamHashAidxColumn, "> shToDelete = HashMultimap.create();");
                            _("for (", StreamId, " streamId : streamIds) {"); {
                                _("smRows.add(", StreamMetadataRow, ".of(streamId));");
                            } _("}");
                            _(StreamMetadataTable, " ", StreamMetadataTable, " = tables.get", StreamMetadataTable, "(t);");
                            _("Map<", StreamMetadataRow, ", StreamMetadata> metadatas = ", StreamMetadataTable, ".getMetadatas(smRows);");
                            _();
                            _("Set<", StreamValueRow, "> streamValueToDelete = Sets.newHashSet();");
                            _("for (Entry<", StreamMetadataRow, ", StreamMetadata> e : metadatas.entrySet()) {"); {
                                _(StreamId, " streamId = e.getKey().getId();");
                                _("long blocks = getNumberOfBlocksFromMetadata(e.getValue());");
                                _("for (long i = 0; i < blocks; i++) {"); {
                                    _("streamValueToDelete.add(", StreamValueRow, ".of(streamId, i));");
                                } _("}");
                                _();
                                _("ByteString streamHash = e.getValue().getHash();");
                                _("Sha256Hash hash = Sha256Hash.EMPTY;");
                                _("if (streamHash != com.google.protobuf.ByteString.EMPTY) {"); {
                                    _("hash = new Sha256Hash(streamHash.toByteArray());");
                                } _("} else {"); {
                                    _("log.error(\"Empty hash for stream \" + streamId);");
                                } _("}");
                                _(StreamHashAidxRow, " hashRow = ", StreamHashAidxRow, ".of(hash);");
                                _(StreamHashAidxColumn, " column = ", StreamHashAidxColumn, ".of(streamId);");
                                _("shToDelete.put(hashRow, column);");
                            } _("}");
                            _();
                            _("tables.get", StreamHashAidxTable, "(t).delete(shToDelete);");
                            _("tables.get", StreamValueTable, "(t).delete(streamValueToDelete);");
                            _(StreamMetadataTable, ".delete(smRows);");
                            _("return null;");
                        } _("}");
                    } _("});");
                } _("}");
            }

            private void touchMetadataWhileMarkingUsedForConflicts() {
                _("private void touchMetadataWhileMarkingUsedForConflicts(Transaction t, Iterable<", StreamId, "> ids) {"); {
                    _(StreamMetadataTable, " metaTable = tables.get", StreamMetadataTable, "(t);");
                    _("Set<", StreamMetadataRow, "> rows = Sets.newHashSet();");
                    _("for (", StreamId, " id : ids) {"); {
                        _("rows.add(", StreamMetadataRow, ".of(id));");
                    } _("}");
                    _("Map<", StreamMetadataRow, ", StreamMetadata> metadatas = metaTable.getMetadatas(rows);");
                    _("for (Map.Entry<", StreamMetadataRow, ", StreamMetadata> e : metadatas.entrySet()) {"); {
                        _("StreamMetadata metadata = e.getValue();");
                        _("Preconditions.checkState(metadata.getStatus() == Status.STORED,");
                        _("        \"Stream: \" + e.getKey().getId() + \" has status: \" + metadata.getStatus());");
                        _("metaTable.putMetadata(e.getKey(), metadata);");
                    } _("}");
                    _("SetView<", StreamMetadataRow, "> missingRows = Sets.difference(rows, metadatas.keySet());");
                    _("if (!missingRows.isEmpty()) {"); {
                        _("throw new IllegalStateException(\"Missing metadata rows for:\" + missingRows");
                        _("        + \" rows: \" + rows + \" metadata: \" + metadatas + \" txn timestamp: \" + t.getTimestamp());");
                    } _("}");
                } _("}");
            }

            private void markStreamsAsUsed() {
                _("@Override");
                _("public void markStreamsAsUsed(final Map<", StreamId, ", byte[]> streamIdsToReference) {"); {
                    _("if (streamIdsToReference.isEmpty()) {"); {
                        _("return;");
                    } _("}");
                    _("txRunner.run(new TxTask() {"); {
                        _("@Override");
                        _("public Void execute(Transaction t) {"); {
                            _("touchMetadataWhileMarkingUsedForConflicts(t, streamIdsToReference.keySet());");
                            _(StreamIdxTable, " index = tables.get", StreamIdxTable, "(t);");
                            _("for (Map.Entry<", StreamId, ", byte[]> entry : streamIdsToReference.entrySet()) {"); {
                                _(StreamId, " streamId = entry.getKey();");
                                _("byte[] reference = entry.getValue();");
                                _(StreamIdxColumn, " col = ", StreamIdxColumn, ".of(reference);");
                                _(StreamIdxColumnValue, " value = ", StreamIdxColumnValue, ".of(col, 0L);");
                                _("index.put(", StreamIdxRow, ".of(streamId), value);");
                            } _("}");
                            _("return null;");
                        } _("}");
                    } _("});");
                } _("}");
            }

            private void removeStreamsAsUsed() {
                _("@Override");
                _("public void removeStreamsAsUsed(final Map<", StreamId, ", byte[]> streamIdsToReference) {"); {
                    _("if (streamIdsToReference.isEmpty()) {"); {
                        _("return;");
                    } _("}");
                    _("txRunner.run(new TxTask() {"); {
                        _("@Override");
                        _("public Void execute(Transaction t) {"); {
                            _(StreamIdxTable, " index = tables.get", StreamIdxTable, "(t);");
                            _("Multimap<", StreamIdxRow, ", ", StreamIdxColumn, "> toDelete = ArrayListMultimap.create(streamIdsToReference.size(), 1);");
                            _("for (Map.Entry<", StreamId, ", byte[]> entry : streamIdsToReference.entrySet()) {"); {
                                _(StreamId, " streamId = entry.getKey();");
                                _("byte[] reference = entry.getValue();");
                                _(StreamIdxColumn, " col = ", StreamIdxColumn, ".of(reference);");
                                _("toDelete.put(", StreamIdxRow, ".of(streamId), col);");
                            } _("}");
                            _("index.delete(toDelete);");
                            _("Collection<", StreamIdxRow, "> rows = Collections2.transform(streamIdsToReference.keySet(), ", StreamIdxRow, ".fromIdFun());");
                            _("Set<", StreamId, "> streamsToKeep = ImmutableSet.copyOf(Collections2.transform(index.getRowsMultimap(rows).keySet(), ", StreamIdxRow, ".getIdFun()));");
                            _(StreamStore, ".of(t, tables).deleteStreams(Sets.difference(streamIdsToReference.keySet(), streamsToKeep));");
                            _("return null;");
                        } _("}");
                    } _("});");
                } _("}");
            }
        }.render();
    }

    private boolean isExpiring() {
        return expirationStrategy == ExpirationStrategy.INDIVIDUALLY_SPECIFIED;
    }

    public String renderIndexCleanupTask() {
        final String StreamStore = name + "StreamStore";

        final String StreamIdxTable = name + "StreamIdxTable";
        final String StreamIdxRow = StreamIdxTable + "." + name + "StreamIdxRow";
        final String StreamIdxColumnValue = StreamIdxTable + "." + name + "StreamIdxColumnValue";

        final String TableFactory = schemaName + "TableFactory";
        final String IndexCleanupTask = getIndexCleanupTaskClassName();
        final String StreamId = streamIdType.getJavaObjectClassName();

        return new Renderer() {
            @Override
            protected void run() {
                packageAndImports();
                _();
                _("public class ", IndexCleanupTask, " implements OnCleanupTask {"); {
                    _();
                    _("private final ", TableFactory, " tables = ", TableFactory, ".of();");
                    _();
                    cellsCleanedUp();
                } _("}");
            }

            private void packageAndImports() {
                _("package ", packageName, ";");
                _();
                _("import java.util.Set;");
                _();
                _("import com.google.common.collect.Multimap;");
                _("import com.google.common.collect.Sets;");
                _("import com.palantir.atlasdb.cleaner.api.OnCleanupTask;");
                _("import com.palantir.atlasdb.keyvalue.api.Cell;");
                _("import com.palantir.atlasdb.table.description.ValueType;");
                _("import com.palantir.atlasdb.transaction.api.Transaction;");

                if (streamIdType == ValueType.SHA256HASH) {
                    _("import com.palantir.util.crypto.Sha256Hash;");
                }
            }

            private void cellsCleanedUp() {
                _("@Override");
                _("public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {"); {
                    _(StreamIdxTable, " usersIndex = tables.get", StreamIdxTable, "(t);");
                    _("Set<", StreamIdxRow, "> rows = Sets.newHashSetWithExpectedSize(cells.size());");
                    _("for (Cell cell : cells) {"); {
                        _("rows.add(", StreamIdxRow, ".of((", StreamId, ") ValueType.", streamIdType.toString(), ".convertToJava(cell.getRowName(), 0)));");
                    } _("}");
                    _("Multimap<", StreamIdxRow, ", ", StreamIdxColumnValue, "> rowsInDb = usersIndex.getRowsMultimap(rows);");
                    _("Set<", StreamId, "> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.keySet().size());");
                    _("for (", StreamIdxRow, " rowToDelete : Sets.difference(rows, rowsInDb.keySet())) {"); {
                        _("toDelete.add(rowToDelete.getId());");
                    } _("}");
                    _(StreamStore, ".of(t, tables).deleteStreams(toDelete);");
                    _("return false;");
                } _("}");
            }
        }.render();
    }

    public String renderMetadataCleanupTask() {
        final String StreamStore = name + "StreamStore";

        final String StreamMetadataTable = name + "StreamMetadataTable";
        final String StreamMetadataRow = StreamMetadataTable + "." + name + "StreamMetadataRow";

        final String TableFactory = schemaName + "TableFactory";
        final String MetadataCleanupTask = getMetadataCleanupTaskClassName();
        final String StreamId = streamIdType.getJavaObjectClassName();

        return new Renderer() {
            @Override
            protected void run() {
                packageAndImports();
                _();
                _("public class ", MetadataCleanupTask, " implements OnCleanupTask {"); {
                    _();
                    _("private final ", TableFactory, " tables = ", TableFactory, ".of();");
                    _();
                    cellsCleanedUp();
                } _("}");
            }

            private void packageAndImports() {
                _("package ", packageName, ";");
                _();
                _("import java.util.Collection;");
                _("import java.util.Map;");
                _("import java.util.Set;");
                _();
                _("import com.google.common.collect.Lists;");
                _("import com.google.common.collect.Sets;");
                _("import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;");
                _("import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;");
                _("import com.palantir.atlasdb.cleaner.api.OnCleanupTask;");
                _("import com.palantir.atlasdb.keyvalue.api.Cell;");
                _("import com.palantir.atlasdb.table.description.ValueType;");
                _("import com.palantir.atlasdb.transaction.api.Transaction;");

                if (streamIdType == ValueType.SHA256HASH) {
                    _("import com.palantir.util.crypto.Sha256Hash;");
                }
            }

            private void cellsCleanedUp() {
                _("@Override");
                _("public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {"); {
                    _(StreamMetadataTable, " metaTable = tables.get", StreamMetadataTable, "(t);");
                    _("Collection<", StreamMetadataRow, "> rows = Lists.newArrayListWithCapacity(cells.size());");
                    _("for (Cell cell : cells) {"); {
                        _("rows.add(", StreamMetadataRow, ".of((", StreamId, ") ValueType.", streamIdType.toString(), ".convertToJava(cell.getRowName(), 0)));");
                    } _("}");
                    _("Map<", StreamMetadataRow, ", StreamMetadata> currentMetadata = metaTable.getMetadatas(rows);");
                    _("Set<", StreamId, "> toDelete = Sets.newHashSet();");
                    _("for (Map.Entry<", StreamMetadataRow, ", StreamMetadata> e : currentMetadata.entrySet()) {"); {
                        _("if (e.getValue().getStatus() != Status.STORED) {"); {
                            _("toDelete.add(e.getKey().getId());");
                        } _("}");
                    } _("}");
                    _(StreamStore, ".of(t, tables).deleteStreams(toDelete);");
                    _("return false;");
                } _("}");
            }
        }.render();
    }


    private static final Class<?>[] IMPORTS = new Class<?>[] {
        BufferedInputStream.class,
        ByteArrayInputStream.class,
        File.class,
        FileNotFoundException.class,
        FileOutputStream.class,
        IOException.class,
        InputStream.class,
        OutputStream.class,
        DigestInputStream.class,
        MessageDigest.class,
        Collection.class,
        Map.class,
        Entry.class,
        Set.class,
        TimeUnit.class,
        IOUtils.class,
        Logger.class,
        LoggerFactory.class,
        Preconditions.class,
        Arrays.class,
        ArrayListMultimap.class,
        Collections2.class,
        HashMultimap.class,
        ImmutableMap.class,
        ImmutableSet.class,
        Lists.class,
        Maps.class,
        Multimap.class,
        Multimaps.class,
        Sets.class,
        Sets.SetView.class,
        CountingInputStream.class,
        Ints.class,
        ByteString.class,
        Status.class,
        StreamMetadata.class,
        StreamMetadata.Builder.class,
        Throwables.class,
        ConcatenatedInputStream.class,
        Cell.class,
        ExpiringStreamStore.class,
        PersistentStreamStore.class,
        TransactionBackedRunner.class,
        TransactionManagerBackedRunner.class,
        TransactionRunner.class,
        Transaction.class,
        TransactionManager.class,
        TransactionTask.class,
        TxTask.class,
        AssertUtils.class,
        ByteArrayIOStream.class,
        Sha256Hash.class,
        DeleteOnCloseFileInputStream.class,
        FileUtils.class,
    };
}
