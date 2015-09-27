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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.CheckForNull;

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
import com.palantir.atlasdb.stream.AbstractExpiringStreamStore;
import com.palantir.atlasdb.stream.AbstractPersistentStreamStore;
import com.palantir.atlasdb.stream.ExpiringStreamStore;
import com.palantir.atlasdb.stream.PersistentStreamStore;
import com.palantir.atlasdb.stream.StreamCleanedException;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
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
                line("package ", packageName, ";");
                line();
                importRenderer.renderImports();
                line();
                line("public final class ", StreamStore, " extends ", (isExpiring() ? "AbstractExpiringStreamStore<" + StreamId + ">" : "AbstractPersistentStreamStore"), " {"); {
                    fields();
                    line();
                    constructors();
                    line();
                    getInMemoryThreshold();
                    line();
                    storeBlock();
                    line();
                    touchMetadataWhileStoringForConflicts();
                    line();
                    putMetadataAndHashIndexTask();
                    line();
                    getNumberOfBlocksFromMetadata();
                    line();
                    createTempFile();
                    line();
                    loadSingleBlockToOutputStream();
                    line();
                    getBlock();
                    line();
                    getMetadata();
                    line();
                    lookupStreamIdByHash();
                    line();
                    putHashIndexTask();
                    line();
                    deleteStreams();
                    if (!isExpiring()) {
                        line();
                        markStreamsAsUsed();
                        line();
                        unmarkStreamAsUsed();
                        line();
                        touchMetadataWhileMarkingUsedForConflicts();
                    }
                    line();
                    line("static final int dummy = 0;");
                } line("}");
            }

            private void fields() {
                line("public static final int BLOCK_SIZE_IN_BYTES = 1000000; // 1MB. DO NOT CHANGE THIS WITHOUT AN UPGRADE TASK");
                line("public static final int IN_MEMORY_THRESHOLD = ", String.valueOf(inMemoryThreshold), "; // streams under this size are kept in memory when loaded");
                line("public static final String STREAM_FILE_PREFIX = \"", name, "_stream_\";");
                line("public static final String STREAM_FILE_SUFFIX = \".tmp\";");
                line();
                line("private static final Logger log = LoggerFactory.getLogger(", StreamStore, ".class);");
                line();
                line("private final ", TableFactory, " tables;");
            }

            private void constructors() {
                line("private ", StreamStore, "(TransactionManager txManager, ", TableFactory, " tables) {"); {
                    line("super(txManager);");
                    line("this.tables = tables;");
                } line("}");
                line();
                line("public static ", StreamStore, " of(TransactionManager txManager, ", TableFactory, " tables) {"); {
                    line("return new ", StreamStore, "(txManager, tables);");
                } line("}");
                line();
                line("/**");
                line(" * This should only be used by test code or as a performance optimization.");
                line(" */");
                line("static ", StreamStore, " of(", TableFactory, " tables) {"); {
                    line("return new ", StreamStore, "(null, tables);");
                } line("}");
            }

            private void storeBlock() {
                String params = isExpiring() ? ", final long duration, final TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                String streamType = isExpiring() ? StreamId : "long";
                line("@Override");
                line("protected void storeBlock(", streamType, " id, long blockNumber, final byte[] block", params, ") {"); {
                    line("Preconditions.checkArgument(block.length <= BLOCK_SIZE_IN_BYTES, \"Block to store in DB must be less than BLOCK_SIZE_IN_BYTES\");");
                    line("Preconditions.checkNotNull(txnMgr);");
                    line("final ", StreamValueRow, " row = ", StreamValueRow, ".of(id, blockNumber);");
                    line("try {"); {
                        line("txnMgr.runTaskThrowOnConflict(new TransactionTask<Void, RuntimeException>() {"); {
                            line("@Override");
                            line("public Void execute(Transaction t) {"); {
                                line("// Do a touch operation on this table to ensure we get a conflict if someone cleans it up.");
                                line("touchMetadataWhileStoringForConflicts(t, row.getId(), row.getBlockId()", args, ");");
                                line("tables.get", StreamValueTable, "(t).putValue(row, block", args, ");");
                                line("return null;");
                            } line("}");
                        } line("});");
                    } line("} catch (RuntimeException e) {"); {
                        line("log.error(\"Error storing block \" + row.getBlockId() + \" for stream id \" + row.getId(), e);");
                        line("throw e;");
                    } line("}");
                } line("}");
            }

            private void touchMetadataWhileStoringForConflicts() {
                String params = isExpiring() ? ", long duration, TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                line("private void touchMetadataWhileStoringForConflicts(Transaction t, ", StreamId, " id, long blockNumber", params, ") {"); {
                    line(StreamMetadataTable, " metaTable = tables.get", StreamMetadataTable, "(t);");
                    line(StreamMetadataRow, " row = ", StreamMetadataRow, ".of(id);");
                    line("StreamMetadata metadata = metaTable.getMetadatas(ImmutableSet.of(row)).values().iterator().next();");
                    line("Preconditions.checkState(metadata.getStatus() == Status.STORING, \"This stream is being cleaned up while storing blocks: \" + id);");
                    line("Builder builder = StreamMetadata.newBuilder(metadata);");
                    line("builder.setLength(blockNumber * BLOCK_SIZE_IN_BYTES + 1);");
                    line("metaTable.putMetadata(row, builder.build()", args, ");");
                } line("}");
            }

            private void putMetadataAndHashIndexTask() {
                String streamType = isExpiring() ? StreamId : "long";
                String params = isExpiring() ? ", long duration, TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                line("@Override");
                line("protected void putMetadataAndHashIndexTask(Transaction t, ", streamType, " streamId, StreamMetadata metadata", params, ") {"); {
                    line(StreamMetadataRow, " row = ", StreamMetadataRow, ".of(streamId);");
                    line(StreamMetadataTable, " mdTable = tables.get", StreamMetadataTable, "(t);");
                    line("if (metadata.getStatus() == Status.STORED) {"); {
                        line("StreamMetadata prevMetadata = getMetadata(t, streamId);");
                        line("if (prevMetadata == null || prevMetadata.getStatus() != Status.STORING) {"); {
                            line("// This can happen if we cleanup old streams.");
                            line("throw new TransactionFailedRetriableException(\"Cannot mark a stream as stored that isn't currently storing: \" + prevMetadata);");
                        } line("}");
                        line("putHashIndexTask(t, row, metadata", args, ");");
                    } line("} else if (metadata.getStatus() == Status.STORING) {"); {
                        line("StreamMetadata prevMetadata = getMetadata(t, streamId);");
                        line("// This will prevent two users trying to store the same id.");
                        line("if (prevMetadata != null) {"); {
                            line("throw new TransactionFailedRetriableException(\"Cannot reuse the same stream id: \" + streamId);");
                        } line("}");
                    } line("}");
                    line();
                    line("mdTable.putMetadata(row, metadata", args, ");");
                } line("}");
            }

            private void getNumberOfBlocksFromMetadata() {
                line("private long getNumberOfBlocksFromMetadata(StreamMetadata metadata) {"); {
                    line("return (metadata.getLength() + BLOCK_SIZE_IN_BYTES - 1) / BLOCK_SIZE_IN_BYTES;");
                } line("}");
            }

            private void getInMemoryThreshold() {
                line("@Override");
                line("protected long getInMemoryThreshold() {"); {
                    line("return IN_MEMORY_THRESHOLD;");
                } line("}");
            }

            private void createTempFile() {
                line("@Override");
                line("protected File createTempFile(", StreamId, " id) throws IOException {"); {
                    line("File file = FileUtils.createTempFile(STREAM_FILE_PREFIX + id, STREAM_FILE_SUFFIX);");
                    line("file.deleteOnExit();");
                    line("return file;");
                } line("}");
            }

            private void loadSingleBlockToOutputStream() {
                line("@Override");
                line("protected void loadSingleBlockToOutputStream(Transaction t, ", StreamId, " streamId, long blockId, OutputStream os) {"); {
                    line(StreamValueRow, " row = ", StreamValueRow, ".of(streamId, blockId);");
                    line("try {"); {
                        line("os.write(getBlock(t, row));");
                    } line("} catch (RuntimeException e) {"); {
                        line("log.error(\"Error getting block \" + row.getBlockId() + \" of stream \" + row.getId(), e);");
                        line("throw e;");
                    } line("} catch (IOException e) {"); {
                        line("log.error(\"Error writing block \" + row.getBlockId() + \" to file when getting stream id \" + row.getId(), e);");
                        line("throw Throwables.rewrapAndThrowUncheckedException(\"Error writing blocks to file when creating stream.\", e);");
                    } line("}");
                } line("}");
            }

            private void getBlock() {
                line("private byte[] getBlock(Transaction t, ", StreamValueRow, " row) {"); {
                    line(StreamValueTable, " valueTable = tables.get", StreamValueTable, "(t);");
                    line("return valueTable.getValues(ImmutableSet.of(row)).get(row);");
                } line("}");
            }

            private void getMetadata() {
                line("@Override");
                line("protected StreamMetadata getMetadata(Transaction t, ", StreamId, " streamId) {"); {
                    line(StreamMetadataRow, " row = ", StreamMetadataRow, ".of(streamId);");
                    line(StreamMetadataTable, " table = tables.get", StreamMetadataTable, "(t);");
                    line("return table.getMetadatas(ImmutableSet.of(row)).get(row);");
                } line("}");
            }

            private void lookupStreamIdByHash() {
                String streamType = isExpiring() ? StreamId : "long";
                line("@Override");
                line("@CheckForNull");
                line("public ", StreamId, " lookupStreamIdByHash(Transaction t, Sha256Hash hash) {"); {
                    line(StreamHashAidxTable, " idx = tables.get", StreamHashAidxTable, "(t);");

                    line("List<", StreamHashAidxColumnValue, "> columns = idx.getRowColumns(", StreamHashAidxRow, ".of(hash));");
                    line("for (", StreamHashAidxColumnValue, " colVal : columns) {"); {
                        line(streamType, " streamId = colVal.getColumnName().getStreamId();");
                        line("StreamMetadata meta = getMetadata(t, streamId);");
                        line("if (meta.getStatus() == Status.STORED) {"); {
                            line("return streamId;");
                        } line("}");
                    } line("}");
                    line("return null;");
                } line("}");
            }

            private void putHashIndexTask() {
                String params = isExpiring() ? ", long duration, TimeUnit unit" : "";
                String args = isExpiring() ? "duration, unit, " : "";
                line("private void putHashIndexTask(Transaction t, ", StreamMetadataRow, " row, StreamMetadata metadata", params, ") {"); {
                    line("Preconditions.checkArgument(");
                    line("        metadata.getStatus() == Status.STORED,");
                    line("        \"Should only index successfully stored streams.\");");
                    line();
                    line("Sha256Hash hash = Sha256Hash.EMPTY;");
                    line("if (metadata.getHash() != com.google.protobuf.ByteString.EMPTY) {"); {
                        line("hash = new Sha256Hash(metadata.getHash().toByteArray());");
                    } line("}");
                    line(StreamHashAidxRow, " hashRow = ", StreamHashAidxRow, ".of(hash);");
                    line(StreamHashAidxColumn, " column = ", StreamHashAidxColumn, ".of(row.getId());");
                    line(StreamHashAidxColumnValue, " columnValue = ", StreamHashAidxColumnValue, ".of(column, 0L);");
                    line(StreamHashAidxTable, " hiTable = tables.get", StreamHashAidxTable, "(t);");
                    line("hiTable.put(", args, "hashRow, columnValue);");
                } line("}");
            }

            private void deleteStreams() {
                line("/**");
                line(" * This should only be used from the cleanup tasks.");
                line(" */");
                line("void deleteStreams(Transaction t, final Set<", StreamId, "> streamIds) {"); {
                    line("if (streamIds.isEmpty()) {"); {
                        line("return;");
                    } line("}");

                    line("Set<", StreamMetadataRow, "> smRows = Sets.newHashSet();");
                    line("Multimap<", StreamHashAidxRow, ", ", StreamHashAidxColumn, "> shToDelete = HashMultimap.create();");
                    line("for (", StreamId, " streamId : streamIds) {"); {
                        line("smRows.add(", StreamMetadataRow, ".of(streamId));");
                    } line("}");
                    line(StreamMetadataTable, " table = tables.get", StreamMetadataTable, "(t);");
                    line("Map<", StreamMetadataRow, ", StreamMetadata> metadatas = table.getMetadatas(smRows);");

                    line("Set<", StreamValueRow, "> streamValueToDelete = Sets.newHashSet();");
                    line("for (Entry<", StreamMetadataRow, ", StreamMetadata> e : metadatas.entrySet()) {"); {
                        line(StreamId, " streamId = e.getKey().getId();");
                        line("long blocks = getNumberOfBlocksFromMetadata(e.getValue());");
                        line("for (long i = 0; i < blocks; i++) {"); {
                            line("streamValueToDelete.add(", StreamValueRow, ".of(streamId, i));");
                        } line("}");

                        line("ByteString streamHash = e.getValue().getHash();");
                        line("Sha256Hash hash = Sha256Hash.EMPTY;");
                        line("if (streamHash != com.google.protobuf.ByteString.EMPTY) {"); {
                            line("hash = new Sha256Hash(streamHash.toByteArray());");
                        } line("} else {"); {
                            line("log.error(\"Empty hash for stream \" + streamId);");
                        } line("}");
                        line(StreamHashAidxRow, " hashRow = ", StreamHashAidxRow, ".of(hash);");
                        line(StreamHashAidxColumn, " column = ", StreamHashAidxColumn, ".of(streamId);");
                        line("shToDelete.put(hashRow, column);");
                    } line("}");

                    line("tables.get", StreamHashAidxTable, "(t).delete(shToDelete);");
                    line("tables.get", StreamValueTable, "(t).delete(streamValueToDelete);");
                    line("table.delete(smRows);");
                } line("}");
            }

            private void touchMetadataWhileMarkingUsedForConflicts() {
                line("@Override");
                line("protected void touchMetadataWhileMarkingUsedForConflicts(Transaction t, long streamId) {"); {
                    line(StreamMetadataTable, " metaTable = tables.get", StreamMetadataTable, "(t);");
                    line("Set<", StreamMetadataRow, "> rows = Sets.newHashSet();");
                    line("rows.add(", StreamMetadataRow, ".of(streamId));");
                    line("Map<", StreamMetadataRow, ", StreamMetadata> metadatas = metaTable.getMetadatas(rows);");
                    line("for (Map.Entry<", StreamMetadataRow, ", StreamMetadata> e : metadatas.entrySet()) {"); {
                        line("StreamMetadata metadata = e.getValue();");
                        line("Preconditions.checkState(metadata.getStatus() == Status.STORED,");
                        line("        \"Stream: \" + e.getKey().getId() + \" has status: \" + metadata.getStatus());");
                        line("metaTable.putMetadata(e.getKey(), metadata);");
                    } line("}");
                    line("SetView<", StreamMetadataRow, "> missingRows = Sets.difference(rows, metadatas.keySet());");
                    line("if (!missingRows.isEmpty()) {"); {
                        line("throw new StreamCleanedException(\"Missing metadata rows for:\" + missingRows");
                        line("        + \" rows: \" + rows + \" metadata: \" + metadatas + \" txn timestamp: \" + t.getTimestamp());");
                    } line("}");
                } line("}");
            }

            private void markStreamsAsUsed() {
                line("@Override");
                line("protected void markStreamAsUsedInternal(Transaction t, long streamId, byte[] reference) {"); {
                    line(StreamIdxTable, " index = tables.get", StreamIdxTable, "(t);");
                    line(StreamIdxColumn, " col = ", StreamIdxColumn, ".of(reference);");
                    line(StreamIdxColumnValue, " value = ", StreamIdxColumnValue, ".of(col, 0L);");
                    line("index.put(", StreamIdxRow, ".of(streamId), value);");
                } line("}");
            }

            private void unmarkStreamAsUsed() {
                line("@Override");
                line("public void unmarkStreamAsUsed(Transaction t, long streamId, byte[] reference) {"); {
                    line(StreamIdxTable, " index = tables.get", StreamIdxTable, "(t);");
                    line(StreamIdxRow, " row = ", StreamIdxRow, ".of(streamId);");
                    line(StreamIdxColumn, " col = ", StreamIdxColumn, ".of(reference);");
                    line("index.delete(row, col);");
                } line("}");
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
                line();
                line("public class ", IndexCleanupTask, " implements OnCleanupTask {"); {
                    line();
                    line("private final ", TableFactory, " tables = ", TableFactory, ".of();");
                    line();
                    cellsCleanedUp();
                } line("}");
            }

            private void packageAndImports() {
                line("package ", packageName, ";");
                line();
                line("import java.util.Set;");
                line();
                line("import com.google.common.collect.Multimap;");
                line("import com.google.common.collect.Sets;");
                line("import com.palantir.atlasdb.cleaner.api.OnCleanupTask;");
                line("import com.palantir.atlasdb.keyvalue.api.Cell;");
                line("import com.palantir.atlasdb.table.description.ValueType;");
                line("import com.palantir.atlasdb.transaction.api.Transaction;");

                if (streamIdType == ValueType.SHA256HASH) {
                    line("import com.palantir.util.crypto.Sha256Hash;");
                }
            }

            private void cellsCleanedUp() {
                line("@Override");
                line("public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {"); {
                    line(StreamIdxTable, " usersIndex = tables.get", StreamIdxTable, "(t);");
                    line("Set<", StreamIdxRow, "> rows = Sets.newHashSetWithExpectedSize(cells.size());");
                    line("for (Cell cell : cells) {"); {
                        line("rows.add(", StreamIdxRow, ".of((", StreamId, ") ValueType.", streamIdType.toString(), ".convertToJava(cell.getRowName(), 0)));");
                    } line("}");
                    line("Multimap<", StreamIdxRow, ", ", StreamIdxColumnValue, "> rowsInDb = usersIndex.getRowsMultimap(rows);");
                    line("Set<", StreamId, "> toDelete = Sets.newHashSetWithExpectedSize(rows.size() - rowsInDb.keySet().size());");
                    line("for (", StreamIdxRow, " rowToDelete : Sets.difference(rows, rowsInDb.keySet())) {"); {
                        line("toDelete.add(rowToDelete.getId());");
                    } line("}");
                    line(StreamStore, ".of(tables).deleteStreams(t, toDelete);");
                    line("return false;");
                } line("}");
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
                line();
                line("public class ", MetadataCleanupTask, " implements OnCleanupTask {"); {
                    line();
                    line("private final ", TableFactory, " tables = ", TableFactory, ".of();");
                    line();
                    cellsCleanedUp();
                } line("}");
            }

            private void packageAndImports() {
                line("package ", packageName, ";");
                line();
                line("import java.util.Collection;");
                line("import java.util.Map;");
                line("import java.util.Set;");
                line();
                line("import com.google.common.collect.Lists;");
                line("import com.google.common.collect.Sets;");
                line("import com.palantir.atlasdb.cleaner.api.OnCleanupTask;");
                line("import com.palantir.atlasdb.keyvalue.api.Cell;");
                line("import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;");
                line("import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;");
                line("import com.palantir.atlasdb.table.description.ValueType;");
                line("import com.palantir.atlasdb.transaction.api.Transaction;");

                if (streamIdType == ValueType.SHA256HASH) {
                    line("import com.palantir.util.crypto.Sha256Hash;");
                }
            }

            private void cellsCleanedUp() {
                line("@Override");
                line("public boolean cellsCleanedUp(Transaction t, Set<Cell> cells) {"); {
                    line(StreamMetadataTable, " metaTable = tables.get", StreamMetadataTable, "(t);");
                    line("Collection<", StreamMetadataRow, "> rows = Lists.newArrayListWithCapacity(cells.size());");
                    line("for (Cell cell : cells) {"); {
                        line("rows.add(", StreamMetadataRow, ".of((", StreamId, ") ValueType.", streamIdType.toString(), ".convertToJava(cell.getRowName(), 0)));");
                    } line("}");
                    line("Map<", StreamMetadataRow, ", StreamMetadata> currentMetadata = metaTable.getMetadatas(rows);");
                    line("Set<", StreamId, "> toDelete = Sets.newHashSet();");
                    line("for (Map.Entry<", StreamMetadataRow, ", StreamMetadata> e : currentMetadata.entrySet()) {"); {
                        line("if (e.getValue().getStatus() != Status.STORED) {"); {
                            line("toDelete.add(e.getKey().getId());");
                        } line("}");
                    } line("}");
                    line(StreamStore, ".of(tables).deleteStreams(t, toDelete);");
                    line("return false;");
                } line("}");
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
        Transaction.class,
        TransactionManager.class,
        TransactionTask.class,
        TxTask.class,
        AssertUtils.class,
        ByteArrayIOStream.class,
        Sha256Hash.class,
        DeleteOnCloseFileInputStream.class,
        FileUtils.class,
        TransactionFailedRetriableException.class,
        StreamCleanedException.class,
        AbstractPersistentStreamStore.class,
        AbstractExpiringStreamStore.class,
        List.class,
        CheckForNull.class,
    };
}
