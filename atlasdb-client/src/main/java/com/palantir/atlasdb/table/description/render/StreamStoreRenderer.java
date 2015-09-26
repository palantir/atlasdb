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
                _("package ", packageName, ";");
                _();
                importRenderer.renderImports();
                _();
                _("public final class ", StreamStore, " extends ", (isExpiring() ? "AbstractExpiringStreamStore<" + StreamId + ">" : "AbstractPersistentStreamStore"), " {"); {
                    fields();
                    _();
                    constructors();
                    _();
                    getInMemoryThreshold();
                    _();
                    storeBlock();
                    _();
                    touchMetadataWhileStoringForConflicts();
                    _();
                    putMetadataAndHashIndexTask();
                    _();
                    getNumberOfBlocksFromMetadata();
                    _();
                    createTempFile();
                    _();
                    loadSingleBlockToOutputStream();
                    _();
                    getBlock();
                    _();
                    getMetadata();
                    _();
                    lookupStreamIdByHash();
                    _();
                    putHashIndexTask();
                    _();
                    deleteStreams();
                    if (!isExpiring()) {
                        _();
                        markStreamsAsUsed();
                        _();
                        unmarkStreamAsUsed();
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
                _("private final ", TableFactory, " tables;");
            }

            private void constructors() {
                _("private ", StreamStore, "(TransactionManager txManager, ", TableFactory, " tables) {"); {
                    _("super(txManager);");
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
                _("static ", StreamStore, " of(", TableFactory, " tables) {"); {
                    _("return new ", StreamStore, "(null, tables);");
                } _("}");
            }

            private void storeBlock() {
                String params = isExpiring() ? ", final long duration, final TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                String streamType = isExpiring() ? StreamId : "long";
                _("@Override");
                _("protected void storeBlock(", streamType, " id, long blockNumber, final byte[] block", params, ") {"); {
                    _("Preconditions.checkArgument(block.length <= BLOCK_SIZE_IN_BYTES, \"Block to store in DB must be less than BLOCK_SIZE_IN_BYTES\");");
                    _("Preconditions.checkNotNull(txnMgr);");
                    _("final ", StreamValueRow, " row = ", StreamValueRow, ".of(id, blockNumber);");
                    _("try {"); {
                        _("txnMgr.runTaskThrowOnConflict(new TransactionTask<Void, RuntimeException>() {"); {
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

            private void putMetadataAndHashIndexTask() {
                String streamType = isExpiring() ? StreamId : "long";
                String params = isExpiring() ? ", long duration, TimeUnit unit" : "";
                String args = isExpiring() ? ", duration, unit" : "";
                _("@Override");
                _("protected void putMetadataAndHashIndexTask(Transaction t, ", streamType, " streamId, StreamMetadata metadata", params, ") {"); {
                    _(StreamMetadataRow, " row = ", StreamMetadataRow, ".of(streamId);");
                    _(StreamMetadataTable, " mdTable = tables.get", StreamMetadataTable, "(t);");
                    _("if (metadata.getStatus() == Status.STORED) {"); {
                        _("StreamMetadata prevMetadata = getMetadata(t, streamId);");
                        _("if (prevMetadata == null || prevMetadata.getStatus() != Status.STORING) {"); {
                            _("// This can happen if we cleanup old streams.");
                            _("throw new TransactionFailedRetriableException(\"Cannot mark a stream as stored that isn't currently storing: \" + prevMetadata);");
                        } _("}");
                        _("putHashIndexTask(t, row, metadata", args, ");");
                    } _("} else if (metadata.getStatus() == Status.STORING) {"); {
                        _("StreamMetadata prevMetadata = getMetadata(t, streamId);");
                        _("// This will prevent two users trying to store the same id.");
                        _("if (prevMetadata != null) {"); {
                            _("throw new TransactionFailedRetriableException(\"Cannot reuse the same stream id: \" + streamId);");
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

            private void getInMemoryThreshold() {
                _("@Override");
                _("protected long getInMemoryThreshold() {"); {
                    _("return IN_MEMORY_THRESHOLD;");
                } _("}");
            }

            private void createTempFile() {
                _("@Override");
                _("protected File createTempFile(", StreamId, " id) throws IOException {"); {
                    _("File file = FileUtils.createTempFile(STREAM_FILE_PREFIX + id, STREAM_FILE_SUFFIX);");
                    _("file.deleteOnExit();");
                    _("return file;");
                } _("}");
            }

            private void loadSingleBlockToOutputStream() {
                _("@Override");
                _("protected void loadSingleBlockToOutputStream(Transaction t, ", StreamId, " streamId, long blockId, OutputStream os) {"); {
                    _(StreamValueRow, " row = ", StreamValueRow, ".of(streamId, blockId);");
                    _("try {"); {
                        _("os.write(getBlock(t, row));");
                    } _("} catch (RuntimeException e) {"); {
                        _("log.error(\"Error getting block \" + row.getBlockId() + \" of stream \" + row.getId(), e);");
                        _("throw e;");
                    } _("} catch (IOException e) {"); {
                        _("log.error(\"Error writing block \" + row.getBlockId() + \" to file when getting stream id \" + row.getId(), e);");
                        _("throw Throwables.rewrapAndThrowUncheckedException(\"Error writing blocks to file when creating stream.\", e);");
                    } _("}");
                } _("}");
            }

            private void getBlock() {
                _("private byte[] getBlock(Transaction t, ", StreamValueRow, " row) {"); {
                    _(StreamValueTable, " valueTable = tables.get", StreamValueTable, "(t);");
                    _("return valueTable.getValues(ImmutableSet.of(row)).get(row);");
                } _("}");
            }

            private void getMetadata() {
                _("@Override");
                _("protected StreamMetadata getMetadata(Transaction t, ", StreamId, " streamId) {"); {
                    _(StreamMetadataRow, " row = ", StreamMetadataRow, ".of(streamId);");
                    _(StreamMetadataTable, " table = tables.get", StreamMetadataTable, "(t);");
                    _("return table.getMetadatas(ImmutableSet.of(row)).get(row);");
                } _("}");
            }

            private void lookupStreamIdByHash() {
                String streamType = isExpiring() ? StreamId : "long";
                _("@Override");
                _("@CheckForNull");
                _("public ", StreamId, " lookupStreamIdByHash(Transaction t, Sha256Hash hash) {"); {
                    _(StreamHashAidxTable, " idx = tables.get", StreamHashAidxTable, "(t);");

                    _("List<", StreamHashAidxColumnValue, "> columns = idx.getRowColumns(", StreamHashAidxRow, ".of(hash));");
                    _("for (", StreamHashAidxColumnValue, " colVal : columns) {"); {
                        _(streamType, " streamId = colVal.getColumnName().getStreamId();");
                        _("StreamMetadata meta = getMetadata(t, streamId);");
                        _("if (meta.getStatus() == Status.STORED) {"); {
                            _("return streamId;");
                        } _("}");
                    } _("}");
                    _("return null;");
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
                _("/**");
                _(" * This should only be used from the cleanup tasks.");
                _(" */");
                _("void deleteStreams(Transaction t, final Set<", StreamId, "> streamIds) {"); {
                    _("if (streamIds.isEmpty()) {"); {
                        _("return;");
                    } _("}");

                    _("Set<", StreamMetadataRow, "> smRows = Sets.newHashSet();");
                    _("Multimap<", StreamHashAidxRow, ", ", StreamHashAidxColumn, "> shToDelete = HashMultimap.create();");
                    _("for (", StreamId, " streamId : streamIds) {"); {
                        _("smRows.add(", StreamMetadataRow, ".of(streamId));");
                    } _("}");
                    _(StreamMetadataTable, " table = tables.get", StreamMetadataTable, "(t);");
                    _("Map<", StreamMetadataRow, ", StreamMetadata> metadatas = table.getMetadatas(smRows);");

                    _("Set<", StreamValueRow, "> streamValueToDelete = Sets.newHashSet();");
                    _("for (Entry<", StreamMetadataRow, ", StreamMetadata> e : metadatas.entrySet()) {"); {
                        _(StreamId, " streamId = e.getKey().getId();");
                        _("long blocks = getNumberOfBlocksFromMetadata(e.getValue());");
                        _("for (long i = 0; i < blocks; i++) {"); {
                            _("streamValueToDelete.add(", StreamValueRow, ".of(streamId, i));");
                        } _("}");

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

                    _("tables.get", StreamHashAidxTable, "(t).delete(shToDelete);");
                    _("tables.get", StreamValueTable, "(t).delete(streamValueToDelete);");
                    _("table.delete(smRows);");
                } _("}");
            }

            private void touchMetadataWhileMarkingUsedForConflicts() {
                _("@Override");
                _("protected void touchMetadataWhileMarkingUsedForConflicts(Transaction t, long streamId) {"); {
                    _(StreamMetadataTable, " metaTable = tables.get", StreamMetadataTable, "(t);");
                    _("Set<", StreamMetadataRow, "> rows = Sets.newHashSet();");
                    _("rows.add(", StreamMetadataRow, ".of(streamId));");
                    _("Map<", StreamMetadataRow, ", StreamMetadata> metadatas = metaTable.getMetadatas(rows);");
                    _("for (Map.Entry<", StreamMetadataRow, ", StreamMetadata> e : metadatas.entrySet()) {"); {
                        _("StreamMetadata metadata = e.getValue();");
                        _("Preconditions.checkState(metadata.getStatus() == Status.STORED,");
                        _("        \"Stream: \" + e.getKey().getId() + \" has status: \" + metadata.getStatus());");
                        _("metaTable.putMetadata(e.getKey(), metadata);");
                    } _("}");
                    _("SetView<", StreamMetadataRow, "> missingRows = Sets.difference(rows, metadatas.keySet());");
                    _("if (!missingRows.isEmpty()) {"); {
                        _("throw new StreamCleanedException(\"Missing metadata rows for:\" + missingRows");
                        _("        + \" rows: \" + rows + \" metadata: \" + metadatas + \" txn timestamp: \" + t.getTimestamp());");
                    } _("}");
                } _("}");
            }

            private void markStreamsAsUsed() {
                _("@Override");
                _("protected void markStreamAsUsedInternal(Transaction t, long streamId, byte[] reference) {"); {
                    _(StreamIdxTable, " index = tables.get", StreamIdxTable, "(t);");
                    _(StreamIdxColumn, " col = ", StreamIdxColumn, ".of(reference);");
                    _(StreamIdxColumnValue, " value = ", StreamIdxColumnValue, ".of(col, 0L);");
                    _("index.put(", StreamIdxRow, ".of(streamId), value);");
                } _("}");
            }

            private void unmarkStreamAsUsed() {
                _("@Override");
                _("public void unmarkStreamAsUsed(Transaction t, long streamId, byte[] reference) {"); {
                    _(StreamIdxTable, " index = tables.get", StreamIdxTable, "(t);");
                    _(StreamIdxRow, " row = ", StreamIdxRow, ".of(streamId);");
                    _(StreamIdxColumn, " col = ", StreamIdxColumn, ".of(reference);");
                    _("index.delete(row, col);");
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
                    _(StreamStore, ".of(tables).deleteStreams(t, toDelete);");
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
                _("import com.palantir.atlasdb.cleaner.api.OnCleanupTask;");
                _("import com.palantir.atlasdb.keyvalue.api.Cell;");
                _("import com.palantir.atlasdb.protos.generated.StreamPersistence.Status;");
                _("import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;");
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
                    _(StreamStore, ".of(tables).deleteStreams(t, toDelete);");
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
