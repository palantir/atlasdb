/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.schema.stream.generated;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

public class StreamTestTableFactory {
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    public static StreamTestTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new StreamTestTableFactory(sharedTriggers);
    }

    private StreamTestTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        this.sharedTriggers = sharedTriggers;
    }

    public static StreamTestTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of());
    }

    public StreamTest2StreamHashAidxTable getStreamTest2StreamHashAidxTable(Transaction t, StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxTrigger... triggers) {
        return StreamTest2StreamHashAidxTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTest2StreamIdxTable getStreamTest2StreamIdxTable(Transaction t, StreamTest2StreamIdxTable.StreamTest2StreamIdxTrigger... triggers) {
        return StreamTest2StreamIdxTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTest2StreamMetadataTable getStreamTest2StreamMetadataTable(Transaction t, StreamTest2StreamMetadataTable.StreamTest2StreamMetadataTrigger... triggers) {
        return StreamTest2StreamMetadataTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTest2StreamValueTable getStreamTest2StreamValueTable(Transaction t, StreamTest2StreamValueTable.StreamTest2StreamValueTrigger... triggers) {
        return StreamTest2StreamValueTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestStreamHashAidxTable getStreamTestStreamHashAidxTable(Transaction t, StreamTestStreamHashAidxTable.StreamTestStreamHashAidxTrigger... triggers) {
        return StreamTestStreamHashAidxTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestStreamIdxTable getStreamTestStreamIdxTable(Transaction t, StreamTestStreamIdxTable.StreamTestStreamIdxTrigger... triggers) {
        return StreamTestStreamIdxTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestStreamMetadataTable getStreamTestStreamMetadataTable(Transaction t, StreamTestStreamMetadataTable.StreamTestStreamMetadataTrigger... triggers) {
        return StreamTestStreamMetadataTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public StreamTestStreamValueTable getStreamTestStreamValueTable(Transaction t, StreamTestStreamValueTable.StreamTestStreamValueTrigger... triggers) {
        return StreamTestStreamValueTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends
            StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxTrigger,
            StreamTest2StreamIdxTable.StreamTest2StreamIdxTrigger,
            StreamTest2StreamMetadataTable.StreamTest2StreamMetadataTrigger,
            StreamTest2StreamValueTable.StreamTest2StreamValueTrigger,
            StreamTestStreamHashAidxTable.StreamTestStreamHashAidxTrigger,
            StreamTestStreamIdxTable.StreamTestStreamIdxTrigger,
            StreamTestStreamMetadataTable.StreamTestStreamMetadataTrigger,
            StreamTestStreamValueTable.StreamTestStreamValueTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putStreamTest2StreamHashAidx(Multimap<StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxRow, ? extends StreamTest2StreamHashAidxTable.StreamTest2StreamHashAidxColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTest2StreamIdx(Multimap<StreamTest2StreamIdxTable.StreamTest2StreamIdxRow, ? extends StreamTest2StreamIdxTable.StreamTest2StreamIdxColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTest2StreamMetadata(Multimap<StreamTest2StreamMetadataTable.StreamTest2StreamMetadataRow, ? extends StreamTest2StreamMetadataTable.StreamTest2StreamMetadataNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTest2StreamValue(Multimap<StreamTest2StreamValueTable.StreamTest2StreamValueRow, ? extends StreamTest2StreamValueTable.StreamTest2StreamValueNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestStreamHashAidx(Multimap<StreamTestStreamHashAidxTable.StreamTestStreamHashAidxRow, ? extends StreamTestStreamHashAidxTable.StreamTestStreamHashAidxColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestStreamIdx(Multimap<StreamTestStreamIdxTable.StreamTestStreamIdxRow, ? extends StreamTestStreamIdxTable.StreamTestStreamIdxColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestStreamMetadata(Multimap<StreamTestStreamMetadataTable.StreamTestStreamMetadataRow, ? extends StreamTestStreamMetadataTable.StreamTestStreamMetadataNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putStreamTestStreamValue(Multimap<StreamTestStreamValueTable.StreamTestStreamValueRow, ? extends StreamTestStreamValueTable.StreamTestStreamValueNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}
