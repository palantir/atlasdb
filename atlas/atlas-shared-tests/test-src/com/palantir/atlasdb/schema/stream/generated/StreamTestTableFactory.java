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
            StreamTestStreamHashAidxTable.StreamTestStreamHashAidxTrigger,
            StreamTestStreamIdxTable.StreamTestStreamIdxTrigger,
            StreamTestStreamMetadataTable.StreamTestStreamMetadataTrigger,
            StreamTestStreamValueTable.StreamTestStreamValueTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
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