/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.calcite;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.api.AtlasDbService;

public class AtlasTable extends AbstractTable implements ScannableTable {
    private final AtlasDbService service;
    private final Supplier<AtlasTableMetadata> metadata;
    private final String name;

    public static AtlasTable create(AtlasDbService service, String name) {
        return new AtlasTable(service,
                Suppliers.memoize(() -> ImmutableAtlasTableMetadata.of(service.getTableMetadata(name)))::get,
                name);
    }

    private AtlasTable(AtlasDbService service, Supplier<AtlasTableMetadata> metadata, String name) {
        this.service = service;
        this.metadata = metadata;
        this.name = name;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return metadata.get().relDataType(typeFactory);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                PagingAtlasEnumerator delegate =
                        PagingAtlasEnumerator.create(service, metadata.get(), name, cancelFlag);
                return new Enumerator<Object[]>() {
                    @Override
                    public Object[] current() {
                        return delegate.current().toObjectArray();
                    }

                    @Override
                    public boolean moveNext() {
                        return delegate.moveNext();
                    }

                    @Override
                    public void reset() {
                        delegate.reset();
                    }

                    @Override
                    public void close() {
                        delegate.reset();
                    }
                };
            }
        };
    }
}
