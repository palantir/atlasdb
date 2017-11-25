/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.table.description;

import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.Validate;
import org.immutables.value.Value;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;

@Value.Immutable
public abstract class NameComponentDescription {
    public abstract String getComponentName();
    public abstract ValueType getType();
    abstract ValueByteOrder byteOrder();

    @Nullable
    @Value.Default
    UniformRowNamePartitioner getUniformPartitioner() {
        return null;
    }

    @Nullable
    @Value.Default
    public ExplicitRowNamePartitioner getExplicitPartitioner() {
        return null;
    }

    public abstract LogSafety getLogSafety();

    @Value.Derived
    public ValueByteOrder getOrder() {
        return byteOrder();
    }

    @Value.Derived
    public boolean isReverseOrder() {
        return getOrder() == ValueByteOrder.DESCENDING;
    }

    @Value.Derived
    public TableMetadataPersistence.NameComponentDescription.Builder persistToProto() {
        TableMetadataPersistence.NameComponentDescription.Builder builder
                = TableMetadataPersistence.NameComponentDescription.newBuilder();
        builder.setComponentName(getComponentName());
        builder.setType(getType().persistToProto());
        builder.setOrder(getOrder());
        builder.setHasUniformPartitioner(getUniformPartitioner() != null);
        if (getExplicitPartitioner() != null) {
            builder.addAllExplicitPartitions(getExplicitPartitioner().values);
        }
        builder.setLogSafety(getLogSafety());
        return builder;
    }

    @Value.Derived
    public static NameComponentDescription hydrateFromProto(TableMetadataPersistence.NameComponentDescription message) {
        ValueType type = ValueType.hydrateFromProto(message.getType());
        UniformRowNamePartitioner uniformPartitioner = new UniformRowNamePartitioner(type);
        if (message.hasHasUniformPartitioner()) {
            uniformPartitioner = message.getHasUniformPartitioner() ? new UniformRowNamePartitioner(type) : null;
        }
        ExplicitRowNamePartitioner explicitPartitioner = null;
        if (message.getExplicitPartitionsCount() > 0) {
            explicitPartitioner = new ExplicitRowNamePartitioner(type, message.getExplicitPartitionsList());
        }
        return ImmutableNameComponentDescription.builder()
                .componentName(message.getComponentName())
                .type(type)
                .byteOrder(message.getOrder())
                .uniformPartitioner(uniformPartitioner)
                .explicitPartitioner(explicitPartitioner)
                .logSafety(message.getLogSafety())
                .build();
    }

    /**
     * Returns true iff the component has a uniform partitioner.
     *
     * NB: a component can have both a uniform partitioner and an explicit partitioner
     */
    @Value.Derived
    public boolean hasUniformPartitioner() {
        return getUniformPartitioner() != null;
    }

    @Value.Derived
    public NameComponentDescription withPartitioners(RowNamePartitioner... partitioners) {
        Set<String> explicit = Sets.newHashSet();
        boolean hasUniform = false;
        for (RowNamePartitioner p : partitioners) {
            hasUniform |= p instanceof UniformRowNamePartitioner;
            if (p instanceof ExplicitRowNamePartitioner) {
                explicit.addAll(((ExplicitRowNamePartitioner) p).values);
            }
        }
        UniformRowNamePartitioner up = null;
        if (hasUniform) {
            up = new UniformRowNamePartitioner(getType());
        }
        ExplicitRowNamePartitioner ep = null;
        if (!explicit.isEmpty()) {
            ep = new ExplicitRowNamePartitioner(getType(), explicit);
        }
        return ImmutableNameComponentDescription.builder()
                .componentName(getComponentName())
                .type(getType())
                .byteOrder(getOrder())
                .uniformPartitioner(up)
                .explicitPartitioner(ep)
                .logSafety(getLogSafety())
                .build();
    }
}
