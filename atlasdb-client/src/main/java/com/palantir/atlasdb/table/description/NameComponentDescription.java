/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
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
package com.palantir.atlasdb.table.description;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.logsafe.Preconditions;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class NameComponentDescription {
    final String componentName;
    final ValueType type;
    final ValueByteOrder order;

    @Nullable
    final UniformRowNamePartitioner uniformPartitioner;

    @Nullable
    final ExplicitRowNamePartitioner explicitPartitioner;

    final LogSafety logSafety;

    public static NameComponentDescription of(String componentName, ValueType valueType) {
        return new Builder().componentName(componentName).type(valueType).build();
    }

    public static NameComponentDescription safe(String componentName, ValueType valueType) {
        return new Builder()
                .componentName(componentName)
                .type(valueType)
                .logSafety(LogSafety.SAFE)
                .build();
    }

    /**
     * Builder for NameComponentDescription. componentName and valueType are required.
     * uniformRowNamePartitioner will be set to a default value unless explicitly set to null.
     */
    public static final class Builder {
        private String componentName;
        private ValueType type;
        private ValueByteOrder order = ValueByteOrder.ASCENDING;
        private UniformRowNamePartitioner uniformPartitioner;
        private ExplicitRowNamePartitioner explicitPartitioner;
        private LogSafety logSafety = LogSafety.UNSAFE;

        private boolean uniformPartitionerExplicitlyNull = false;

        public Builder componentName(String name) {
            this.componentName = name;
            return this;
        }

        public Builder type(ValueType valueType) {
            this.type = valueType;
            return this;
        }

        public Builder byteOrder(ValueByteOrder byteOrder) {
            this.order = byteOrder;
            return this;
        }

        public Builder uniformRowNamePartitioner(UniformRowNamePartitioner partitioner) {
            this.uniformPartitioner = partitioner;
            uniformPartitionerExplicitlyNull = partitioner == null;
            return this;
        }

        public Builder explicitRowNamePartitioner(ExplicitRowNamePartitioner partitioner) {
            this.explicitPartitioner = partitioner;
            return this;
        }

        public Builder logSafety(LogSafety safety) {
            this.logSafety = safety;
            return this;
        }

        public NameComponentDescription build() {
            Preconditions.checkNotNull(
                    componentName, "componentName must be set when building a NameComponentDescription");
            Preconditions.checkNotNull(type, "type must be set when building a NameComponentDescription");

            if (uniformPartitioner == null && !uniformPartitionerExplicitlyNull) {
                uniformPartitioner = new UniformRowNamePartitioner(type);
            }

            return new NameComponentDescription(
                    componentName, type, order, uniformPartitioner, explicitPartitioner, logSafety);
        }
    }

    private NameComponentDescription(
            String componentName,
            ValueType type,
            ValueByteOrder order,
            UniformRowNamePartitioner uniform,
            ExplicitRowNamePartitioner explicit,
            LogSafety logSafety) {
        this.componentName = componentName;
        this.type = type;
        this.order = order;
        this.uniformPartitioner = uniform;
        this.explicitPartitioner = explicit;
        this.logSafety = logSafety;
    }

    public String getComponentName() {
        return componentName;
    }

    public ValueType getType() {
        return type;
    }

    public boolean isReverseOrder() {
        return order == ValueByteOrder.DESCENDING;
    }

    public ValueByteOrder getOrder() {
        return order;
    }

    public LogSafety getLogSafety() {
        return logSafety;
    }

    public TableMetadataPersistence.NameComponentDescription.Builder persistToProto() {
        TableMetadataPersistence.NameComponentDescription.Builder builder =
                TableMetadataPersistence.NameComponentDescription.newBuilder();
        builder.setComponentName(componentName);
        builder.setType(type.persistToProto());
        builder.setOrder(getOrder());
        builder.setHasUniformPartitioner(uniformPartitioner != null);
        if (explicitPartitioner != null) {
            builder.addAllExplicitPartitions(explicitPartitioner.values);
        }
        builder.setLogSafety(logSafety);
        return builder;
    }

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
        return new NameComponentDescription(
                message.getComponentName(),
                type,
                message.getOrder(),
                uniformPartitioner,
                explicitPartitioner,
                message.getLogSafety());
    }

    /**
     * Returns true iff the component has a uniform partitioner.
     *
     * NB: a component can have both a uniform partitioner and an explicit partitioner
     */
    public boolean hasUniformPartitioner() {
        return uniformPartitioner != null;
    }

    /**
     * Returns true iff the component has an explicit partitioner.
     *
     * NB: a component can have both an explicit partitioner and a uniform partitioner
     */
    @Nullable
    public ExplicitRowNamePartitioner getExplicitPartitioner() {
        return explicitPartitioner;
    }

    public NameComponentDescription withPartitioners(RowNamePartitioner... partitioners) {
        Set<String> explicit = new HashSet<>();
        boolean hasUniform = false;
        for (RowNamePartitioner p : partitioners) {
            hasUniform |= p instanceof UniformRowNamePartitioner;
            if (p instanceof ExplicitRowNamePartitioner) {
                explicit.addAll(((ExplicitRowNamePartitioner) p).values);
            }
        }
        UniformRowNamePartitioner up = null;
        if (hasUniform) {
            up = new UniformRowNamePartitioner(type);
        }
        ExplicitRowNamePartitioner ep = null;
        if (!explicit.isEmpty()) {
            ep = new ExplicitRowNamePartitioner(type, explicit);
        }
        return new NameComponentDescription(componentName, type, order, up, ep, logSafety);
    }

    @Override
    public String toString() {
        return "NameComponentDescription [componentName=" + componentName + ", order=" + order + ", type=" + type
                + ", logSafety=" + logSafety + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;

        int result = 1;
        result = prime * result + (componentName == null ? 0 : componentName.hashCode());
        result = prime * result + (type == null ? 0 : type.hashCode());
        result = prime * result + (order == null ? 0 : order.hashCode());
        result = prime * result + logSafety.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        NameComponentDescription other = (NameComponentDescription) obj;
        if (componentName == null) {
            if (other.getComponentName() != null) {
                return false;
            }
        } else if (!componentName.equals(other.getComponentName())) {
            return false;
        }
        if (type == null) {
            if (other.getType() != null) {
                return false;
            }
        } else if (!type.equals(other.getType())) {
            return false;
        }
        if (order == null) {
            if (other.getOrder() != null) {
                return false;
            }
        } else if (!order.equals(other.getOrder())) {
            return false;
        }
        if (logSafety != other.logSafety) {
            return false;
        }
        return true;
    }
}
