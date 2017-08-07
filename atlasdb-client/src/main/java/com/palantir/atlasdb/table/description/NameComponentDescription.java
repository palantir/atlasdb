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

import com.google.common.collect.Sets;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;

@Immutable
public class NameComponentDescription {
    final String componentName;
    final ValueType type;
    final ValueByteOrder order;
    @Nullable final UniformRowNamePartitioner uniformPartitioner;
    @Nullable final ExplicitRowNamePartitioner explicitPartitioner;
    final LogSafety logSafety;

    // TODO: make references use the builder
    // TODO 2: remove all of these constructors
    public static final class Builder {
        private String componentName;
        private ValueType type;
        private ValueByteOrder order;
        private UniformRowNamePartitioner uniformPartitioner;
        private ExplicitRowNamePartitioner explicitPartitioner;
        private LogSafety logSafety = LogSafety.UNSAFE;

        Builder componentName(String name) {
            this.componentName = name;
            return this;
        }

        Builder type(ValueType valueType) {
            this.type = valueType;
            return this;
        }

        Builder byteOrder(ValueByteOrder byteOrder) {
            this.order = byteOrder;
            return this;
        }

        Builder uniformRowNamePartitioner(UniformRowNamePartitioner partitioner) {
            this.uniformPartitioner = partitioner;
            return this;
        }

        Builder explicitRowNamePartitioner(ExplicitRowNamePartitioner partitioner) {
            this.explicitPartitioner = partitioner;
            return this;
        }

        Builder logSafety(LogSafety safety) {
            this.logSafety = safety;
            return this;
        }

        NameComponentDescription build() {
            Validate.notNull(componentName, "componentName must be set when building a NameComponentDescription");
            Validate.notNull(type, "type must be set when building a NameComponentDescription");
            Validate.notNull(order, "order must be set when building a NameComponentDescription");

            if (uniformPartitioner == null) {
                uniformPartitioner = new UniformRowNamePartitioner(type);
            }

            return new NameComponentDescription(componentName, type, order, uniformPartitioner, explicitPartitioner, logSafety);
        }
//
    }

    public NameComponentDescription() {
        this("name", ValueType.BLOB);
    }

    public NameComponentDescription(String componentName, ValueType type) {
        this(componentName, type, ValueByteOrder.ASCENDING, new UniformRowNamePartitioner(type), null);
    }

    @Deprecated
    public NameComponentDescription(String componentName, ValueType type, boolean reverseOrder) {
        this.componentName = componentName;
        this.type = type;
        this.order = reverseOrder ? ValueByteOrder.DESCENDING : ValueByteOrder.ASCENDING;
        this.uniformPartitioner = new UniformRowNamePartitioner(type);
        this.explicitPartitioner = null;
        this.logSafety = LogSafety.UNSAFE;
    }

    public NameComponentDescription(String componentName,
            ValueType type,
            ValueByteOrder order) {
        this(componentName, type, order, LogSafety.UNSAFE);
    }

    public NameComponentDescription(String componentName,
            ValueType type,
            ValueByteOrder order,
            LogSafety logSafety) {
        this(componentName, type, order, new UniformRowNamePartitioner(type), null, logSafety);
    }

    public NameComponentDescription(String componentName,
                                    ValueType type,
                                    ValueByteOrder order,
                                    UniformRowNamePartitioner uniform,
                                    ExplicitRowNamePartitioner explicit) {
        this(componentName, type, order, uniform, explicit, LogSafety.UNSAFE);
    }

    public NameComponentDescription(String componentName,
                                    ValueType type,
                                    ValueByteOrder order,
                                    UniformRowNamePartitioner uniform,
                                    ExplicitRowNamePartitioner explicit,
                                    LogSafety logSafety) {
        Validate.notNull(componentName);
        Validate.notNull(type);
        Validate.notNull(order);
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
        TableMetadataPersistence.NameComponentDescription.Builder builder
                = TableMetadataPersistence.NameComponentDescription.newBuilder();
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
     * NB: a component can have both a uniform partitioner and an explicit partitioner
     */
    public boolean hasUniformPartitioner() {
        return uniformPartitioner != null;
    }

    /**
     * NB: a component can have both an explicit partitioner and a uniform partitioner
     */
    @Nullable
    public ExplicitRowNamePartitioner getExplicitPartitioner() {
        return explicitPartitioner;
    }

    public NameComponentDescription withPartitioners(RowNamePartitioner... partitioners) {
        Set<String> explicit = Sets.newHashSet();
        boolean hasUniform = false;
        for (RowNamePartitioner p : partitioners) {
            hasUniform |= p instanceof UniformRowNamePartitioner;
            if (p instanceof ExplicitRowNamePartitioner) {
                explicit.addAll(((ExplicitRowNamePartitioner) p).values);
            }
        }
        UniformRowNamePartitioner u = null;
        if (hasUniform) {
            u = new UniformRowNamePartitioner(type);
        }
        ExplicitRowNamePartitioner e = null;
        if (!explicit.isEmpty()) {
            e = new ExplicitRowNamePartitioner(type, explicit);
        }
        return new NameComponentDescription(componentName, type, order, u, e, logSafety);
    }

    @Override
    public String toString() {
        return "NameComponentDescription [componentName=" + componentName
                + ", order=" + order + ", type=" + type + ", logSafety=" + logSafety + "]";
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
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
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
