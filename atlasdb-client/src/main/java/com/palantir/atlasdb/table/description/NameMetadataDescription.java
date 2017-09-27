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

import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.NameMetadataDescription.Builder;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.common.base.Throwables;
import com.palantir.util.Pair;

@Immutable
public class NameMetadataDescription {
    public static final String HASH_ROW_COMPONENT_NAME = "hashOfRowComponents";

    private final List<NameComponentDescription> rowParts;
    private final int numberOfComponentsHashed;

    public NameMetadataDescription() {
        this(ImmutableList.of(
                new NameComponentDescription.Builder().componentName("name").type(ValueType.BLOB).build()), 0);
    }

    private NameMetadataDescription(List<NameComponentDescription> components, int numberOfComponentsHashed) {
        Preconditions.checkArgument(!components.isEmpty());
        this.rowParts = ImmutableList.copyOf(components);
        for (NameComponentDescription nameComponent : rowParts.subList(0, rowParts.size() - 1)) {
            if (nameComponent.type == ValueType.BLOB || nameComponent.type == ValueType.STRING) {
                throw new IllegalArgumentException("string or blob must be on end.  components were: " + components);
            }
        }
        this.numberOfComponentsHashed = numberOfComponentsHashed;
    }

    public static NameMetadataDescription create(List<NameComponentDescription> components) {
        return create(components, 0);
    }

    @Deprecated
    public static NameMetadataDescription create(List<NameComponentDescription> components,
            boolean hasFirstComponentHash) {

        return NameMetadataDescription.create(components, hasFirstComponentHash ? 1 : 0);
    }

    public static NameMetadataDescription create(List<NameComponentDescription> components, int numberOfComponentsHashed) {
        Preconditions.checkArgument(numberOfComponentsHashed <= components.size(),
                "Number of hashed components can't exceed total number of row components.");
        if (numberOfComponentsHashed == 0) {
            return new NameMetadataDescription(components, numberOfComponentsHashed);
        } else {
            List<NameComponentDescription> withHashRowComponent = Lists.newArrayListWithCapacity(components.size() + 1);
            withHashRowComponent.add(new NameComponentDescription.Builder()
                    .componentName(HASH_ROW_COMPONENT_NAME)
                    .type(ValueType.FIXED_LONG)
                    .build());
            withHashRowComponent.addAll(components);
            return new NameMetadataDescription(withHashRowComponent, numberOfComponentsHashed);
        }
    }

    public List<NameComponentDescription> getRowParts() {
        return rowParts;
    }

    public int numberOfComponentsHashed() {
        return numberOfComponentsHashed;
    }

    public List<RowNamePartitioner> getPartitionersForRow() {
        NameComponentDescription firstPart = rowParts.get(0);
        List<RowNamePartitioner> partitioners = Lists.newArrayList();
        if (firstPart.hasUniformPartitioner()) {
            Preconditions.checkArgument(UniformRowNamePartitioner.allowsUniformPartitioner(firstPart.getType()));
            partitioners.add(new UniformRowNamePartitioner(firstPart.getType()));
        }
        if (firstPart.getExplicitPartitioner() != null) {
            partitioners.add(firstPart.getExplicitPartitioner());
        }
        if (partitioners.isEmpty()) {
            return partitioners;
        }
        for (NameComponentDescription component : rowParts.subList(1, rowParts.size())) {
            if (!component.hasUniformPartitioner() && component.getExplicitPartitioner() == null) {
                return partitioners;
            }
            List<RowNamePartitioner> nextPartitioners = Lists.newArrayList();
            if (component.getExplicitPartitioner() != null) {
                for (RowNamePartitioner rowNamePartitioner : partitioners) {
                    nextPartitioners.addAll(rowNamePartitioner.compound(component.getExplicitPartitioner()));
                }
            }
            if (component.hasUniformPartitioner()) {
                for (RowNamePartitioner rowNamePartitioner : partitioners) {
                    nextPartitioners.addAll(rowNamePartitioner.compound(
                            new UniformRowNamePartitioner(component.getType())));
                }
            }
            partitioners = nextPartitioners;
        }
        return partitioners;
    }

    public String renderToJson(byte[] name) {
        StringBuilder sb = new StringBuilder("{");
        int offset = 0;
        Iterator<NameComponentDescription> it = rowParts.iterator();
        byte[] flippedName = null;
        while (it.hasNext()) {
            NameComponentDescription component = it.next();
            if (component.isReverseOrder() && flippedName == null) {
                flippedName = EncodingUtils.flipAllBits(name);
            }
            Pair<String, Integer> parse;
            if (component.isReverseOrder()) {
                parse = component.type.convertToJson(flippedName, offset);
            } else {
                parse = component.type.convertToJson(name, offset);
            }
            offset += parse.rhSide;
            sb.append('"').append(component.componentName).append('"').append(": ").append(parse.lhSide);
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        if (offset != name.length) {
            throw new IllegalArgumentException("This name has extra trailing bytes: "
                    + Cells.getNameFromBytes(PtBytes.tail(name, offset))
                    + " trailing " + sb.toString());
        }
        sb.append("}");
        return sb.toString();
    }

    public byte[] parseFromJson(String json, boolean allowPrefix) {
        try {
            JSONObject obj = (JSONObject) new JSONParser().parse(json);
            int numDefinedFields = countNumDefinedFields(obj);
            byte[][] bytes = new byte[numDefinedFields][];

            Preconditions.checkArgument(numDefinedFields > 0,
                    "JSON object needs a field named: %s.  Passed json was: %s",
                    rowParts.get(0).getComponentName(),
                    json);
            Preconditions.checkArgument(allowPrefix || numDefinedFields == rowParts.size(),
                    "JSON object has %s defined fields, but the number of row components is %s.  Passed json was: %s",
                    numDefinedFields,
                    rowParts.size(),
                    json);

            for (int i = 0; i < numDefinedFields; ++i) {
                NameComponentDescription desc = rowParts.get(i);
                String str = String.valueOf(obj.get(desc.getComponentName()));
                bytes[i] = desc.getType().convertFromString(str);
                if (desc.isReverseOrder()) {
                    EncodingUtils.flipAllBitsInPlace(bytes[i]);
                }
            }

            return com.google.common.primitives.Bytes.concat(bytes);
        } catch (ParseException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private int countNumDefinedFields(JSONObject obj) {
        int numFields = 0;
        for (; numFields < rowParts.size(); ++numFields) {
            if (!obj.containsKey(rowParts.get(numFields).getComponentName())) {
                break;
            }
        }

        for (int i = numFields + 1; i < rowParts.size(); ++i) {
            String componentName = rowParts.get(i).getComponentName();
            if (obj.containsKey(componentName)) {
                throw new IllegalArgumentException("JSON object is missing field: "
                        + rowParts.get(i - 1).getComponentName());
            }
        }
        return numFields;
    }

    public TableMetadataPersistence.NameMetadataDescription.Builder persistToProto() {
        Builder builder = TableMetadataPersistence.NameMetadataDescription.newBuilder();
        for (NameComponentDescription part : rowParts) {
            builder.addNameParts(part.persistToProto());
        }
        builder.setNumberOfComponentsHashed(numberOfComponentsHashed);

        return builder;
    }

    public static NameMetadataDescription hydrateFromProto(TableMetadataPersistence.NameMetadataDescription message) {
        List<NameComponentDescription> list = Lists.newArrayList();
        for (TableMetadataPersistence.NameComponentDescription part : message.getNamePartsList()) {
            list.add(NameComponentDescription.hydrateFromProto(part));
        }

        // Call constructor over factory because the list might already contain the hash row component
        return new NameMetadataDescription(list, getNumberOfComponentsHashedFromProto(message));
    }

    private static int getNumberOfComponentsHashedFromProto(TableMetadataPersistence.NameMetadataDescription message) {
        int numberOfComponentsHashed;
        if (message.getHasFirstComponentHash()) {
            numberOfComponentsHashed = 1;
        } else {
            numberOfComponentsHashed = message.getNumberOfComponentsHashed();
        }
        return numberOfComponentsHashed;
    }

    @Override
    public String toString() {
        return "NameMetadataDescription [rowParts=" + rowParts + ", numberOfComponentsHashed=" +
                numberOfComponentsHashed + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (numberOfComponentsHashed == 1 ? 1231 : 1237);
        result = prime * result + ((rowParts == null) ? 0 : rowParts.hashCode());
        if (numberOfComponentsHashed > 1) {
            result = prime * result + numberOfComponentsHashed;
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        NameMetadataDescription that = (NameMetadataDescription) obj;

        if (numberOfComponentsHashed != that.numberOfComponentsHashed) {
            return false;
        }
        return rowParts != null ? rowParts.equals(that.rowParts) : that.rowParts == null;
    }
}
