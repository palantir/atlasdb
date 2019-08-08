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
package com.palantir.atlasdb.table.description.render;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.logsafe.Preconditions;

public class TypeAndName {
    public final String type;
    public final String name;
    public final ValueByteOrder byteOrder;
    public final ValueType valueType;

    public TypeAndName(String type, String name) {
        this(type, name, ValueByteOrder.ASCENDING, ValueType.BLOB);
    }

    public TypeAndName(String type, String name, ValueByteOrder byteOrder, ValueType valueType) {
        this.type = type;
        this.name = name;
        this.byteOrder = byteOrder;
        this.valueType = valueType;
    }

    public String getType() {
        String start = "";
        if (byteOrder == ValueByteOrder.DESCENDING) {
            start = "@Descending ";
        }
        return start + type;
    }

    public String getTypeAndName() {
        return getType() + " " + name;
    }

    public String getFieldName() {
        if (type.endsWith("...")) {
            return new TypeAndName(type.replace("...", "[]"), name, byteOrder, valueType).getTypeAndName();
        } else {
            return getTypeAndName();
        }
    }

    @Override
    public String toString() {
        return getTypeAndName();
    }

    public boolean isListType() {
        return type.startsWith("List<") && type.endsWith(">");
    }

    public String getNonListType() {
        Preconditions.checkArgument(isListType());
        return type.replaceFirst("List<", "").replaceAll(">$", "");
    }

    public String getVarArgParameterFromList(String separator, int requiredArgs) {
        Preconditions.checkArgument(isListType());
        String nonListType = getType().replaceFirst("List<", "").replaceAll(">$", "");
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < requiredArgs; i++) {
            s.append(nonListType + " " + name + i).append(separator);
        }
        return s.append(nonListType + "... " + name + requiredArgs).toString();
    }

    public String getListFromVarArgParameter(int requiredArgs) {
        Preconditions.checkArgument(isListType());
        StringBuilder s = new StringBuilder(type.replaceFirst("List<", "ImmutableList.<"));
        s.append("builder()");
        for (int i = 0; i < requiredArgs; i++) {
            s.append(".add(").append(name + i).append(")");
        }
        return s.append(".add(").append(name + requiredArgs).append(").build()").toString();
    }
}
