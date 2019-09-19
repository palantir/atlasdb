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
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.NamedColumnDescription.Builder;
import javax.annotation.concurrent.Immutable;

@Immutable
public class NamedColumnDescription {
    final String shortName;
    final String longName;
    final ColumnValueDescription value;
    final LogSafety logSafety;

    public NamedColumnDescription(String shortName, String longName, ColumnValueDescription value) {
        this(shortName, longName, value, LogSafety.UNSAFE);
    }

    public NamedColumnDescription(String shortName,
                                  String longName,
                                  ColumnValueDescription value,
                                  LogSafety logSafety) {
        this.shortName = shortName;
        this.longName = longName;
        this.value = value;
        this.logSafety = logSafety;
    }

    public String getShortName() {
        return shortName;
    }

    public String getLongName() {
        return longName;
    }

    public ColumnValueDescription getValue() {
        return value;
    }

    public LogSafety getLogSafety() {
        return logSafety;
    }

    public TableMetadataPersistence.NamedColumnDescription.Builder persistToProto() {
        Builder builder = TableMetadataPersistence.NamedColumnDescription.newBuilder();
        builder.setShortName(shortName);
        builder.setLongName(longName);
        builder.setValue(value.persistToProto());
        builder.setLogSafety(logSafety);
        return builder;
    }

    public static NamedColumnDescription hydrateFromProto(TableMetadataPersistence.NamedColumnDescription message) {
        return new NamedColumnDescription(
                message.getShortName(),
                message.getLongName(),
                ColumnValueDescription.hydrateFromProto(message.getValue()),
                message.getLogSafety());
    }

    @Override
    public String toString() {
        return "NamedColumnDescription [shortName=" + shortName + ", longName=" + longName
                + ", value=" + value + ", logSafety=" + logSafety + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;

        int result = 1;
        result = prime * result + (shortName == null ? 0 : shortName.hashCode());
        result = prime * result + (longName == null ? 0 : longName.hashCode());
        result = prime * result + (value == null ? 0 : value.hashCode());
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
        NamedColumnDescription other = (NamedColumnDescription) obj;
        if (shortName == null) {
            if (other.getShortName() != null) {
                return false;
            }
        } else if (!shortName.equals(other.getShortName())) {
            return false;
        }
        if (longName == null) {
            if (other.getLongName() != null) {
                return false;
            }
        } else if (!longName.equals(other.getLongName())) {
            return false;
        }
        if (value == null) {
            if (other.getValue() != null) {
                return false;
            }
        } else if (!value.equals(other.getValue())) {
            return false;
        }
        if (logSafety != other.logSafety) {
            return false;
        }
        return true;
    }
}
