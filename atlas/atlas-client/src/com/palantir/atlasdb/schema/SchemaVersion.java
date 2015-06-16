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

package com.palantir.atlasdb.schema;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.protos.generated.UpgradePersistence;

public class SchemaVersion {
    private static final SchemaVersion INVALID = new SchemaVersion(0L, 0L, 0L);

    public static SchemaVersion create(long version) {
        return create(version, 0L);
    }

    public static SchemaVersion create(long version, long hotfix) {
        return create(version, hotfix, 0L);
    }

    public static SchemaVersion create(long version, long hotfix, long hotfixHotfix) {
        Preconditions.checkArgument(version > 0, "version must be greater than 0");
        return new SchemaVersion(version, hotfix, hotfixHotfix);
    }

    public static SchemaVersion invalidSchemaVersion() {
        return INVALID;
    }

    public final static Function<SchemaVersion, UpgradePersistence.SchemaVersion> persister =
            new Function<SchemaVersion, UpgradePersistence.SchemaVersion>() {
        @Override
        public UpgradePersistence.SchemaVersion apply(SchemaVersion version) {
            return version.toProto();
        }
    };

    public final static Function<UpgradePersistence.SchemaVersion, SchemaVersion> hydrator =
            new Function<UpgradePersistence.SchemaVersion, SchemaVersion>() {
        @Override
        public SchemaVersion apply(UpgradePersistence.SchemaVersion proto) {
            return SchemaVersion.fromProto(proto);
        }
    };

    public static SchemaVersion fromProto(UpgradePersistence.SchemaVersion proto) {
        return create(proto.getVersion(), proto.getHotfix(), proto.getHotfixHotfix());
    }

    private final long version;
    private final long hotfix;
    private final long hotfixHotfix;

    private SchemaVersion(long version, long hotfix, long hotfixHotfix) {
        Preconditions.checkArgument(version >= 0, "version must be at least 0");
        Preconditions.checkArgument(hotfix >= 0, "hotfix must be at least 0");
        Preconditions.checkArgument(hotfixHotfix >= 0, "hotfix hotfix must be at least 0");
        this.version = version;
        this.hotfix = hotfix;
        this.hotfixHotfix = hotfixHotfix;
    }

    public long getVersion() {
        return version;
    }

    public long getHotfix() {
        return hotfix;
    }

    public long getHotfixHotfix() {
        return hotfixHotfix;
    }

    public UpgradePersistence.SchemaVersion toProto() {
        return UpgradePersistence.SchemaVersion.newBuilder().
                setVersion(version).
                setHotfix(hotfix).
                setHotfixHotfix(hotfixHotfix).
                build();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (hotfix ^ (hotfix >>> 32));
        result = prime * result + (int) (hotfixHotfix ^ (hotfixHotfix >>> 32));
        result = prime * result + (int) (version ^ (version >>> 32));
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
        SchemaVersion other = (SchemaVersion) obj;
        if (hotfix != other.hotfix)
            return false;
        if (hotfixHotfix != other.hotfixHotfix)
            return false;
        if (version != other.version)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SchemaVersion [version=" + version + ", hotfix=" + hotfix + ", hotfixHotfix="
                + hotfixHotfix + "]";
    }

    public static Ordering<SchemaVersion> getOrdering() {
        return new Ordering<SchemaVersion>() {
            @Override
            public int compare(SchemaVersion left, SchemaVersion right) {
                if (left.version != right.version) {
                    return Longs.compare(left.version, right.version);
                } else if (left.hotfix != right.hotfix) {
                    return Longs.compare(left.hotfix, right.hotfix);
                } else {
                    return Longs.compare(left.hotfixHotfix, right.hotfixHotfix);
                }
            }
        };
    }
}
