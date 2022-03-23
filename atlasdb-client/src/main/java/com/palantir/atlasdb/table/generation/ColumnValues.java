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
package com.palantir.atlasdb.table.generation;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.AbstractMessage;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.table.api.ColumnValue;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;
import com.palantir.common.persist.Persistable.Hydrator;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public final class ColumnValues {

    private ColumnValues() {
        // should not be instantiated
    }

    public static <T extends Persistable, V extends ColumnValue<?>> Map<Cell, byte[]> toCellValues(Multimap<T, V> map) {
        Map<Cell, byte[]> ret = Maps.newHashMapWithExpectedSize(map.size());
        for (Map.Entry<T, Collection<V>> e : map.asMap().entrySet()) {
            byte[] rowName = e.getKey().persistToBytes();
            for (V val : e.getValue()) {
                ret.put(Cell.create(rowName, val.persistColumnName()), val.persistValue());
            }
        }
        return ret;
    }

    public static <T extends Persistable, V extends ColumnValue<?>> Map.Entry<Cell, byte[]> toCellValue(
            T key, V value) {
        return Map.entry(Cell.create(key.persistToBytes(), value.persistColumnName()), value.persistValue());
    }

    public static <T extends Persistable, V extends ColumnValue<?>> Set<Cell> toCells(Multimap<T, V> map) {
        Set<Cell> ret = Sets.newHashSetWithExpectedSize(map.size());
        for (Map.Entry<T, Collection<V>> e : map.asMap().entrySet()) {
            byte[] rowName = e.getKey().persistToBytes();
            for (ColumnValue<?> val : e.getValue()) {
                ret.add(Cell.create(rowName, val.persistColumnName()));
            }
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public static <T extends AbstractMessage> T parseProtoBuf(Class<T> clazz, byte[] msg) {
        try {
            Method parseMethod = clazz.getMethod("parseFrom", byte[].class);
            return (T) parseMethod.invoke(null, msg);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    public static <T extends Persistable> T parsePersistable(Class<T> persistableClazz, byte[] bytes) {
        try {
            Field field = persistableClazz.getDeclaredField(Persistable.HYDRATOR_NAME);
            @SuppressWarnings("unchecked")
            Hydrator<T> hydrator = (Hydrator<T>) field.get(null);
            return hydrator.hydrateFromBytes(bytes);
        } catch (SecurityException e) {
            throw Throwables.throwUncheckedException(e);
        } catch (NoSuchFieldException e) {
            throw Throwables.throwUncheckedException(e);
        } catch (IllegalAccessException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }
}
