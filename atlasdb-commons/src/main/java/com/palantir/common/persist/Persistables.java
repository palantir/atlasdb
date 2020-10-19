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
package com.palantir.common.persist;

import com.google.common.base.Function;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable.Hydrator;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Persistables {
    @SuppressWarnings("unchecked")
    public static <T extends Persistable> Hydrator<T> getHydrator(Class<T> clazz) {
        try {
            Field field = clazz.getField(Persistable.HYDRATOR_NAME);
            int modifiers = field.getModifiers();
            if (!Modifier.isPublic(modifiers) || !Modifier.isStatic(modifiers) || !Modifier.isFinal(modifiers)) {
                return null;
            }
            return (Hydrator<T>) field.get(null);
        } catch (NoSuchFieldException e) {
            return null;
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    public static Function<Persistable, byte[]> persistToBytesFunction() {
        return Persistable::persistToBytes;
    }

    public static List<byte[]> persistAll(Iterable<? extends Persistable> persistables) {
        List<byte[]> output;
        if (persistables instanceof Collection) {
            output = new ArrayList<>(((Collection<? extends Persistable>) persistables).size());
        } else {
            output = new ArrayList<>();
        }
        return persistAll(persistables, output);
    }

    private static List<byte[]> persistAll(Iterable<? extends Persistable> persistables, @Output List<byte[]> output) {
        for (Persistable persistable : persistables) {
            output.add(persistable.persistToBytes());
        }
        return output;
    }
}
