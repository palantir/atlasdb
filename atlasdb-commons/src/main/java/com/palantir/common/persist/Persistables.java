/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.common.persist;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable.Hydrator;

public class Persistables {
    @SuppressWarnings("unchecked")
    public static <T extends Persistable> Hydrator<T> getHydrator(Class<T> clazz) {
        try {
            Field field = clazz.getField(Persistable.HYDRATOR_NAME);
            int modifiers = field.getModifiers();
            if (!Modifier.isPublic(modifiers)
                    || !Modifier.isStatic(modifiers)
                    || !Modifier.isFinal(modifiers)) {
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
        return input -> input.persistToBytes();
    }

    public static List<byte[]> persistAll(Iterable<? extends Persistable> persistables) {
        List<byte[]> output;
        if (persistables instanceof Collection) {
            output = Lists.newArrayListWithCapacity(((Collection<? extends Persistable>) persistables).size());
        } else {
            output = Lists.newArrayList();
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
