// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.common.persist;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import com.google.common.collect.Lists;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.JsonPersistable.Hydrator;

public class JsonPersistables {
    private JsonPersistables() { /* */ }

    @SuppressWarnings("unchecked")
    public static <T extends JsonPersistable> Hydrator<T> getHydrator(Class<T> clazz) {
        try {
            Field field = clazz.getField(JsonPersistable.HYDRATOR_NAME);
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

    public static List<String> persistAll(Iterable<? extends JsonPersistable> persistables) {
        List<String> ret = Lists.newArrayList();
        for (JsonPersistable persistable : persistables) {
            ret.add(persistable.persistToJson());
        }
        return ret;
    }

}
