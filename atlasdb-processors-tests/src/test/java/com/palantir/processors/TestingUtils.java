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
package com.palantir.processors;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

final class TestingUtils {
    private TestingUtils() {}

    static <T> Set<String> extractMethods(Class<T> klass) {
        return extractMethodsSatisfyingPredicate(klass, _unused -> true);
    }

    static <T> Set<String> extractMethodsSatisfyingPredicate(Class<T> klass, Predicate<Method> predicate) {
        return Arrays.stream(klass.getDeclaredMethods())
                .filter(predicate)
                .map(TestingUtils::methodToString)
                .collect(Collectors.toSet());
    }

    private static String methodToString(Method method) {
        return String.format("%s,%s,%s", method.getReturnType(), method.getName(), extractMethodParameterTypes(method));
    }

    private static String extractMethodParameterTypes(Method method) {
        return Arrays.stream(method.getParameterTypes())
                .map(Class::getCanonicalName)
                .collect(Collectors.toList())
                .toString();
    }

    static <T> Set<String> extractNonStaticMethods(Class<T> klass) {
        return extractMethodsSatisfyingPredicate(klass, method -> !Modifier.isStatic(method.getModifiers()));
    }
}
