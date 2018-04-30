/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.processors;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

final class TestingUtils {
    private TestingUtils() {}

    static Set<String> extractMethods(Class klass) {
        return Arrays.stream(klass.getDeclaredMethods())
                .map(TestingUtils::methodToString)
                .collect(Collectors.toSet());
    }

    static String methodToString(Method method) {
        return String.format("%s,%s,%s",
                method.getReturnType(),
                method.getName(),
                extractMethodParameterTypes(method));
    }

    static String extractMethodParameterTypes(Method method) {
        return Arrays.stream(method.getParameterTypes())
                .map(Class::getCanonicalName)
                .collect(Collectors.toList())
                .toString();
    }

    static Set<String> extractConstructors(Class klass) {
        return Arrays.stream(klass.getConstructors())
                .map(TestingUtils::constructorToString)
                .collect(Collectors.toSet());
    }

    static String constructorToString(Constructor constructor) {
        return String.format("%s", extractConstructorParameterTypes(constructor));
    }

    private static String extractConstructorParameterTypes(Constructor constructor) {
        return Arrays.stream(constructor.getParameterTypes())
                .map(Class::getCanonicalName)
                .collect(Collectors.toList())
                .toString();
    }
}
