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
package com.palantir.atlasdb.http.errors;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.palantir.remoting2.errors.SerializableStackTraceElement;

/**
 * We only need to test that our custom toString() can cope with Optional.empty() in a reasonable way.
 */
public class AtlasDbStackTraceElementTest {
    private static final String CLASS_NAME = "Class$1$2$3";
    private static final String METHOD_NAME = "doWork";
    private static final String FILE_NAME = "foo.class";
    private static final int LINE_NUMBER = 42;

    @Test
    public void canConvertToStringWithNoData() {
        AtlasDbStackTraceElement element = ImmutableAtlasDbStackTraceElement.builder().build();

        assertClassAndMethodName(
                element,
                AtlasDbStackTraceElement.UNKNOWN_CLASS,
                AtlasDbStackTraceElement.UNKNOWN_METHOD);
    }

    @Test
    public void canConvertToStringWithClassNameWithoutMethodName() {
        AtlasDbStackTraceElement element = ImmutableAtlasDbStackTraceElement.builder()
                .className(CLASS_NAME)
                .build();

        assertClassAndMethodName(
                element,
                CLASS_NAME,
                AtlasDbStackTraceElement.UNKNOWN_METHOD);
    }

    @Test
    public void canConvertToStringWithMethodNameWithoutClassName() {
        AtlasDbStackTraceElement element = ImmutableAtlasDbStackTraceElement.builder()
                .methodName(METHOD_NAME)
                .build();

        assertClassAndMethodName(
                element,
                AtlasDbStackTraceElement.UNKNOWN_CLASS,
                METHOD_NAME);
    }

    @Test
    public void canConvertToStringWithBothMethodAndClassName() {
        AtlasDbStackTraceElement element = ImmutableAtlasDbStackTraceElement.builder()
                .className(CLASS_NAME)
                .methodName(METHOD_NAME)
                .build();

        assertClassAndMethodName(element, CLASS_NAME, METHOD_NAME);
    }

    @Test
    public void canConvertToStringWithUnknownLineNumber() {
        AtlasDbStackTraceElement element = ImmutableAtlasDbStackTraceElement.builder()
                .className(CLASS_NAME)
                .methodName(METHOD_NAME)
                .fileName(FILE_NAME)
                .build();

        assertClassAndMethodName(element, CLASS_NAME, METHOD_NAME);
        assertThat(element.toString()).contains(FILE_NAME);
    }

    @Test
    public void canConvertToStringWithKnownLineNumber() {
        AtlasDbStackTraceElement element = ImmutableAtlasDbStackTraceElement.builder()
                .className(CLASS_NAME)
                .methodName(METHOD_NAME)
                .fileName(FILE_NAME)
                .lineNumber(LINE_NUMBER)
                .build();

        assertClassAndMethodName(element, CLASS_NAME, METHOD_NAME);
        assertThat(element.toString())
                .contains(FILE_NAME)
                .contains(String.valueOf(LINE_NUMBER));
    }

    @Test
    public void canCreateFromHttpRemotingSerializableStackTraceElementWithNoData() {
        SerializableStackTraceElement elementWithNoData = SerializableStackTraceElement.builder()
                .build();
        assertCreatedStackTraceElementMatches(elementWithNoData);
    }

    @Test
    public void canCreateFromHttpRemotingSerializableStackTraceElementWithPartialData() {
        SerializableStackTraceElement elementWithPartialData = SerializableStackTraceElement.builder()
                .className(CLASS_NAME)
                .fileName(FILE_NAME)
                .build();
        assertCreatedStackTraceElementMatches(elementWithPartialData);
    }

    @Test
    public void canCreateFromHttpRemotingSerializableStackTraceElementW1ithFullData() {
        SerializableStackTraceElement elementWithFullData = SerializableStackTraceElement.builder()
                .className(CLASS_NAME)
                .methodName(METHOD_NAME)
                .fileName(FILE_NAME)
                .lineNumber(LINE_NUMBER)
                .build();
        assertCreatedStackTraceElementMatches(elementWithFullData);
    }

    private static void assertCreatedStackTraceElementMatches(SerializableStackTraceElement element) {
        AtlasDbStackTraceElement atlasDbElement = AtlasDbStackTraceElement.fromSerializableStackTraceElement(element);
        RemotingExceptionTestUtils.assertStackTraceElementsMatch(atlasDbElement, element);
    }

    private static void assertClassAndMethodName(
            AtlasDbStackTraceElement stackTraceElement,
            String className,
            String methodName) {
        assertThat(stackTraceElement.toString())
                .contains(className)
                .contains(methodName);
    }
}
