/*
 * Copyright 2017 Palantir Technologies
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

        assertThat(element.toString())
                .contains(AtlasDbStackTraceElement.UNKNOWN_CLASS)
                .contains(AtlasDbStackTraceElement.UNKNOWN_METHOD);
    }

    @Test
    public void canConvertToStringWithClassNameWithoutMethodName() {
        AtlasDbStackTraceElement element = ImmutableAtlasDbStackTraceElement.builder()
                .className(CLASS_NAME)
                .build();

        assertThat(element.toString())
                .contains(CLASS_NAME)
                .contains(AtlasDbStackTraceElement.UNKNOWN_METHOD);
    }

    @Test
    public void canConvertToStringWithMethodNameWithoutClassName() {
        AtlasDbStackTraceElement element = ImmutableAtlasDbStackTraceElement.builder()
                .methodName(METHOD_NAME)
                .build();

        assertThat(element.toString())
                .contains(AtlasDbStackTraceElement.UNKNOWN_CLASS)
                .contains(METHOD_NAME);
    }

    @Test
    public void canConvertToStringWithBothMethodAndClassName() {
        AtlasDbStackTraceElement element = ImmutableAtlasDbStackTraceElement.builder()
                .className(CLASS_NAME)
                .methodName(METHOD_NAME)
                .build();

        assertThat(element.toString())
                .contains(CLASS_NAME)
                .contains(METHOD_NAME);
    }

    @Test
    public void canConvertToStringWithUnknownLineNumber() {
        AtlasDbStackTraceElement element = ImmutableAtlasDbStackTraceElement.builder()
                .className(CLASS_NAME)
                .methodName(METHOD_NAME)
                .fileName(FILE_NAME)
                .build();

        assertThat(element.toString())
                .contains(CLASS_NAME)
                .contains(METHOD_NAME)
                .contains(FILE_NAME);
    }

    @Test
    public void canConvertToStringWithKnownLineNumber() {
        AtlasDbStackTraceElement element = ImmutableAtlasDbStackTraceElement.builder()
                .className(CLASS_NAME)
                .methodName(METHOD_NAME)
                .fileName(FILE_NAME)
                .lineNumber(LINE_NUMBER)
                .build();

        assertThat(element.toString())
                .contains(CLASS_NAME)
                .contains(METHOD_NAME)
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
    public void canCreateFromHttpRemotingSerializableStackTraceElementWithFullData() {
        SerializableStackTraceElement elementWithFullData = SerializableStackTraceElement.builder()
                .className(CLASS_NAME)
                .methodName(METHOD_NAME)
                .fileName(FILE_NAME)
                .lineNumber(LINE_NUMBER)
                .build();
        assertCreatedStackTraceElementMatches(elementWithFullData);
    }

    private void assertCreatedStackTraceElementMatches(SerializableStackTraceElement element) {
        AtlasDbStackTraceElement atlasDbElement = AtlasDbStackTraceElement.fromSerializableStackTraceElement(element);
        RemotingAssertions.assertStackTraceElementsMatch(atlasDbElement, element);
    }
}
