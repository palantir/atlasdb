/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.keyvalue.cassandra.async.queries.CqlQuerySpec;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThrowingCqlClientTest {

    @Mock
    CqlQuerySpec<Object> cqlQuerySpec;

    @Test
    public void executeQueryThrowsException() {
        assertThatLoggableExceptionThrownBy(() -> ThrowingCqlClient.of().executeQuery(cqlQuerySpec)).isInstanceOf(
                SafeIllegalStateException.class);
    }

    @Test
    public void ofReturnsSameInstanceOnSuccessiveInvocations() {
        assertThat(ThrowingCqlClient.of()).isSameAs(ThrowingCqlClient.of());
    }

    @Test
    public void isValidAlwaysFalse() {
        assertThat(ThrowingCqlClient.of().isValid()).isFalse();
    }
}
