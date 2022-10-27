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

package com.palantir.atlasdb.atomic.mcas;

import static com.palantir.logsafe.testing.Assertions.assertThat;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.junit.Test;

public class CasRequestTest {
    private static final byte[] START = PtBytes.toBytes(1);
    private static final ByteBuffer WRAPPED_START = ByteBuffer.wrap(START);

    private static final byte[] COMMIT = PtBytes.toBytes(2889);
    private static final ByteBuffer WRAPPED_COMMIT = ByteBuffer.wrap(COMMIT);
    private static final Cell CELL = Cell.create(PtBytes.toBytes("r"), PtBytes.toBytes("c"));

    @Test
    public void canComputeRank() {
        assertThat(ImmutableCasRequest.of(CELL, WRAPPED_COMMIT, WRAPPED_COMMIT).rank())
                .isEqualTo(UpdateRank.TOUCH);
        assertThat(ImmutableCasRequest.of(CELL, WRAPPED_START, WRAPPED_COMMIT).rank())
                .isEqualTo(UpdateRank.COMMIT);
        assertThat(ImmutableCasRequest.of(
                                CELL,
                                WRAPPED_START,
                                MarkAndCasConsensusForgettingStore.WRAPPED_ABORTED_TRANSACTION_STAGING_VALUE)
                        .rank())
                .isEqualTo(UpdateRank.ABORT);
    }

    @Test
    public void failsCorrectlyForUntriedTouch() {
        CasRequest casRequest = ImmutableCasRequest.of(CELL, WRAPPED_COMMIT, WRAPPED_COMMIT);

        Exception failureUntried = CasRequest.failureUntried(casRequest);
        assertThat(failureUntried).isInstanceOf(CheckAndSetException.class).hasMessageContaining("");

        CheckAndSetException checkAndSetException = (CheckAndSetException) failureUntried;
        assertThat(checkAndSetException.getActualValues()).isEmpty();
    }

    @Test
    public void failsCorrectlyForTouch() {
        CasRequest casRequest = ImmutableCasRequest.of(CELL, WRAPPED_COMMIT, WRAPPED_COMMIT);

        byte[] abortedVal = MarkAndCasConsensusForgettingStore.WRAPPED_ABORTED_TRANSACTION_STAGING_VALUE.array();

        Exception failureUntried = CasRequest.failure(casRequest, Optional.of(abortedVal));
        assertThat(failureUntried).isInstanceOf(CheckAndSetException.class).hasMessageContaining("");

        CheckAndSetException checkAndSetException = (CheckAndSetException) failureUntried;
        assertThat(checkAndSetException.getActualValues()).containsOnly(abortedVal);
    }

    @Test
    public void failsCorrectlyForCommit() {
        CasRequest casRequest = ImmutableCasRequest.of(CELL, WRAPPED_START, WRAPPED_COMMIT);
        assertKeyAlreadyExists(CasRequest.failureUntried(casRequest));
        assertKeyAlreadyExists(CasRequest.failure(casRequest, Optional.of(COMMIT)));
    }

    @Test
    public void failsCorrectlyForAbort() {
        CasRequest casRequest = ImmutableCasRequest.of(
                CELL, WRAPPED_START, MarkAndCasConsensusForgettingStore.WRAPPED_ABORTED_TRANSACTION_STAGING_VALUE);
        assertKeyAlreadyExists(CasRequest.failureUntried(casRequest));
        assertKeyAlreadyExists(CasRequest.failure(casRequest, Optional.of(COMMIT)));
    }

    private static void assertKeyAlreadyExists(Exception failureUntried) {
        assertThat(failureUntried).isInstanceOf(KeyAlreadyExistsException.class).hasMessageContaining("");

        KeyAlreadyExistsException keyAlreadyExistsException = (KeyAlreadyExistsException) failureUntried;
        assertThat(keyAlreadyExistsException.getExistingKeys()).containsOnly(CELL);
    }
}
