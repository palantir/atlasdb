/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.hackweek;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.GeneratedMessageV3;
import com.palantir.atlasdb.protos.generated.TransactionService;
import com.palantir.atlasdb.protos.generated.TransactionService.CheckReadConflictsResponse;
import com.palantir.atlasdb.protos.generated.TransactionService.CommitWritesRequest;
import com.palantir.atlasdb.protos.generated.TransactionService.CommitWritesResponse;
import com.palantir.atlasdb.protos.generated.TransactionService.GetFreshTimestampRequest;
import com.palantir.atlasdb.protos.generated.TransactionService.GetImmutableTimestampRequest;
import com.palantir.atlasdb.protos.generated.TransactionService.ImmutableTimestamp;
import com.palantir.atlasdb.protos.generated.TransactionService.StartTransactionsRequest;
import com.palantir.atlasdb.protos.generated.TransactionService.Timestamp;
import com.palantir.atlasdb.protos.generated.TransactionService.TimestampRange;
import com.palantir.atlasdb.protos.generated.TransactionService.UnlockRequest;
import com.palantir.atlasdb.protos.generated.TransactionService.WaitForCommitRequest;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class TransactionServiceClient implements JamesTransactionService {
    private static final MediaType OCTET_STREAM = MediaType.parse("application/octet-stream");
    private final OkHttpClient client;
    private final String url;

    public TransactionServiceClient(OkHttpClient client, String url) {
        this.client = client;
        this.url = url;
    }

    private Request request(GeneratedMessageV3 body) {
        return new Request.Builder()
                .url(url)
                .post(RequestBody.create(OCTET_STREAM, body.toByteArray()))
                .build();
    }

    private <T> T deserialize(Response response, Deserializer<T> deserializer) throws IOException {
        checkState(response.isSuccessful());
        return deserializer.deserialize(response.body().byteStream());
    }

    private interface Deserializer<T> {
        T deserialize(InputStream stream) throws IOException;
    }

    private <T> T execute(GeneratedMessageV3 request, Deserializer<T> deserializer) {
        try {
            return deserialize(client.newCall(request(request)).execute(), deserializer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void execute(GeneratedMessageV3 request) {
        try {
            checkState(client.newCall(request(request)).execute().isSuccessful());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ListenableFuture<?> executeAsync(GeneratedMessageV3 request) {
        SettableFuture<?> future = SettableFuture.create();
        client.newCall(request(request)).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                future.setException(e);
            }

            @Override
            public void onResponse(Call call, Response response) {
                checkState(response.isSuccessful());
                future.set(null);
            }
        });
        return future;
    }

    @Override
    public ImmutableTimestamp getImmutableTimestamp() {
        return execute(GetImmutableTimestampRequest.newBuilder().build(), ImmutableTimestamp::parseFrom);
    }

    @Override
    public Timestamp getFreshTimestamp() {
        return execute(GetFreshTimestampRequest.newBuilder().build(), Timestamp::parseFrom);
    }

    @Override
    public TimestampRange startTransactions(long numberOfTransactions) {
        return execute(
                StartTransactionsRequest.newBuilder().setNumberOfTransactions(numberOfTransactions).build(),
                TimestampRange::parseFrom);
    }

    @Override
    public CommitWritesResponse commitWrites(long startTimestamp,
            List<TransactionService.TableCell> writes) {
        return execute(CommitWritesRequest.newBuilder()
                .addAllWrites(writes)
                .setStartTimestamp(startTimestamp)
                .build(), CommitWritesResponse::parseFrom);
    }

    @Override
    public CheckReadConflictsResponse checkReadConflicts(long startTimestamp,
            List<TransactionService.TableCell> reads, List<TransactionService.TableRange> ranges) {
        return execute(TransactionService.CheckReadConflictsRequest.newBuilder()
                        .setStartTimestamp(startTimestamp)
                        .addAllReads(reads)
                        .addAllRangeReads(ranges)
                        .build(),
                CheckReadConflictsResponse::parseFrom);
    }

    @Override
    public ListenableFuture<?> waitForCommit(List<Long> startTimestamp) {
        return executeAsync(WaitForCommitRequest.newBuilder()
                .addAllStartTimestamps(startTimestamp)
                .build());
    }

    @Override
    public void unlock(List<Long> startTimestamps) {
        execute(UnlockRequest.newBuilder().addAllStartTimestamps(startTimestamps).build());
    }
}
