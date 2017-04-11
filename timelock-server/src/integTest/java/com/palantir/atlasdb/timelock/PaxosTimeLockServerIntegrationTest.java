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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.assertj.core.util.Lists;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

import io.dropwizard.testing.ResourceHelpers;

public class PaxosTimeLockServerIntegrationTest {
    private static final String NOT_FOUND_CODE = "404";
    private static final String TOO_MANY_REQUESTS_CODE="429";

    private static final String CLIENT_1 = "test";
    private static final String CLIENT_2 = "test2";
    private static final String CLIENT_3 = "test3";
    private static final String NONEXISTENT_CLIENT = "nonexistent";
    private static final String INVALID_CLIENT = "test2\b";

    private static final int NUM_CLIENTS = 5;
    private static final int MAX_SERVER_THREADS = 50;
    private static final int SELECTOR_THREADS = 8;
    private static final int ACCEPTOR_THREADS = 4;
    private static final int AVAILABLE_THREADS = MAX_SERVER_THREADS - SELECTOR_THREADS - ACCEPTOR_THREADS - 1;
    private static final int LOCAL_TC_LIMIT = AVAILABLE_THREADS / 2 / NUM_CLIENTS;
    private static final int GLOBAL_TC_LIMIT = AVAILABLE_THREADS - LOCAL_TC_LIMIT * NUM_CLIENTS;


    private static final long ONE_MILLION = 1000000;
    private static final long TWO_MILLION = 2000000;
    private static final int FORTY_TWO = 42;

    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();
    private static final String LOCK_CLIENT_NAME = "lock-client-name";
    private static final SortedMap<LockDescriptor, LockMode> LOCK_MAP =
            ImmutableSortedMap.of(StringLockDescriptor.of("lock1"), LockMode.WRITE);
    private static final File TIMELOCK_CONFIG_TEMPLATE =
            new File(ResourceHelpers.resourceFilePath("paxosSingleServer.yml"));

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
    private static final TemporaryConfigurationHolder TEMPORARY_CONFIG_HOLDER =
            new TemporaryConfigurationHolder(TEMPORARY_FOLDER, TIMELOCK_CONFIG_TEMPLATE);
    private static final TimeLockServerHolder TIMELOCK_SERVER_HOLDER =
            new TimeLockServerHolder(TEMPORARY_CONFIG_HOLDER::getTemporaryConfigFileLocation);

    private final TimestampService timestampService = getTimestampService(CLIENT_1);
    private final TimestampManagementService timestampManagementService = getTimestampManagementService(CLIENT_1);

    private static final LockRequest SLOW_REQUEST = LockRequest
            .builder(ImmutableSortedMap.of(StringLockDescriptor.of("lock"), LockMode.WRITE))
//            .blockForAtMost(SimpleTimeDuration.of(100, TimeUnit.MILLISECONDS))
            .doNotBlock()
            .build();

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(TEMPORARY_FOLDER)
            .around(TEMPORARY_CONFIG_HOLDER)
            .around(TIMELOCK_SERVER_HOLDER);

    private TrustManager[] get_trust_mgr() {
        TrustManager[] certs = new TrustManager[] {
                new X509TrustManager() {
                    public X509Certificate[ ] getAcceptedIssuers() { return null; }
                    public void checkClientTrusted(X509Certificate[ ] certs, String t) { }
                    public void checkServerTrusted(X509Certificate[ ] certs, String t) { }
                }
        };
        return certs;
    }

    @Test
    public void notExceedingThreadCountLimitsSucceeds() throws Exception {
        List<RemoteLockService> lockService = ImmutableList.of(getLockService(CLIENT_1));

        assertThat(lockAndUnlockAndCountExceptions(lockService, LOCAL_TC_LIMIT + GLOBAL_TC_LIMIT))
                .isEqualTo(0);
    }

    /**
     *  The tests that verify the thread count limit has been exceeded suffer from a race condition where the first
     *  request may be completely served before all the other requests have been assigned to threads. This will acquire
     *  a lock and release the thread. Since the lock is now held, all other threads serving lock requests from the same
     *  client will block for 100 ms (see SLOW_REQUEST), ensuring that those threads will not be released before all
     *  the other requests have been processed.
     *
     *  Due to the uncertainty for the first thread per client, the number of requests that will be met by a 429
     *  response may be off by 1 per client that exceeded the limit.
     */
    @Test
    public void exceedingThreadCountLimitsReturns429() throws Exception {
        List<RemoteLockService> lockService = ImmutableList.of(getLockService(CLIENT_1));


        assertThat(lockAndUnlockAndCountExceptions(lockService, LOCAL_TC_LIMIT + GLOBAL_TC_LIMIT + FORTY_TWO))
                .isBetween(FORTY_TWO, FORTY_TWO);
    }

    @Test
    public void multipleClientsCanShareGlobalThreads() throws Exception {
        List<RemoteLockService> lockServiceList = ImmutableList.of(
                getLockService(CLIENT_1), getLockService(CLIENT_2), getLockService(CLIENT_3));

        assertThat(lockAndUnlockAndCountExceptions(lockServiceList, LOCAL_TC_LIMIT + GLOBAL_TC_LIMIT / 3))
                .isEqualTo(0);
    }

    @Test
    public void globalThreadCountLimitAppliesToAllClients() throws Exception {
        List<RemoteLockService> lockServiceList = ImmutableList.of(
                getLockService(CLIENT_1), getLockService(CLIENT_2), getLockService(CLIENT_3));

        assertThat(lockAndUnlockAndCountExceptions(lockServiceList, LOCAL_TC_LIMIT + GLOBAL_TC_LIMIT))
                .isBetween(2 * GLOBAL_TC_LIMIT , 2 * GLOBAL_TC_LIMIT);
    }

    @Ignore
    @Test
    public void clientsCanUseTheirAllowanceWhenGlobalLimitIsReached() throws Exception {
        RemoteLockService lockService1 = getLockService(CLIENT_1);
        RemoteLockService lockService2 = getLockService(CLIENT_2);
        ExecutorService executorService =
                Executors.newFixedThreadPool(GLOBAL_TC_LIMIT + 2 * LOCAL_TC_LIMIT + FORTY_TWO);
        List<Future<LockRefreshToken>> initialRequests;
        List<Future<LockRefreshToken>> secondClientRequests;
        List<Future<LockRefreshToken>> followUpRequests;

        CountDownLatch countDownLatch = new CountDownLatch(GLOBAL_TC_LIMIT + 2 * LOCAL_TC_LIMIT + FORTY_TWO);

        // This will exhaust all the globally available threads
        initialRequests = requestLocks(lockService1, GLOBAL_TC_LIMIT + LOCAL_TC_LIMIT + FORTY_TWO, executorService, countDownLatch);

        // The following are assigned to CLIENT_2's threads
        secondClientRequests = requestLocks(lockService2, LOCAL_TC_LIMIT, executorService, countDownLatch);

        // CLIENT_1 still cannot acquire a thread
        followUpRequests = requestLocks(lockService1, FORTY_TWO, executorService, countDownLatch);

        Thread.sleep(100);

        assertThat(unlockAndCountExceptions(lockService2, secondClientRequests)).isEqualTo(0);
        secondClientRequests = requestLocks(lockService2, LOCAL_TC_LIMIT, executorService, countDownLatch);
        assertThat(unlockAndCountExceptions(lockService2, secondClientRequests)).isEqualTo(0);

        // Verify that all the global threads were occupied by CLIENT_1's requests
        assertThat(unlockAndCountExceptions(lockService1, initialRequests)).isBetween(FORTY_TWO , FORTY_TWO);
        assertThat(unlockAndCountExceptions(lockService1, followUpRequests)).isBetween(FORTY_TWO , FORTY_TWO);
    }

    @Test
    public void lockServiceShouldAllowUsToTakeOutLocks() throws InterruptedException {
        RemoteLockService lockService = getLockService(CLIENT_1);

        LockRefreshToken token = lockService.lock(LOCK_CLIENT_NAME, LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());

        assertThat(token).isNotNull();

        lockService.unlock(token);
    }

    @Test
    public void lockServiceShouldAllowUsToTakeOutSameLockInDifferentNamespaces() throws InterruptedException {
        RemoteLockService lockService1 = getLockService(CLIENT_1);
        RemoteLockService lockService2 = getLockService(CLIENT_2);

        LockRefreshToken token1 = lockService1.lock(LOCK_CLIENT_NAME, LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());
        LockRefreshToken token2 = lockService2.lock(LOCK_CLIENT_NAME, LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());

        assertThat(token1).isNotNull();
        assertThat(token2).isNotNull();

        lockService1.unlock(token1);
        lockService2.unlock(token2);
    }

    @Test
    public void lockServiceShouldNotAllowUsToRefreshLocksFromDifferentNamespaces() throws InterruptedException {
        RemoteLockService lockService1 = getLockService(CLIENT_1);
        RemoteLockService lockService2 = getLockService(CLIENT_2);

        LockRefreshToken token = lockService1.lock(LOCK_CLIENT_NAME, LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());

        assertThat(token).isNotNull();
        assertThat(lockService1.refreshLockRefreshTokens(ImmutableList.of(token))).isNotEmpty();
        assertThat(lockService2.refreshLockRefreshTokens(ImmutableList.of(token))).isEmpty();

        lockService1.unlock(token);
    }

    @Test
    public void timestampServiceShouldGiveUsIncrementalTimestamps() {
        long timestamp1 = timestampService.getFreshTimestamp();
        long timestamp2 = timestampService.getFreshTimestamp();

        assertThat(timestamp1).isLessThan(timestamp2);
    }

    @Test
    public void timestampServiceShouldRespectDistinctClientsWhenIssuingTimestamps() {
        TimestampService timestampService1 = getTimestampService(CLIENT_1);
        TimestampService timestampService2 = getTimestampService(CLIENT_2);

        long firstServiceFirstTimestamp = timestampService1.getFreshTimestamp();
        long secondServiceFirstTimestamp = timestampService2.getFreshTimestamp();

        long firstServiceSecondTimestamp = timestampService1.getFreshTimestamp();
        long secondServiceSecondTimestamp = timestampService2.getFreshTimestamp();

        assertEquals(firstServiceFirstTimestamp + 1, firstServiceSecondTimestamp);
        assertEquals(secondServiceFirstTimestamp + 1, secondServiceSecondTimestamp);
    }

    @Test
    public void timestampServiceRespectsTimestampManagementService() {
        long currentTimestampIncrementedByOneMillion = timestampService.getFreshTimestamp() + ONE_MILLION;
        timestampManagementService.fastForwardTimestamp(currentTimestampIncrementedByOneMillion);
        assertThat(timestampService.getFreshTimestamp()).isGreaterThan(currentTimestampIncrementedByOneMillion);
    }

    @Test
    public void timestampManagementServiceRespectsTimestampService() {
        long currentTimestampIncrementedByOneMillion = timestampService.getFreshTimestamp() + ONE_MILLION;
        timestampManagementService.fastForwardTimestamp(currentTimestampIncrementedByOneMillion);
        getFortyTwoFreshTimestamps(timestampService);
        timestampManagementService.fastForwardTimestamp(currentTimestampIncrementedByOneMillion + 1);
        assertThat(timestampService.getFreshTimestamp())
                .isGreaterThan(currentTimestampIncrementedByOneMillion + FORTY_TWO);
    }

    private static void getFortyTwoFreshTimestamps(TimestampService timestampService) {
        for (int i = 0; i < FORTY_TWO; i++) {
            timestampService.getFreshTimestamp();
        }
    }

    @Test
    public void fastForwardRespectsDistinctClients() {
        TimestampManagementService anotherClientTimestampManagementService = getTimestampManagementService(CLIENT_2);

        long currentTimestamp = timestampService.getFreshTimestamp();
        anotherClientTimestampManagementService.fastForwardTimestamp(currentTimestamp + ONE_MILLION);
        assertEquals(currentTimestamp + 1, timestampService.getFreshTimestamp());
    }

    @Test
    public void fastForwardToThePastDoesNothing() {
        long currentTimestamp = timestampService.getFreshTimestamp();
        long currentTimestampIncrementedByOneMillion = currentTimestamp + ONE_MILLION;
        long currentTimestampIncrementedByTwoMillion = currentTimestamp + TWO_MILLION;

        timestampManagementService.fastForwardTimestamp(currentTimestampIncrementedByTwoMillion);
        timestampManagementService.fastForwardTimestamp(currentTimestampIncrementedByOneMillion);
        assertThat(timestampService.getFreshTimestamp()).isGreaterThan(currentTimestampIncrementedByTwoMillion);
    }

    @Test
    public void returnsNotFoundOnQueryingNonexistentClient() {
        RemoteLockService lockService = getLockService(NONEXISTENT_CLIENT);
        assertThatThrownBy(lockService::currentTimeMillis)
                .hasMessageContaining(NOT_FOUND_CODE);
    }

    @Test
    public void returnsNotFoundOnQueryingTimestampWithNonexistentClient() {
        TimestampService nonExistentTimestampService = getTimestampService(NONEXISTENT_CLIENT);
        assertThatThrownBy(nonExistentTimestampService::getFreshTimestamp)
                .hasMessageContaining(NOT_FOUND_CODE);
    }

    @Test
    public void throwsOnQueryingTimestampWithWithInvalidClientName() {
        TimestampService invalidTimestampService = getTimestampService(INVALID_CLIENT);
        assertThatThrownBy(invalidTimestampService::getFreshTimestamp)
                .hasMessageContaining("Unexpected char 0x08 at 5 in header value: test");
    }

    @Test
    public void supportsClientNamesMatchingPaxosRoles() throws InterruptedException {
        getTimestampService("learner").getFreshTimestamp();
        getTimestampService("acceptor").getFreshTimestamp();
    }

    @Test
    public void throwsOnFastForwardWithUnspecifiedParameter() throws IOException {
        Response response = makeEmptyPostToUri(getFastForwardUriForClientOne());
        assertThat(response.code()).isEqualTo(HttpStatus.BAD_REQUEST_400);
    }

    @Test
    public void throwsOnFastForwardWithIncorrectParameter() throws IOException {
        String uriWithParam = getFastForwardUriForClientOne() + "?newMinimum=1200";
        Response response = makeEmptyPostToUri(uriWithParam);
        assertThat(response.code()).isEqualTo(HttpStatus.BAD_REQUEST_400);
    }

    private List<Future<LockRefreshToken>> requestLocks(RemoteLockService lockService, int number, ExecutorService executorService, CountDownLatch countDownLatch) {
        List<Future<LockRefreshToken>> futures = Lists.newArrayList();
        for (int i = 0; i < number; i++) {
            int currentTrial = i;
            futures.add(executorService.submit(() -> {
                countDownLatch.countDown();
                countDownLatch.await();
                LockRefreshToken asdg = lockService.lock(lockService.toString() + String.valueOf(currentTrial), SLOW_REQUEST);
//                Thread.sleep(100);
                return asdg;
            }));
        }
        return futures;
    }

    private int unlockAndCountExceptions(RemoteLockService lockService, List<Future<LockRefreshToken>> futures) {
        final int[] exceptionCounter = {0};
        futures.forEach(future -> {
//            System.out.println("TASK IS DONE: " + future.isDone());
//          future.cancel(true);
                try {
                    LockRefreshToken token = future.get();
                    if (token != null) {
                        lockService.unlock(token);
                    }
                } catch (Exception e) {
                    assertThat(e).hasMessageContaining(TOO_MANY_REQUESTS_CODE);
                    exceptionCounter[0]++;
                }

        });
        return exceptionCounter[0];
    }

    private int lockAndUnlockAndCountExceptions(List<RemoteLockService> lockServices, int numRequests) throws Exception {
        List<LockRefreshToken>  bases = lockServices.stream().map(lockService -> {
            try {
                return lockService.lock("LOCK", SLOW_REQUEST);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }).collect(Collectors.toList());

        CountDownLatch countDownLatch = new CountDownLatch(lockServices.size() * numRequests);
        ExecutorService executorService = Executors.newFixedThreadPool(lockServices.size() * numRequests);
        Map<RemoteLockService, List<Future<LockRefreshToken>>> futureMap = new HashMap<>();
        lockServices.forEach(service -> futureMap.put(service, requestLocks(service, numRequests, executorService, countDownLatch)));
//        Thread.sleep(100);
        int num = futureMap.entrySet().stream()
                .mapToInt(entry -> unlockAndCountExceptions(entry.getKey(), entry.getValue())).sum();
        for(int i = 0; i < lockServices.size(); ++i) {
            lockServices.get(i).unlock(bases.get(i));
        }
        return num;
    }

    private static String getFastForwardUriForClientOne() {
        return getRootUriForClient(CLIENT_1) + "/timestamp-management/fast-forward";
    }

    private static Response makeEmptyPostToUri(String uri) throws IOException {
        OkHttpClient client = new OkHttpClient();
        return client.newCall(new Request.Builder()
                .url(uri)
                .post(RequestBody.create(MediaType.parse("application/json"), ""))
                .build()).execute();
    }

    private static RemoteLockService getLockService(String client) {
        return getProxyForService(client, RemoteLockService.class);
    }

    private static TimestampService getTimestampService(String client) {
        return getProxyForService(client, TimestampService.class);
    }

    private static TimestampManagementService getTimestampManagementService(String client) {
        return getProxyForService(client, TimestampManagementService.class);
    }

    private static <T> T getProxyForService(String client, Class<T> clazz) {
        return AtlasDbHttpClients.createProxy(
                NO_SSL,
                getRootUriForClient(client),
                clazz,
                client);
    }

    private static String getRootUriForClient(String client) {
        return String.format("http://localhost:%d/%s", TIMELOCK_SERVER_HOLDER.getTimelockPort(), client);
    }
}
