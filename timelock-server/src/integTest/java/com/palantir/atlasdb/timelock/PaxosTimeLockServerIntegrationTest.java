/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.BadRequestException;

import org.assertj.core.util.Lists;
import org.awaitility.Awaitility;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.FeignOkHttpClients;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.leader.PingableLeader;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

import io.dropwizard.testing.ResourceHelpers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class PaxosTimeLockServerIntegrationTest {
    private static final String CLIENT_1 = "test";
    private static final String CLIENT_2 = "test2";
    private static final String CLIENT_3 = "test3";
    private static final String LEARNER = "learner";
    private static final String ACCEPTOR = "acceptor";
    private static final List<String> CLIENTS = ImmutableList.of(CLIENT_1, CLIENT_2, CLIENT_3, LEARNER, ACCEPTOR);
    private static final String INVALID_CLIENT = "test2\b";

    private static final int MAX_SERVER_THREADS = 100;
    private static final int SELECTOR_THREADS = 8;
    private static final int ACCEPTOR_THREADS = 4;
    private static final int AVAILABLE_THREADS = MAX_SERVER_THREADS - SELECTOR_THREADS - ACCEPTOR_THREADS - 1;
    private static final int SHARED_TC_LIMIT = AVAILABLE_THREADS;

    private static final long ONE_MILLION = 1000000;
    private static final long TWO_MILLION = 2000000;
    private static final int FORTY_TWO = 42;

    private static final String LOCK_CLIENT_NAME = "remoteLock-client-name";
    public static final LockDescriptor LOCK_1 = StringLockDescriptor.of("lock1");
    private static final SortedMap<LockDescriptor, LockMode> LOCK_MAP =
            ImmutableSortedMap.of(LOCK_1, LockMode.WRITE);
    private static final File TIMELOCK_CONFIG_TEMPLATE =
            new File(ResourceHelpers.resourceFilePath("paxosSingleServerWithAsyncLock.yml"));

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
    private static final TemporaryConfigurationHolder TEMPORARY_CONFIG_HOLDER =
            new TemporaryConfigurationHolder(TEMPORARY_FOLDER, TIMELOCK_CONFIG_TEMPLATE);
    private static final TimeLockServerHolder TIMELOCK_SERVER_HOLDER =
            new TimeLockServerHolder(TEMPORARY_CONFIG_HOLDER::getTemporaryConfigFileLocation);

    private static final com.palantir.lock.LockRequest REQUEST_LOCK_WITH_LONG_TIMEOUT = com.palantir.lock.LockRequest
            .builder(ImmutableSortedMap.of(StringLockDescriptor.of("lock"), LockMode.WRITE))
            .timeoutAfter(SimpleTimeDuration.of(20, TimeUnit.SECONDS))
            .blockForAtMost(SimpleTimeDuration.of(4, TimeUnit.SECONDS))
            .build();

    private final TimestampService timestampService = getTimestampService(CLIENT_1);
    private final TimestampManagementService timestampManagementService = getTimestampManagementService(CLIENT_1);

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(TEMPORARY_FOLDER)
            .around(TEMPORARY_CONFIG_HOLDER)
            .around(TIMELOCK_SERVER_HOLDER);

    @BeforeClass
    public static void waitForClusterToStabilize() {
        PingableLeader leader = AtlasDbHttpClients.createProxy(
                Optional.of(TestProxies.SSL_SOCKET_FACTORY),
                "https://localhost:" + TIMELOCK_SERVER_HOLDER.getTimelockPort(),
                PingableLeader.class);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> {
                    try {
                        // Returns true only if this node is ready to serve timestamps and locks on all clients.
                        CLIENTS.forEach(client -> getTimelockService(client).getFreshTimestamp());
                        CLIENTS.forEach(client -> getTimelockService(client).currentTimeMillis());
                        CLIENTS.forEach(client -> getLockService(client).currentTimeMillis());
                        return leader.ping();
                    } catch (Throwable t) {
                        return false;
                    }
                });
    }

    @Test
    public void singleClientCanUseLocalAndSharedThreads() throws Exception {
        List<LockService> lockService = ImmutableList.of(getLockService(CLIENT_1));

        assertThat(lockAndUnlockAndCountExceptions(lockService, SHARED_TC_LIMIT))
                .isEqualTo(0);
    }

    @Test
    public void multipleClientsCanUseSharedThreads() throws Exception {
        List<LockService> lockServiceList = ImmutableList.of(
                getLockService(CLIENT_1), getLockService(CLIENT_2), getLockService(CLIENT_3));

        assertThat(lockAndUnlockAndCountExceptions(lockServiceList, SHARED_TC_LIMIT / 3))
                .isEqualTo(0);
    }

    @Test
    public void throwsOnSingleClientRequestingSameLockTooManyTimes() throws Exception {
        List<LockService> lockServiceList = ImmutableList.of(getLockService(CLIENT_1));

        int exceedingRequests = 10;
        int maxRequestsForOneClient = SHARED_TC_LIMIT;

        assertThat(lockAndUnlockAndCountExceptions(lockServiceList, exceedingRequests + maxRequestsForOneClient))
                .isEqualTo(exceedingRequests);
    }

    @Test
    public void throwsOnTwoClientsRequestingSameLockTooManyTimes() throws Exception {
        List<LockService> lockServiceList = ImmutableList.of(
                getLockService(CLIENT_1), getLockService(CLIENT_2));
        int exceedingRequests = 10;
        int requestsPerClient = (SHARED_TC_LIMIT + exceedingRequests) / 2;

        assertThat(lockAndUnlockAndCountExceptions(lockServiceList, requestsPerClient))
                .isEqualTo(exceedingRequests - 1);
    }

    private int lockAndUnlockAndCountExceptions(List<LockService> lockServices, int numRequestsPerClient)
            throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(lockServices.size() * numRequestsPerClient);

        Map<LockService, LockRefreshToken> tokenMap = new HashMap<>();
        for (LockService service : lockServices) {
            LockRefreshToken token = service.lock(CLIENT_1, REQUEST_LOCK_WITH_LONG_TIMEOUT);
            assertNotNull(token);
            tokenMap.put(service, token);
        }

        List<Future<LockRefreshToken>> futures = Lists.newArrayList();
        for (LockService lockService : lockServices) {
            for (int i = 0; i < numRequestsPerClient; i++) {
                int currentTrial = i;
                futures.add(executorService.submit(() -> lockService.lock(
                                CLIENT_2 + String.valueOf(currentTrial), REQUEST_LOCK_WITH_LONG_TIMEOUT)));
            }
        }

        executorService.shutdown();
        executorService.awaitTermination(2, TimeUnit.SECONDS);

        AtomicInteger exceptionCounter = new AtomicInteger(0);
        futures.forEach(future -> {
            try {
                assertNull(future.get());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                assertThat(cause.getClass().getName()).contains("RetryableException");
                assertRemoteExceptionWithStatus(cause.getCause(), HttpStatus.TOO_MANY_REQUESTS_429);
                exceptionCounter.getAndIncrement();
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        });

        tokenMap.forEach((service, token) -> assertTrue(service.unlock(token)));

        return exceptionCounter.get();
    }

    @Test
    public void lockServiceShouldAllowUsToTakeOutLocks() throws InterruptedException {
        LockService lockService = getLockService(CLIENT_1);

        LockRefreshToken token = lockService.lock(LOCK_CLIENT_NAME, com.palantir.lock.LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());

        assertThat(token).isNotNull();

        lockService.unlock(token);
    }

    @Test
    public void lockServiceShouldAllowUsToTakeOutSameLockInDifferentNamespaces() throws InterruptedException {
        LockService lockService1 = getLockService(CLIENT_1);
        LockService lockService2 = getLockService(CLIENT_2);

        LockRefreshToken token1 = lockService1.lock(LOCK_CLIENT_NAME, com.palantir.lock.LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());
        LockRefreshToken token2 = lockService2.lock(LOCK_CLIENT_NAME, com.palantir.lock.LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());

        assertThat(token1).isNotNull();
        assertThat(token2).isNotNull();

        lockService1.unlock(token1);
        lockService2.unlock(token2);
    }

    @Test
    public void lockServiceShouldNotAllowUsToRefreshLocksFromDifferentNamespaces() throws InterruptedException {
        LockService lockService1 = getLockService(CLIENT_1);
        LockService lockService2 = getLockService(CLIENT_2);

        LockRefreshToken token = lockService1.lock(LOCK_CLIENT_NAME, com.palantir.lock.LockRequest.builder(LOCK_MAP)
                .doNotBlock()
                .build());

        assertThat(token).isNotNull();
        assertThat(lockService1.refreshLockRefreshTokens(ImmutableList.of(token))).isNotEmpty();
        assertThat(lockService2.refreshLockRefreshTokens(ImmutableList.of(token))).isEmpty();

        lockService1.unlock(token);
    }

    @Test
    public void asyncLockServiceShouldAllowUsToTakeOutLocks() throws InterruptedException {
        TimelockService timelockService = getTimelockService(CLIENT_1);

        LockToken token = timelockService.lock(newLockV2Request(LOCK_1)).getToken();

        assertThat(timelockService.unlock(ImmutableSet.of(token))).contains(token);
    }

    @Test
    public void asyncLockServiceShouldAllowUsToTakeOutSameLockInDifferentNamespaces() throws InterruptedException {
        TimelockService lockService1 = getTimelockService(CLIENT_1);
        TimelockService lockService2 = getTimelockService(CLIENT_2);

        LockToken token1 = lockService1.lock(newLockV2Request(LOCK_1)).getToken();
        LockToken token2 = lockService2.lock(newLockV2Request(LOCK_1)).getToken();

        lockService1.unlock(ImmutableSet.of(token1));
        lockService2.unlock(ImmutableSet.of(token2));
    }

    @Test
    public void asyncLockServiceShouldNotAllowUsToRefreshLocksFromDifferentNamespaces() throws InterruptedException {
        TimelockService lockService1 = getTimelockService(CLIENT_1);
        TimelockService lockService2 = getTimelockService(CLIENT_2);

        LockToken token = lockService1.lock(newLockV2Request(LOCK_1)).getToken();

        assertThat(lockService1.refreshLockLeases(ImmutableSet.of(token))).isNotEmpty();
        assertThat(lockService2.refreshLockLeases(ImmutableSet.of(token))).isEmpty();

        lockService1.unlock(ImmutableSet.of(token));
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

    @Test
    public void lockServiceShouldDisallowGettingMinLockedInVersionId() {
        LockService lockService = getLockService(CLIENT_1);
        assertThatThrownBy(() -> lockService.getMinLockedInVersionId(CLIENT_1))
                .isInstanceOf(AtlasDbRemoteException.class)
                .satisfies(remoteException -> {
                    AtlasDbRemoteException atlasDbRemoteException = (AtlasDbRemoteException) remoteException;
                    assertThat(atlasDbRemoteException.getErrorName())
                            .isEqualTo(BadRequestException.class.getCanonicalName());
                    assertThat(atlasDbRemoteException.getStatus())
                            .isEqualTo(HttpStatus.BAD_REQUEST_400);
                });
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
        assertThat(timestampService.getFreshTimestamp())
                .isBetween(currentTimestamp + 1, currentTimestamp + ONE_MILLION - 1);
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
    public void throwsOnQueryingTimestampWithWithInvalidClientName() {
        TimestampService invalidTimestampService = getTimestampService(INVALID_CLIENT);
        assertThatThrownBy(invalidTimestampService::getFreshTimestamp)
                .hasMessageContaining("Unexpected char 0x08");
    }

    @Test
    public void supportsClientNamesMatchingPaxosRoles() throws InterruptedException {
        getTimestampService(LEARNER).getFreshTimestamp();
        getTimestampService(ACCEPTOR).getFreshTimestamp();
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

    @Test
    public void leadershipEventsSmokeTest() throws IOException {
        MetricsOutput metrics = getMetricsOutput();

        metrics.assertContainsMeter("leadership.gained");
        metrics.assertContainsMeter("leadership.lost");
        metrics.assertContainsMeter("leadership.proposed");
        metrics.assertContainsMeter("leadership.no-quorum");
        metrics.assertContainsMeter("leadership.proposed.failure");

        assertThat(metrics.getMeter("leadership.gained").get("count").intValue()).isEqualTo(1);
        assertThat(metrics.getMeter("leadership.proposed").get("count").intValue()).isEqualTo(1);
    }

    @Test
    // TODO(nziebart): test remote service instrumentation - we need a multi-node server config for this
    public void instrumentationSmokeTest() throws IOException {
        getTimestampService(CLIENT_1).getFreshTimestamp();
        getLockService(CLIENT_1).currentTimeMillis();
        getTimelockService(CLIENT_1).lock(newLockV2Request(LOCK_1)).getToken();

        MetricsOutput metrics = getMetricsOutput();

        // time / lock services
        metrics.assertContainsTimer(
                "com.palantir.atlasdb.timelock.AsyncTimelockService.getFreshTimestamp");
        metrics.assertContainsTimer("com.palantir.lock.LockService.currentTimeMillis");

        // local leader election classes
        metrics.assertContainsTimer("com.palantir.paxos.PaxosLearner.learn");
        metrics.assertContainsTimer("com.palantir.paxos.PaxosAcceptor.accept");
        metrics.assertContainsTimer("com.palantir.paxos.PaxosProposer.propose");
        metrics.assertContainsTimer("com.palantir.leader.PingableLeader.ping");
        metrics.assertContainsTimer("com.palantir.leader.LeaderElectionService.blockOnBecomingLeader");

        // local timestamp bound classes
        metrics.assertContainsTimer("com.palantir.timestamp.TimestampBoundStore.getUpperLimit");
        metrics.assertContainsTimer("com.palantir.paxos.PaxosLearner.getGreatestLearnedValue");
        metrics.assertContainsTimer("com.palantir.paxos.PaxosAcceptor.accept");
        metrics.assertContainsTimer("com.palantir.paxos.PaxosProposer.propose");

        // async lock
        // TODO(nziebart): why does this flake on circle?
        //assertContainsTimer(metrics, "lock.blocking-time");
    }

    private static String getFastForwardUriForClientOne() {
        return getRootUriForClient(CLIENT_1) + "/timestamp-management/fast-forward";
    }

    private static Response makeEmptyPostToUri(String uri) throws IOException {
        OkHttpClient client = new OkHttpClient.Builder()
                .sslSocketFactory(TestProxies.SSL_SOCKET_FACTORY)
                .connectionSpecs(FeignOkHttpClients.CONNECTION_SPEC_WITH_CYPHER_SUITES)
                .build();
        return client.newCall(new Request.Builder()
                .url(uri)
                .post(RequestBody.create(MediaType.parse("application/json"), ""))
                .build()).execute();
    }

    private static MetricsOutput getMetricsOutput() throws IOException {
        return new MetricsOutput(new ObjectMapper().readTree(
                new URL("http", "localhost", TIMELOCK_SERVER_HOLDER.getAdminPort(), "/metrics")));
    }

    private static TimelockService getTimelockService(String client) {
        return getProxyForService(client, TimelockService.class);
    }

    private static LockService getLockService(String client) {
        return getProxyForService(client, LockService.class);
    }

    private static TimestampService getTimestampService(String client) {
        return getProxyForService(client, TimestampService.class);
    }

    private static TimestampManagementService getTimestampManagementService(String client) {
        return getProxyForService(client, TimestampManagementService.class);
    }

    private static <T> T getProxyForService(String client, Class<T> clazz) {
        return AtlasDbHttpClients.createProxy(
                Optional.of(TestProxies.SSL_SOCKET_FACTORY),
                getRootUriForClient(client),
                clazz,
                client);
    }

    private static String getRootUriForClient(String client) {
        return String.format("https://localhost:%d/%s", TIMELOCK_SERVER_HOLDER.getTimelockPort(), client);
    }

    private static void assertRemoteExceptionWithStatus(Throwable throwable, int expectedStatus) {
        assertThat(throwable).isInstanceOf(AtlasDbRemoteException.class);

        AtlasDbRemoteException remoteException = (AtlasDbRemoteException) throwable;
        assertThat(remoteException.getStatus()).isEqualTo(expectedStatus);
    }

    private LockRequest newLockV2Request(LockDescriptor lock) {
        return LockRequest.of(ImmutableSet.of(lock), 10_000L);
    }
}
