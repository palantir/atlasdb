/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.paxos;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public final class PaxosQuorumChecker {

    public static final int DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS = 5;
    private static final Logger log = LoggerFactory.getLogger(PaxosQuorumChecker.class);

    private PaxosQuorumChecker() {
        // Private constructor. Disallow instantiation.
    }

    /**
     * Collects a list of responses from a quorum of remote services.
     *
     * @param remotes a list endpoints to make the remote call on
     * @param request the request to make on each of the remote endpoints
     * @param quorumSize number of acknowledge requests required to reach quorum
     * @param executor runs the requests
     * @return a list responses
     */
    public static <SERVICE, RESPONSE extends PaxosResponse> List<RESPONSE> collectQuorumResponses(ImmutableList<SERVICE> remotes,
                                                                                                  final Function<SERVICE, RESPONSE> request,
                                                                                                  int quorumSize,
                                                                                                  Executor executor,
                                                                                                  long remoteRequestTimeoutInSec) {
        CompletionService<RESPONSE> responseCompletionService = new ExecutorCompletionService<RESPONSE>(executor);

        // kick off all the requests
        List<Future<RESPONSE>> allFutures = Lists.newArrayList();
        for (final SERVICE remote : remotes) {
            allFutures.add(responseCompletionService.submit(new Callable<RESPONSE>() {
                @Override
                public RESPONSE call() throws Exception {
                    return request.apply(remote);
                }
            }));
        }

        boolean interrupted = false;
        List<RESPONSE> receivedResponses = new ArrayList<RESPONSE>();
        int acksRecieved = 0;
        int nacksRecieved = 0;

        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(remoteRequestTimeoutInSec);
            // handle responses
            while (acksRecieved < quorumSize) {
                try {
                    // check if quorum is impossible (nack quorum failure)
                    if (nacksRecieved > remotes.size() - quorumSize) {
                        break;
                    }

                    Future<RESPONSE> responseFuture = responseCompletionService.poll(
                            deadline - System.nanoTime(),
                            TimeUnit.NANOSECONDS);
                    // check if out of responses (no quorum failure)
                    if (responseFuture == null) {
                        return receivedResponses;
                    }

                    // reject invalid or repeat promises
                    RESPONSE response = responseFuture.get();
                    if (response.isSuccessful()) {
                        acksRecieved++;
                    } else {
                        nacksRecieved++;
                    }

                    // record response
                    receivedResponses.add(response);
                } catch (InterruptedException e) {
                    log.info("paxos request interrupted", e);
                    interrupted = true;
                    break;
                } catch (ExecutionException e) {
                    nacksRecieved++;
                    log.info("error requesting paxos message", e.getCause());
                }
            }

            // poll for extra completed futures
            Future<RESPONSE> future;
            while ((future = responseCompletionService.poll()) != null) {
                try {
                    receivedResponses.add(future.get());
                } catch (InterruptedException e) {
                    log.info("paxos request interrupted", e);
                    interrupted = true;
                    break;
                } catch (ExecutionException e) {
                    log.info("error requesting paxos message", e.getCause());
                }
            }

        } finally {
            // cancel pending futures (during failures)
            for (Future<RESPONSE> future : allFutures) {
                future.cancel(false);
            }

            // reset interrupted flag
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        return receivedResponses;
    }

    public static boolean hasQuorum(List<? extends PaxosResponse> responses, int quorumSize) {
        return Collections2.filter(responses, PaxosResponses.isSuccessfulPredicate()).size() >= quorumSize;
    }
}
