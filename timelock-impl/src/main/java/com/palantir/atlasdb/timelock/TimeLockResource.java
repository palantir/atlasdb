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

import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.lock.LockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

@Path("/{client: [a-zA-Z0-9_-]+}")
public class TimeLockResource {
    private final Logger log = LoggerFactory.getLogger(TimeLockResource.class);

    private final Function<String, TimeLockServices>  clientServicesFactory;
    private final ConcurrentMap<String, TimeLockServices> servicesByClient = Maps.newConcurrentMap();
    private final Supplier<Integer> maxNumberOfClients;

    public TimeLockResource(
            Function<String, TimeLockServices> clientServicesFactory,
            Supplier<Integer> maxNumberOfClients) {
        this.clientServicesFactory = clientServicesFactory;
        this.maxNumberOfClients = maxNumberOfClients;
    }

    @Path("/lock")
    public LockService getLockService(@PathParam("client") String client) {
        return getOrCreateServices(client).getLockService();
    }

    @Path("/timestamp")
    public TimestampService getTimeService(@PathParam("client") String client) {
        return getOrCreateServices(client).getTimestampService();
    }

    @Path("/timelock")
    public Object getTimelockService(@PathParam("client") String client) {
        return getOrCreateServices(client).getTimelockService().getPresentService();
    }

    @Path("/timestamp-management")
    public TimestampManagementService getTimestampManagementService(@PathParam("client") String client) {
        return getOrCreateServices(client).getTimestampManagementService();
    }

    @VisibleForTesting
    TimeLockServices getOrCreateServices(String client) {
        return servicesByClient.computeIfAbsent(client, this::createNewClient);
    }

    private TimeLockServices createNewClient(String client) {
        Preconditions.checkArgument(!client.equals(PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE),
                "The client name '%s' is reserved for the leader election service, and may not be "
                        + "used.",
                PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE);

        if (servicesByClient.size() >= maxNumberOfClients.get()) {
            log.error(
                    "Unable to create timelock services for client {}, as it would exceed the maximum number of "
                            + "allowed clients ({}). If this is intentional, the maximum number of clients can be "
                            + "increased via the maximum-number-of-clients runtime config property.",
                    SafeArg.of("client", client),
                    SafeArg.of("maxNumberOfClients", maxNumberOfClients));
            throw new IllegalStateException("Maximum number of clients exceeded");
        }

        return clientServicesFactory.apply(client);
    }
}
