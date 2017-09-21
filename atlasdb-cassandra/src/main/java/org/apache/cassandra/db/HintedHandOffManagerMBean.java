/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package org.apache.cassandra.db;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public interface HintedHandOffManagerMBean {
    /**
     * Nuke all hints from this node to `ep`.
     *
     * @param host String rep. of endpoint address to delete hints for, either ip address ("127.0.0.1") or hostname
     */
    void deleteHintsForEndpoint(String host);

    /**
     *  Truncate all the hints.
     */
    void truncateAllHints() throws ExecutionException, InterruptedException;

    /**
     * List all the endpoints that this node has hints for.
     *
     * @return set of endpoints; as Strings
     */
    List<String> listEndpointsPendingHints();

    /**
     * Force hint delivery to an endpoint.
     */
    void scheduleHintDelivery(String host) throws UnknownHostException;

    /**
     * Pause hints delivery process.
     */
    void pauseHintsDelivery(boolean pauseHintsDelivery);
}
