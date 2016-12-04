/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.atomix;

import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientProperties;

import com.google.common.collect.Lists;

public class AggressiveTimelockPoller {
    private static final String TIMELOCK_HOST_1 = "http://localhost:8080";
    private static final String TIMELOCK_HOST_2 = "http://localhost:8082";
    private static final String TIMELOCK_HOST_3 = "http://localhost:8084";
    private static final String TIMESTAMP_PATH = "/%s/timestamp/fresh-timestamp";

    public static void main(String[] args) {
        String timestampPath = String.format(TIMESTAMP_PATH, "test");
        String path1 = TIMELOCK_HOST_1 + timestampPath;
        String path2 = TIMELOCK_HOST_2 + timestampPath;
        String path3 = TIMELOCK_HOST_3 + timestampPath;
        String[] paths = { path1, path2, path3 };
        int id = 0;

        List<Long> timestamps = Lists.newArrayList();
        Client client = ClientBuilder.newClient().property(ClientProperties.READ_TIMEOUT, 3000).property(ClientProperties.CONNECT_TIMEOUT, 3000);
        WebTarget target = client.target(paths[id]);
        while (timestamps.size() < 1e6) {
            try {
                Response r = target.request().post(null);
                if (r.getStatusInfo().getStatusCode() == 503) {
                    id = (id + 1) % 3;
                    target = client.target(paths[id]);
                    continue;
                }
                Long ts = r.readEntity(Long.class);
                timestamps.add(ts);
            } catch (Exception e) {
                id = (id + 1) % 3;
                target = client.target(paths[id]);
            }
            if (timestamps.size() % 1e5 == 0) {
                System.out.println("Got " + timestamps.size() + " timestamps.");
            }
        }

        // Check
        for (int i = 1; i < timestamps.size(); i++) {
            if (timestamps.get(i) < timestamps.get(i-1)) {
                System.err.println("Ordering violation! Got " + timestamps.get(i) + " after " + timestamps.get(i-1));
            }
        }
    }
}
