/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.background;

import com.google.common.collect.Iterators;
import com.palantir.atlasdb.buggify.api.BuggifyFactory;
import com.palantir.atlasdb.workload.resource.CassandraResource;
import java.util.Iterator;
import java.util.List;

public class BackgroundCassandraJob implements Runnable {

    private final CassandraResource cassandraResource;
    private final Iterator<String> cassandraHosts;

    private final BuggifyFactory buggify;

    public BackgroundCassandraJob(
            List<String> cassandraHosts, CassandraResource cassandraResource, BuggifyFactory buggify) {
        this.cassandraResource = cassandraResource;
        this.cassandraHosts = Iterators.cycle(cassandraHosts);
        this.buggify = buggify;
    }

    @Override
    public void run() {
        buggify.maybe(0.20).run(() -> cassandraResource.flush(cassandraHosts.next()));
        buggify.maybe(0.20).run(() -> cassandraResource.compact(cassandraHosts.next()));
    }
}
