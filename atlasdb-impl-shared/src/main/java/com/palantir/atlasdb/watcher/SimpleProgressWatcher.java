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
package com.palantir.atlasdb.watcher;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

final class SimpleProgressWatcher implements ProgressWatcher {
    private final SimpleWatch root = new SimpleWatch(
            ImmutableList.<String> of(),
            ImmutableList.<String> of());

    private final LoadingCache<List<String>, AtomicLong> durationMillisByPathNameParts =
            CacheBuilder.newBuilder().build(new CacheLoader<List<String>, AtomicLong>() {
                @Override
                public AtomicLong load(List<String> pathNameParts) throws Exception {
                    return new AtomicLong();
                }
            });

    private final JointReporter jointReporter = new JointReporter();

    private final class SimpleWatch implements Watch {
        private final List<String> pathNameParts, instanceNameParts;
        private final long startMillis = System.currentTimeMillis();
        private final AtomicInteger steps = new AtomicInteger();
        private final AtomicLong subInstanceNameGenerator = new AtomicLong();
        private volatile int maxSteps = 0;
        private volatile String itemsName = "items";

        public SimpleWatch(List<String> pathNameParts, List<String> instanceNameParts) {
            this.pathNameParts = pathNameParts;
            this.instanceNameParts = instanceNameParts;
        }

        @Override
        public Watch watch(String subPathNamePart,
                           String subInstanceNamePartFormat,
                           Object... subInstanceNamePartArgs) {
            String subInstanceNamePart =
                    String.format(subInstanceNamePartFormat, subInstanceNamePartArgs);
            List<String> subPathNameParts =
                    ImmutableList.<String> builder().addAll(pathNameParts).add(subPathNamePart).build();
            List<String> subInstanceNameParts =
                    ImmutableList.<String> builder().addAll(instanceNameParts).add(
                            subInstanceNamePart).build();
            return new SimpleWatch(subPathNameParts, subInstanceNameParts).start();
        }

        @Override
        public Watch watch(String subPathNamePart) {
            return watch(subPathNamePart, "%s", subInstanceNameGenerator.getAndIncrement());
        }

        @Override
        public void done() {
            long stopMillis = System.currentTimeMillis();
            jointReporter.stop(getName(), stopMillis - startMillis);
            durationMillisByPathNameParts.getUnchecked(pathNameParts).addAndGet(
                    stopMillis - startMillis);
        }

        @Override
        public Watch step(int dSteps) {
            steps.addAndGet(dSteps);
            jointReporter.step(getName(), steps.get(), maxSteps, itemsName);
            return this;
        }

        @Override
        public Watch step() {
            step(1);
            return this;
        }

        @Override
        public Watch expect(int maxSteps1, String itemsName1) {
            this.maxSteps = maxSteps1;
            this.itemsName = itemsName1;
            return this;
        }

        @Override
        public Watch expect(String itemsName1) {
            return expect(0, itemsName1);
        }

        @Override
        public Watch say(String key, Object val) {
            jointReporter.say(getName(), "%s = %s", key, val);
            return this;
        }

        private SimpleWatch start() {
            jointReporter.start(getName());
            return this;
        }

        private String getName() {
            int nParts = pathNameParts.size();
            List<String> combinedParts = Lists.newArrayListWithCapacity(nParts);
            for (int i = 0; i < nParts; i++) {
                combinedParts.add(String.format(
                        "%s[%s]",
                        pathNameParts.get(i),
                        instanceNameParts.get(i)));
            }
            return Joiner.on('.').join(combinedParts);
        }
    }

    @Override
    public void totals() {
        for (List<String> pathNameParts : durationMillisByPathNameParts.asMap().keySet()) {
            total(pathNameParts);
        }
    }

    @Override
    public void addReporter(Reporter reporter) {
        jointReporter.add(reporter);
    }

    @Override
    public void total(String... pathNamePartsArray) {
        total(Arrays.asList(pathNamePartsArray));
    }

    private void total(List<String> pathNameParts) {
        String pathName = Joiner.on('.').join(pathNameParts);
        long totalDuration = durationMillisByPathNameParts.getUnchecked(pathNameParts).get();
        jointReporter.total(pathName, totalDuration);
    }

    @Override
    public Watch watch(String subPathNameComponent,
                       String subInstanceNameComponentFormat,
                       Object... subInstanceNameComponentArgs) {
        return root.watch(
                subPathNameComponent,
                subInstanceNameComponentFormat,
                subInstanceNameComponentArgs);
    }

    @Override
    public Watch watch(String subPathNameComponent) {
        return root.watch(subPathNameComponent);
    }
}
