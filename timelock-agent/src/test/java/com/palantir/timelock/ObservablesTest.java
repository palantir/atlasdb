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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.timelock.Observables;

import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;

public class ObservablesTest {
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

    @Test
    public void blockingMostRecentCanWaitIfNoValueIsAvailableYet() {
        Observable<Integer> observable = Observable.fromFuture(
                EXECUTOR_SERVICE.submit(() -> {
                    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                    return 1;
                }));
        assertThat(Observables.blockingMostRecent(observable).get()).isEqualTo(1);
    }

    @Test
    public void blockingMostRecentCanSkipValuesIfThereIsSomethingMoreRecent() {
        Observable<Integer> observable = Observable.just(1, 2, 3, 4, 5);
        assertThat(Observables.blockingMostRecent(observable).get()).isEqualTo(5);
    }

    @Test
    public void blockingMostRecentCanReadFreshValues() {
        Observable<Long> counter = Observable.intervalRange(
                1, 3, 0, 500, TimeUnit.MILLISECONDS, Schedulers.computation());
        Supplier<Long> supplier = Observables.blockingMostRecent(counter);

        assertThat(supplier.get()).isEqualTo(1L);
        Uninterruptibles.sleepUninterruptibly(600, TimeUnit.MILLISECONDS);
        assertThat(supplier.get()).isEqualTo(2L);
        Uninterruptibles.sleepUninterruptibly(600, TimeUnit.MILLISECONDS);
        assertThat(supplier.get()).isEqualTo(3L);
    }
}