/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client.timestampleases;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.lock.client.CloseableSupplier;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import com.palantir.lock.client.LazyInstanceCloseableSupplier;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public final class MinLeasedTimestampGetterImpl implements MinLeasedTimestampGetter {
    private final Namespace namespace;
    private final CloseableSupplier<MultiClientMinLeasedTimestampGetter> delegate;

    private MinLeasedTimestampGetterImpl(
            Namespace namespace, CloseableSupplier<MultiClientMinLeasedTimestampGetter> delegate) {
        this.namespace = namespace;
        this.delegate = delegate;
    }

    private static MinLeasedTimestampGetter create(
            Namespace namespace, CloseableSupplier<MultiClientMinLeasedTimestampGetter> delegate) {
        return new MinLeasedTimestampGetterImpl(namespace, delegate);
    }

    public static MinLeasedTimestampGetter create(
            String namespace, CloseableSupplier<MultiClientMinLeasedTimestampGetter> delegate) {
        return create(Namespace.of(namespace), delegate);
    }

    public static MinLeasedTimestampGetter create(
            String namespace, Supplier<InternalMultiClientConjureTimelockService> conjureService) {
        return create(
                namespace,
                LazyInstanceCloseableSupplier.of(
                        Suppliers.compose(MultiClientMinLeasedTimestampGetter::create, conjureService::get)));
    }

    @Override
    public Map<TimestampLeaseName, Long> getMinLeasedTimestamps(Set<TimestampLeaseName> timestampNames) {
        return delegate.getDelegate().getMinLeasedTimestamps(namespace, timestampNames);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
