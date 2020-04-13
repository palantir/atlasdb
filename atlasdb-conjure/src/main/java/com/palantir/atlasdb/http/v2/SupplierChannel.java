/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.atlasdb.http.v2;

import java.util.function.Supplier;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.dialogue.Channel;
import com.palantir.dialogue.Endpoint;
import com.palantir.dialogue.Request;
import com.palantir.dialogue.Response;

final class SupplierChannel implements Channel {
    private final Supplier<Channel> channelSupplier;

    SupplierChannel(Supplier<Channel> channelSupplier) {
        this.channelSupplier = channelSupplier;
    }

    @Override
    public ListenableFuture<Response> execute(Endpoint endpoint, Request request) {
        Channel delegate = channelSupplier.get();
        return delegate.execute(endpoint, request);
    }
}
