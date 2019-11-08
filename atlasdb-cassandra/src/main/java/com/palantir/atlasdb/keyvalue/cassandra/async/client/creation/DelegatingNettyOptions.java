/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async.client.creation;

import com.datastax.oss.driver.internal.core.context.NettyOptions;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.Timer;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;

interface DelegatingNettyOptions extends NettyOptions {
    NettyOptions delegate();

    @Override
    default Class<? extends Channel> channelClass() {
        return delegate().channelClass();
    }

    @Override
    default EventExecutorGroup adminEventExecutorGroup() {
        return delegate().adminEventExecutorGroup();
    }

    @Override
    default EventLoopGroup ioEventLoopGroup() {
        return delegate().ioEventLoopGroup();
    }

    @Override
    default void afterChannelInitialized(Channel arg0) {
        delegate().afterChannelInitialized(arg0);
    }

    @Override
    default Future<Void> onClose() {
        return delegate().onClose();
    }

    @Override
    default Timer getTimer() {
        return delegate().getTimer();
    }

    @Override
    default ByteBufAllocator allocator() {
        return delegate().allocator();
    }

    @Override
    default void afterBootstrapInitialized(Bootstrap arg0) {
        delegate().afterBootstrapInitialized(arg0);
    }

}
