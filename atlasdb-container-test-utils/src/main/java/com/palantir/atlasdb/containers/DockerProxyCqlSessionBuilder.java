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

package com.palantir.atlasdb.containers;

import java.net.SocketAddress;

import javax.annotation.Nonnull;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.util.Timer;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;

/**
 * Implementation of a {@link CqlSessionBuilder} used for testing. This builder sets up the
 * {@link com.datastax.oss.driver.internal.core.context.InternalDriverContext} such that {@link NettyOptions} forward
 * all communication to the specified proxy passed during builder creation.
 */
final class DockerProxyCqlSessionBuilder extends CqlSessionBuilder {
    private final SocketAddress proxyAddress;

    DockerProxyCqlSessionBuilder(SocketAddress proxyAddress) {
        this.proxyAddress = proxyAddress;
    }

    @Override
    protected DriverContext buildContext(
            DriverConfigLoader configLoader,
            ProgrammaticArguments programmaticArguments) {
        return new WrappingDriverContext(configLoader, programmaticArguments, proxyAddress);
    }

    /**
     * This class follows the intended usage explained by {@link DefaultDriverContext}.
     */
    private static final class WrappingDriverContext extends DefaultDriverContext {
        private final SocksProxyNettyOptions nettyOptions;

        WrappingDriverContext(
                DriverConfigLoader configLoader,
                ProgrammaticArguments programmaticArguments,
                SocketAddress proxyAddress) {
            super(configLoader, programmaticArguments);
            this.nettyOptions = new SocksProxyNettyOptions(super.getNettyOptions(), proxyAddress);
        }

        @Nonnull
        @Override
        public NettyOptions getNettyOptions() {
            return nettyOptions;
        }
    }

    /**
     * Custom implementation of {@link NettyOptions}. This implementation sets a socks proxy to be used when
     * communicating with Cassandra in a dockerized environment.
     */
    private static class SocksProxyNettyOptions implements NettyOptions {
        private final SocketAddress proxyAddress;
        private final NettyOptions delegate;

        SocksProxyNettyOptions(NettyOptions nettyOptions, SocketAddress proxyAddress) {
            this.delegate = nettyOptions;
            this.proxyAddress = proxyAddress;
        }

        /**
         * This method sets up the proxy on an initialized channel.
         *
         * @param channel which was initialized
         */
        @Override
        public void afterChannelInitialized(Channel channel) {
            delegate.afterChannelInitialized(channel);
            channel.pipeline().addFirst(new Socks5ProxyHandler(proxyAddress));
        }

        @Override
        public Class<? extends Channel> channelClass() {
            return delegate.channelClass();
        }

        @Override
        public EventExecutorGroup adminEventExecutorGroup() {
            return delegate.adminEventExecutorGroup();
        }

        @Override
        public EventLoopGroup ioEventLoopGroup() {
            return delegate.ioEventLoopGroup();
        }

        @Override
        public Future<Void> onClose() {
            return delegate.onClose();
        }

        @Override
        public Timer getTimer() {
            return delegate.getTimer();
        }

        @Override
        public ByteBufAllocator allocator() {
            return delegate.allocator();
        }
        @Override
        public void afterBootstrapInitialized(Bootstrap bootstrap) {
            delegate.afterBootstrapInitialized(bootstrap);
        }
    }
}
