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

package com.palantir.atlasdb.timelock.config;

import static com.palantir.atlasdb.timelock.lock.TargetedSweepLockDecorator.LOCK_ACQUIRES_PER_SECOND;

import java.util.Map;
import java.util.Set;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonDeserialize(as = ImmutableTargetedSweepLockControlConfig.class)
@JsonSerialize(as = ImmutableTargetedSweepLockControlConfig.class)
public abstract class TargetedSweepLockControlConfig {

    public enum Mode {
        ENABLED_BY_DEFAULT,
        DISABLED_BY_DEFAULT
    }

    @Value.Default
    Mode mode() {
        return Mode.DISABLED_BY_DEFAULT;
    }

    @JsonProperty("excluded-clients")
    abstract Set<String> excludedClients();

    @JsonProperty("concurrency")
    abstract Map<String, ShardAndThreadConfig> config();

    public RateLimitConfig rateLimitConfig(String client) {
        return ImmutableRateLimitConfig.builder()
                .enabled(excludedClients().contains(client) ^ mode() == Mode.ENABLED_BY_DEFAULT)
                .config(config().getOrDefault(client, ShardAndThreadConfig.defaultConfig()))
                .build();
    }

    public static TargetedSweepLockControlConfig defaultConfig() {
        return ImmutableTargetedSweepLockControlConfig.builder().build();
    }

    @Value.Immutable
    public interface RateLimitConfig {
        @Value.Parameter
        boolean enabled();

        @Value.Default
        @Value.Parameter
        default ShardAndThreadConfig config() {
            return ShardAndThreadConfig.defaultConfig();
        }
    }

    @JsonDeserialize(as = ImmutableShardAndThreadConfig.class)
    @JsonSerialize(as = ImmutableShardAndThreadConfig.class)
    @Value.Immutable
    public interface ShardAndThreadConfig {

        @Value.Default
        default int nodes() {
            return 1;
        }

        @JsonProperty("conservative-threads")
        @Value.Default
        default int conservativeThreads() {
            return 1;
        }

        @JsonProperty("thorough-threads")
        @Value.Default
        default int thoroughThreads() {
            return 1;
        }

        @Value.Default
        default int shards() {
            return 8;
        }

        default double conservativePermitsPerSecond() {
            return permitsPerSecond(nodes(), conservativeThreads(), shards());
        }

        default double thoroughPermitsPerSecond() {
            return permitsPerSecond(nodes(), thoroughThreads(), shards());
        }

        static double permitsPerSecond(int nodes, int threads, int shards) {
            int concurrency = Math.min(threads * nodes, shards);
            return ((double) LOCK_ACQUIRES_PER_SECOND) * concurrency / shards;
        }

        static ShardAndThreadConfig defaultConfig() {
            return ImmutableShardAndThreadConfig.builder().build();
        }
    }

}
