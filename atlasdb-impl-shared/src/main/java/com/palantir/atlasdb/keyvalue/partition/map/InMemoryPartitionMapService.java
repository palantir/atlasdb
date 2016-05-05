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
package com.palantir.atlasdb.keyvalue.partition.map;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;

public class InMemoryPartitionMapService implements PartitionMapService {

    @GuardedBy("this")
    private DynamicPartitionMap partitionMap;
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    private InMemoryPartitionMapService(DynamicPartitionMap partitionMap) {
        this.partitionMap = partitionMap;
    }

    public static InMemoryPartitionMapService create(DynamicPartitionMap partitionMap) {
        return new InMemoryPartitionMapService(Preconditions.checkNotNull(partitionMap));
    }

    /**
     * You must store a map using {@link #updateMap(DynamicPartitionMap)} before
     * calling {@link #getMap()} or {@link #getMapVersion()}.
     *
     */
    public static InMemoryPartitionMapService createEmpty() {
        return new InMemoryPartitionMapService(null);
    }

    @Override
    public synchronized DynamicPartitionMap getMap() {
        lock.readLock().lock();
        try {
            return Preconditions.checkNotNull(partitionMap);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public synchronized long getMapVersion() {
        lock.readLock().lock();
        try {
            return partitionMap.getVersion();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public synchronized void updateMap(DynamicPartitionMap partitionMap) {
        lock.writeLock().lock();
        try {
            this.partitionMap = Preconditions.checkNotNull(partitionMap);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public synchronized long updateMapIfNewer(DynamicPartitionMap partitionMap) {
        lock.writeLock().lock();

        try {
            final long originalVersion = this.partitionMap == null ? -1L : this.partitionMap.getVersion();

            if (partitionMap.getVersion() > originalVersion) {
                this.partitionMap = Preconditions.checkNotNull(partitionMap);
            }
            return originalVersion;
        } finally {
            lock.writeLock().unlock();
        }
    }

}
