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
package com.palantir.atlasdb.transaction.service;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.palantir.common.base.Throwables;

public class ZooKeeperMetadataStorageService implements MetadataStorageService {
    private static final String ZOOKEEPER_PATH = "/";
    private static final int SESSION_TIMEOUT = 10000;
    private static final int NUMBER_OR_TRIES = 2;
    private ZooKeeper zooKeeper;
    private String servers;


    private ZooKeeperMetadataStorageService(String servers) {
        this.servers = servers;
    }

    private void init() {
        try {
            zooKeeper = new ZooKeeper(servers, SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // do nothing
                }
            });
        } catch (IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    public static ZooKeeperMetadataStorageService create(String servers) {
        ZooKeeperMetadataStorageService storageService = new ZooKeeperMetadataStorageService(servers);
        storageService.init();
        return storageService;
    }

    @Override
    public void put(String name, byte[] data) {
        for (int i = 0; i < NUMBER_OR_TRIES; i++) {
            try {
                if (zooKeeper.exists(ZOOKEEPER_PATH + name, false) != null)
                    zooKeeper.setData(ZOOKEEPER_PATH + name, data, -1);
                else
                    zooKeeper.create(ZOOKEEPER_PATH + name, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                return;
            } catch(KeeperException.ConnectionLossException e1) {
                if (i == NUMBER_OR_TRIES - 1)
                    throw Throwables.throwUncheckedException(e1);
            } catch(KeeperException.SessionExpiredException e1) {
                if (i == NUMBER_OR_TRIES - 1) {
                    throw Throwables.throwUncheckedException(e1);
                } else {
                    init();
                }
            } catch(KeeperException e) {
                throw Throwables.throwUncheckedException(e);
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            }
        }
    }

    @Override
    public byte[] get(String name) {
        for (int i = 0; i < NUMBER_OR_TRIES; i++) {
            try {
                if (zooKeeper.exists(ZOOKEEPER_PATH + name, false) != null)
                    return zooKeeper.getData(ZOOKEEPER_PATH + name, false, new Stat());
                else
                    return null;
            } catch(KeeperException.ConnectionLossException e1) {
                if (i == NUMBER_OR_TRIES - 1)
                    throw Throwables.throwUncheckedException(e1);
            } catch(KeeperException.SessionExpiredException e1) {
                if (i == NUMBER_OR_TRIES - 1) {
                    throw Throwables.throwUncheckedException(e1);
                } else {
                    init();
                }
            } catch (KeeperException e) {
                throw Throwables.throwUncheckedException(e);
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            }
        }
        return null; // it will not get to it
    }
}
