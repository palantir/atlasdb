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
package com.palantir.atlasdb.keyvalue.partition;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.NavigableMap;
import java.util.Scanner;
import java.util.concurrent.Callable;

import javax.annotation.CheckForNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.KeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.endpoint.SimpleKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.exception.EndpointVersionTooOldException;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.partition.quorum.QuorumParameters;
import com.palantir.atlasdb.keyvalue.partition.status.EndpointWithStatus;
import com.palantir.atlasdb.keyvalue.partition.util.CycleMap;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;

public class DynamicPartitionMapManager {

    private DynamicPartitionMap partitionMap;

    public DynamicPartitionMapManager(String masterUri) {
        PartitionMapService masterPms = RemotingPartitionMapService.createClientSide(masterUri);
        partitionMap = masterPms.getMap();
    }

    public DynamicPartitionMapManager(DynamicPartitionMap dpm) {
        partitionMap = dpm;
    }

    @CheckForNull
    private <T> T runRetryableTask(Callable<T> task, Scanner scanner) {
        while (true) {
            try {
                return task.call();
            } catch (Exception e) {
                e.printStackTrace(System.out);
                if (e instanceof EndpointVersionTooOldException) {
                    System.out.println("Pushing new map to endpoint...");
                    ((EndpointVersionTooOldException) e).pushNewMap(partitionMap);
                    System.out.println("Pushed.");
                }
                System.out.print("Retry? (y/n) ");
                if (!scanner.nextLine().equals("y")) {
                    System.out.println("Fatal? (y/n)");
                    if (!scanner.nextLine().equals("n")) {
                        throw Throwables.throwUncheckedException(e);
                    }
                    break;
                }
            }
        }
        return null;
    }

    public void addEndpoint(String kvsUri, String pmsUri, final byte[] key, String rack, Scanner scanner) {
        final SimpleKeyValueEndpoint skve = SimpleKeyValueEndpoint.create(kvsUri, pmsUri, rack);

        runRetryableTask(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                System.out.println("Adding...");
                boolean added = partitionMap.addEndpoint(key, skve);
                Preconditions.checkState(added);
                return null;
            }

        }, scanner);

        pushMapToEndpointsNonCritical();

        runRetryableTask(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                System.out.println("Backfilling...");
                partitionMap.backfillAddedEndpoint(key);
                return null;
            }
        }, scanner);

        pushMapToEndpointsNonCritical();

        runRetryableTask(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                System.out.println("Promoting...");
                partitionMap.promoteAddedEndpoint(key);
                return null;
            }
        }, scanner);

        pushMapToEndpointsNonCritical();
    }

    public void removeEndpoint(final byte[] key, Scanner scanner) {
        runRetryableTask(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                System.out.println("Removing...");
                boolean removed = partitionMap.removeEndpoint(key);
                Preconditions.checkState(removed);
                return null;
            }

        }, scanner);

        pushMapToEndpointsNonCritical();

        runRetryableTask(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                System.out.println("Backfilling...");
                partitionMap.backfillRemovedEndpoint(key);
                return null;
            }
        }, scanner);

        pushMapToEndpointsNonCritical();

        runRetryableTask(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                System.out.println("Promoting...");
                partitionMap.promoteRemovedEndpoint(key);
                return null;
            }
        }, scanner);

        pushMapToEndpointsNonCritical();
    }

    private void pushMapToEndpointsNonCritical() {
        try {
            partitionMap.pushMapToEndpoints();
        } catch (RuntimeException e) {
            System.out.println("Non-critical error encountered while pushing map to endpoints:");
            e.printStackTrace(System.out);
        }
    }

    public void updateLocalMap(String pmsUri) {
        partitionMap = RemotingPartitionMapService.createClientSide(pmsUri).getMap();
    }

    public void addEndpointInteractive(Scanner scanner) {
        System.out.println("Adding endpoint");
        byte[] key = readKey(scanner);

        System.out.print("Enter key value service URI: ");
        String kvsUri = scanner.nextLine();

        System.out.print("Enter partition map service URI: ");
        String pmsUri = scanner.nextLine();

        System.out.print("Enter rack name: ");
        String rack = scanner.nextLine();

        System.out.println("Adding " + SimpleKeyValueEndpoint.create(kvsUri, pmsUri, rack) + " at key " + Arrays.toString(key));
        System.out.print("y/n? ");
        if (!scanner.nextLine().equals("y")) {
            System.out.println("Aborting.");
            return;
        }

        addEndpoint(kvsUri, pmsUri, key, rack, scanner);
    }

    public void removeEndpointInteractive(Scanner scanner) {
        System.out.println("Removing endpoint");
        byte[] key = readKey(scanner);

        System.out.println("Removing endpoint at " + Arrays.toString(key));
        System.out.println("y/n?");
        if (!scanner.nextLine().equals("y")) {
            System.out.println("Aborting.");
            return;
        }

        removeEndpoint(key, scanner);
    }

    private static byte[] readKey(Scanner scanner) {
        System.out.print("Key length: ");

        int keyLen = Integer.parseInt(scanner.nextLine());
        byte[] key = new byte[keyLen];

        for (int i=0; i<keyLen; ++i) {
            System.out.println("Enter unsigned byte no. " + i);
            int intByte = Integer.parseInt(scanner.nextLine());
            if (intByte < 0 || intByte > 255) {
                throw new NumberFormatException("Invalid range for unsigned byte!");
            }
            key[i] = (byte) intByte;
        }

        return key;
    }

    public void updateLocalMapInteractive(Scanner scanner) {
        System.out.println("Updating local map");
        System.out.println("Enter PMS Uri to download new map: ");
        String pmsUri = scanner.nextLine();
        updateLocalMap(pmsUri);
    }

    public void setVersionInteractive(Scanner scanner) {
        Preconditions.checkState(partitionMap instanceof DynamicPartitionMapImpl);
        System.out.println("Setting partition map version");
        System.out.println("Current version: " + partitionMap.getVersion());
        System.out.print("New version: ");
        long newVersion = Long.parseLong(scanner.nextLine());
        ((DynamicPartitionMapImpl) partitionMap).setVersion(newVersion);
        pushMapToEndpointsNonCritical();
    }

    public void pushToUriInteractive(Scanner scanner) {
        System.out.println("Pushing local map to URI");

        System.out.print("Enter PMS URI: ");
        String pmsUri = scanner.nextLine();
        pushMapToUri(pmsUri);
    }

    public void pushMapToUri(String pmsUri) {
        RemotingPartitionMapService.createClientSide(pmsUri).updateMap(partitionMap);
    }

    public static void main(String[] args) {

        System.out.println("AtlasDb Dynamic Partition Map Manager");
        System.out.print("Enter PMS Uri to download initial map (empty for empty map): ");
        final DynamicPartitionMapManager instance;

        try (Scanner scanner = new Scanner(System.in)) {
            String initialPmsUri = scanner.nextLine();
            if (!initialPmsUri.equals("")) {
                instance = new DynamicPartitionMapManager(initialPmsUri);
            } else {
                System.out.println("This is new partition map wizard");

                System.out.print("replication factor: ");
                int repf = Integer.parseInt(scanner.nextLine());
                System.out.print("read factor: ");
                int readf = Integer.parseInt(scanner.nextLine());
                System.out.print("write factor: ");
                int writef = Integer.parseInt(scanner.nextLine());

                QuorumParameters parameters = new QuorumParameters(repf, readf, writef);
                NavigableMap<byte[], KeyValueEndpoint> initialRing = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());

                while (initialRing.size() < repf) {
                    System.out.print("kvs URI: ");
                    String kvsUri = scanner.nextLine();
                    System.out.print("pms URI: ");
                    String pmsUri = scanner.nextLine();
                    System.out.print("rack: ");
                    String rack = scanner.nextLine();
                    byte[] key = readKey(scanner);
                    SimpleKeyValueEndpoint kve = SimpleKeyValueEndpoint.create(kvsUri, pmsUri, rack);
                    initialRing.put(key, kve);
                }

                DynamicPartitionMapImpl dpmi = DynamicPartitionMapImpl.create(parameters, initialRing, PTExecutors.newCachedThreadPool());
                instance = new DynamicPartitionMapManager(dpmi);
            }

            boolean exit = false;
            while (!exit) {
                System.out.println("Local partition map:");
                System.out.println(instance.partitionMap);

                System.out.println("MAIN MENU");
                System.out.println("1. Add endpoint");
                System.out.println("2. Remove endpoint");
                System.out.println("3. Update local map");
                System.out.println("4. Set version (deprecated, test only)");
                System.out.println("5. Push local map to Uri");
                System.out.println("6. Clear all endpoint kvss");
                System.out.println("0. Exit");
                System.out.print("Choice: ");

                try {
                    switch (Integer.parseInt(scanner.nextLine())) {
                    case 1:
                        instance.addEndpointInteractive(scanner);
                        continue;
                    case 2:
                        instance.removeEndpointInteractive(scanner);
                        continue;
                    case 3:
                        instance.updateLocalMapInteractive(scanner);
                        continue;
                    case 4:
                        instance.setVersionInteractive(scanner);
                        continue;
                    case 5:
                        instance.pushToUriInteractive(scanner);
                        continue;
                    case 6:
                        instance.clearAllEndpointKvssInteractive(scanner);
                        continue;
                    case 0:
                        exit = true;
                        continue;
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace(System.out);
                    continue;
                } catch (RuntimeException e) {
                    System.out.println("ERROR DURING OPERATION");
                    e.printStackTrace(System.out);
                    System.out.println("\n\nReturning to main menu\n\n");
                    continue;
                }

                System.out.println("Unrecognized command.");
            }
        }
    }

    private void clearAllEndpointKvssInteractive(Scanner scanner) {
        try {
            Field f = partitionMap.getClass().getDeclaredField("ring");
            f.setAccessible(true);
            @SuppressWarnings("unchecked")
            CycleMap<?, EndpointWithStatus> ring = (CycleMap<?, EndpointWithStatus>) f.get(partitionMap);
            f.setAccessible(false);
            System.err.println("Ring=" + ring);
            for (EndpointWithStatus ews : ring.values()) {
                KeyValueService kvs = ews.get().keyValueService();
                for (TableReference tableRef : kvs.getAllTableNames()) {
                    System.err.println("Dropping table " + tableRef.getQualifiedName() + " from " + kvs);
                    kvs.dropTable(tableRef);
                }
            }
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }
}
