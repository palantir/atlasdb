package com.palantir.atlasdb.keyvalue.partition;

import java.util.Arrays;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.partition.api.DynamicPartitionMap;
import com.palantir.atlasdb.keyvalue.partition.endpoint.SimpleKeyValueEndpoint;
import com.palantir.atlasdb.keyvalue.partition.map.DynamicPartitionMapImpl;
import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;
import com.palantir.atlasdb.keyvalue.remoting.RemotingPartitionMapService;

public class DynamicPartitionMapManager {

    private static final Logger log = LoggerFactory.getLogger(DynamicPartitionMapManager.class);

    private DynamicPartitionMap partitionMap;

    public DynamicPartitionMapManager(String masterUri, String[] allUris) {
        PartitionMapService masterPms = RemotingPartitionMapService.createClientSide(masterUri);
        partitionMap = masterPms.getMap();
    }

    public void addEndpoint(String kvsUri, String pmsUri, byte[] key, Scanner scanner) {
        SimpleKeyValueEndpoint skve = new SimpleKeyValueEndpoint(kvsUri, pmsUri);
        partitionMap.addEndpoint(key, skve, "");
        pushMapToEndpointsWithRetry(scanner);
        partitionMap.promoteAddedEndpoint(key);
        pushMapToEndpointsWithRetry(scanner);
    }

    public void removeEndpoint(byte[] key, Scanner scanner) {
        partitionMap.removeEndpoint(key);
        pushMapToEndpointsWithRetry(scanner);
        partitionMap.promoteRemovedEndpoint(key);
        pushMapToEndpointsWithRetry(scanner);
    }

    private void pushMapToEndpointsWithRetry(Scanner scanner) {
        while (true) {
            System.out.println("Pushing map to endpoints.");
            try {
                partitionMap.pushMapToEndpoints();
                break;
            } catch (RuntimeException e) {
                e.printStackTrace();
                System.out.print("Retry? (y/n)");
                if (!scanner.nextLine().equals("y")) {
                    throw e;
                }
            }
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

        System.out.println("Adding " + new SimpleKeyValueEndpoint(kvsUri, pmsUri) + " at key " + Arrays.toString(key));
        System.out.println("y/n?");
        if (!scanner.nextLine().equals("y")) {
            System.out.println("Aborting.");
            return;
        }

        addEndpoint(kvsUri, pmsUri, key, scanner);
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

    public void updateLocalMapInteractive() {
        throw new UnsupportedOperationException();
    }

    public void setVersionInteractive(Scanner scanner) {
        Preconditions.checkState(partitionMap instanceof DynamicPartitionMapImpl);
        System.out.println("Setting partition map version");
        System.out.println("Current version: " + partitionMap.getVersion());
        System.out.print("New version: ");
        long newVersion = Long.parseLong(scanner.nextLine());
        ((DynamicPartitionMapImpl) partitionMap).setVersion(newVersion);
        pushMapToEndpointsWithRetry(scanner);
    }

    public static void main(String[] args) {

        System.out.println("AtlasDb Dynamic Partition Map Manager");
        System.out.print("Enter PMS Uri to download initial map: ");
        DynamicPartitionMapManager instance;

        try (Scanner scanner = new Scanner(System.in)) {
            String initialPmsUri = scanner.nextLine();
            System.out.println();
            instance = new DynamicPartitionMapManager(initialPmsUri, args);

            boolean exit = false;
            while (!exit) {
                System.out.println("Local partition map:");
                System.out.println(instance.partitionMap);

                System.out.println("1. Add endpoint");
                System.out.println("2. Remove endpoint");
                System.out.println("3. Update local map");
                System.out.println("4. Set version (deprecated, test only)");
                System.out.println("0. Exit");

                try {
                    switch (Integer.parseInt(scanner.nextLine())) {
                    case 1:
                        instance.addEndpointInteractive(scanner);
                        continue;
                    case 2:
                        instance.removeEndpointInteractive(scanner);
                        continue;
                    case 3:
                        instance.updateLocalMapInteractive();
                        continue;
                    case 4:
                        instance.setVersionInteractive(scanner);
                        continue;
                    case 0:
                        exit = true;
                        continue;
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                    continue;
                }

                System.out.println("Unrecognized command.");
            }
        }
    }
}
