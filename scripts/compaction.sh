#!/bin/bash
lineOfKeyspaces=`./service/bin/cqlsh -e "describe keyspaces;"`
echo $lineOfKeyspaces

for keyspace in ${lineOfKeyspaces[@]}
do
        echo "keyspace = '$keyspace'"
        keyspaceWithoutQuotes="${keyspace//\"}"
        echo "keyspaceWithoutQuotes = $keyspaceWithoutQuotes"
        if [[ $keyspace == *"system"* || $keyspace == *"__simple_rf_test_keyspace__"* ]]; then
                echo "Skipping $keyspace as this is a test or system keyspace!"
                continue;
        fi
        txTable=" _transactions"
        ./service/bin/nodetool flush && ./service/bin/nodetool compact $keyspaceWithoutQuotes$txTable
        echo "Compacted the $txTable table in keyspace $keyspace!"
done
