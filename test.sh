while read lineOfKeyspaces; do
    for keyspace in $lineOfKeyspaces; do
        echo "keyspace = '$keyspace'"
        if [[ $keyspace == *"system"* ]]; then
  		echo "Skipping as this is a system keyspace!"
                continue;
        fi
        txTable=" _transactions"
        echo $txTable
        ccm node1 cqlsh -e "use $keyspace; alter table \"_transactions\" with gc_grace_seconds = 0;"
        ccm node1 nodetool flush && ccm node1 nodetool compact $keyspace$txTable
        echo "Compacted the $txTable in keyspace $keyspace!"
    done
done
