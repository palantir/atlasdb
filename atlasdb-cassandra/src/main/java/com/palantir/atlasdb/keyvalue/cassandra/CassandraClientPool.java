package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import org.apache.cassandra.thrift.Cassandra;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.base.FunctionCheckedException;

public interface CassandraClientPool {
    <V, K extends Exception> V runOnHost(InetSocketAddress specifiedHost,
            FunctionCheckedException<Cassandra.Client, V, K> fn) throws K;
    <V, K extends Exception> V run(FunctionCheckedException<Cassandra.Client, V, K> fn) throws K;
    <V, K extends Exception> V runWithRetryOnHost(
            InetSocketAddress specifiedHost,
            FunctionCheckedException<Cassandra.Client, V, K> fn) throws K;
    InetSocketAddress getAddressForHost(String host) throws UnknownHostException;
    <V, K extends Exception> V runWithRetry(FunctionCheckedException<Cassandra.Client, V, K> fn) throws K;
    InetSocketAddress getRandomHostForKey(byte[] key);
    void shutdown();
}
