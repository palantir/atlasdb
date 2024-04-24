/// *
// * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package com.palantir.atlasdb.workload.migration.cql;
//
// import com.datastax.driver.core.ExecutionInfo;
// import com.datastax.driver.core.Session;
// import java.util.Map;
// import java.util.Set;
// import java.util.function.Function;
// import one.util.streamex.StreamEx;
//
// public class CqlCassandraKeyspaceReplicationStrategyManager implements CassandraKeyspaceReplicationStrategyManager {
//    public static final String TOPOLOGY_STRATEGY_KEY = "class";
//    public static final String NETWORK_TOPOLOGY_STRATEGY = "NetworkTopologyStrategy";
//
//    @Override
//    public void setReplicationFactorToThreeForDatacenters(Set<String> datacenters, String keyspace) {
//        Map<String, String> datacenterReplicationFactor = StreamEx.of(datacenters)
//                .mapToEntry(_datacenter -> "3")
//                .append(TOPOLOGY_STRATEGY_KEY, NETWORK_TOPOLOGY_STRATEGY)
//                .toMap();
//        //
//        //        runWithCqlSession(session -> {
//        //            SchemaMutationResult.fromExecutionInfo()
//        //        })
//    }
//
//    private <T> T runWithCqlSession(Function<Session, T> sessionConsumer) {
//        try (Session session = null) {
//            return sessionConsumer.apply(session);
//        }
//    }
//
//    public enum SchemaMutationResult {
//        SUCCESS,
//        SCHEMA_NOT_IN_AGREEMENT;
//
//        public static SchemaMutationResult fromExecutionInfo(ExecutionInfo executionInfo) {
//            return executionInfo.isSchemaInAgreement()
//                    ? SchemaMutationResult.SUCCESS
//                    : SchemaMutationResult.SCHEMA_NOT_IN_AGREEMENT;
//        }
//    }
// }
