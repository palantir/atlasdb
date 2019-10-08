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

package com.palantir.atlasdb.refactorings;

import org.junit.Test;

import com.palantir.baseline.refaster.RefasterTestHelper;

public class UseTypedUserAgentVsStringTest {

    @Test
    public void canConvertFromToStringUserAgent() {
        RefasterTestHelper.forRefactoring(UseTypedUserAgentVsString.class)
                .withInputLines("Test",
                        "import com.codahale.metrics.MetricRegistry;",
                        "import com.palantir.atlasdb.factory.TransactionManagers;",
                        "import com.palantir.atlasdb.keyvalue.api.KeyValueService;",
                        "import com.palantir.conjure.java.api.config.service.UserAgent;",
                        "import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;",
                        "",
                        "class Test {",
                        "    public void main(String[] args) {",
                        "        UserAgent userAgent = UserAgent.of(UserAgent.Agent.of(\"product\", \"1.2.3\"));",
                        "        KeyValueService kvs = TransactionManagers.builder()",
                        "                .config(null)",
                        "                .userAgent(userAgent.toString())",
                        "                .globalMetricsRegistry(new MetricRegistry())",
                        "                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())",
                        "                .build()",
                        "                .serializable()",
                        "                .getKeyValueService();",
                        "        System.out.println(kvs.isInitialized());",
                        "    }",
                        "}")
                .hasOutputLines(
                        "import com.codahale.metrics.MetricRegistry;",
                        "import com.palantir.atlasdb.factory.TransactionManagers;",
                        "import com.palantir.atlasdb.keyvalue.api.KeyValueService;",
                        "import com.palantir.conjure.java.api.config.service.UserAgent;",
                        "import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;",
                        "",
                        "class Test {",
                        "    public void main(String[] args) {",
                        "        UserAgent userAgent = UserAgent.of(UserAgent.Agent.of(\"product\", \"1.2.3\"));",
                        "        KeyValueService kvs = TransactionManagers.builder()",
                        "                .config(null).userAgent(userAgent)",
                        "                .globalMetricsRegistry(new MetricRegistry())",
                        "                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())",
                        "                .build()",
                        "                .serializable()",
                        "                .getKeyValueService();",
                        "        System.out.println(kvs.isInitialized());",
                        "    }",
                        "}");
    }

    @Test
    public void canConvertFromFormattedUserAgent() {
        RefasterTestHelper.forRefactoring(UseTypedUserAgentVsString.class)
                .withInputLines("Test",
                        "import com.codahale.metrics.MetricRegistry;",
                        "import com.palantir.atlasdb.factory.TransactionManagers;",
                        "import com.palantir.atlasdb.keyvalue.api.KeyValueService;",
                        "import com.palantir.conjure.java.api.config.service.UserAgent;",
                        "import com.palantir.conjure.java.api.config.service.UserAgents;",
                        "import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;",
                        "",
                        "class Test {",
                        "    public void main(String[] args) {",
                        "        UserAgent userAgent = UserAgent.of(UserAgent.Agent.of(\"product\", \"1.2.3\"));",
                        "        KeyValueService kvs = TransactionManagers.builder()",
                        "                .config(null)",
                        "                .userAgent(UserAgents.format(userAgent))",
                        "                .globalMetricsRegistry(new MetricRegistry())",
                        "                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())",
                        "                .build()",
                        "                .serializable()",
                        "                .getKeyValueService();",
                        "        System.out.println(kvs.isInitialized());",
                        "    }",
                        "}")
                .hasOutputLines(
                        "import com.codahale.metrics.MetricRegistry;",
                        "import com.palantir.atlasdb.factory.TransactionManagers;",
                        "import com.palantir.atlasdb.keyvalue.api.KeyValueService;",
                        "import com.palantir.conjure.java.api.config.service.UserAgent;",
                        "import com.palantir.conjure.java.api.config.service.UserAgents;",
                        "import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;",
                        "",
                        "class Test {",
                        "    public void main(String[] args) {",
                        "        UserAgent userAgent = UserAgent.of(UserAgent.Agent.of(\"product\", \"1.2.3\"));",
                        "        KeyValueService kvs = TransactionManagers.builder()",
                        "                .config(null).userAgent(userAgent)",
                        "                .globalMetricsRegistry(new MetricRegistry())",
                        "                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())",
                        "                .build()",
                        "                .serializable()",
                        "                .getKeyValueService();",
                        "        System.out.println(kvs.isInitialized());",
                        "    }",
                        "}");
    }
}
