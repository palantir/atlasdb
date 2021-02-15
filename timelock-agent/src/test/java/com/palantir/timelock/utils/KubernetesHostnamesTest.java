/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.timelock.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KubernetesHostnamesTest {

    private static final KubernetesHostnames NON_K8S_INSTANCE = new KubernetesHostnames(() -> "fake-url");
    private static final KubernetesHostnames K8S_INSTANCE =
            new KubernetesHostnames(() -> "svc-47775-2.svc-47775.namespace.svc.cluster.local");

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void getCurrentHostname_not_k8s() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Not running in a k8s stateful set.");
        NON_K8S_INSTANCE.getCurrentHostname();
    }

    @Test
    public void getCurrentHostname_k8s() throws Exception {
        assertThat(K8S_INSTANCE.getCurrentHostname()).isEqualTo("svc-47775-2.svc-47775.namespace");
    }

    @Test
    public void getClusterMembers_not_k8s() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Not running in a k8s stateful set.");
        NON_K8S_INSTANCE.getClusterMembers(4);
    }

    @Test
    public void getClusterMembers_k8s() throws Exception {
        assertThat(K8S_INSTANCE.getClusterMembers(3))
                .containsOnly(
                        "svc-47775-0.svc-47775.namespace",
                        "svc-47775-1.svc-47775.namespace",
                        "svc-47775-2.svc-47775.namespace");
    }

    @Test
    public void getClusterMembers_k8s_incorrectCount() throws Exception {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Current Pod ID 2 indicates a cluster size greater than the expected 2.");
        K8S_INSTANCE.getClusterMembers(2);
    }
}
