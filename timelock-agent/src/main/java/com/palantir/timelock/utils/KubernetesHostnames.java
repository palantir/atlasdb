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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KubernetesHostnames {

    public static final KubernetesHostnames INSTANCE = new KubernetesHostnames(() -> {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    });

    private static final Logger log = LoggerFactory.getLogger(KubernetesHostnames.class);
    private static final String POD_HOSTNAME_TEMPLATE = "%s-%d.%s.%s";

    private final Supplier<String> currentHostnameSupplier;

    /** The pattern for hostnames when running in a k8s statefulset. */
    private static final Pattern POD_HOSTNAME_TEMPLATE_REGEX =
            Pattern.compile("(?<service>[a-z0-9\\-.]+)-(?<podId>\\d+)\\.\\k<service>"
                    + "\\.(?<namespace>[a-z0-9\\-.]+)\\.svc\\.cluster\\.local");

    @VisibleForTesting
    KubernetesHostnames(Supplier<String> currentHostnameSupplier) {
        this.currentHostnameSupplier = currentHostnameSupplier;
    }

    public String getCurrentHostname() {
        Matcher matcher = getHostnameComponents();
        return String.format(
                POD_HOSTNAME_TEMPLATE,
                matcher.group("service"),
                Integer.valueOf(matcher.group("podId")),
                matcher.group("service"),
                matcher.group("namespace"));
    }

    public List<String> getClusterMembers(int expectedClusterSize) {
        Matcher podTemplateMatcher = getHostnameComponents();

        int ourPodId = Integer.parseInt(podTemplateMatcher.group("podId"));
        Preconditions.checkArgument(
                ourPodId < expectedClusterSize,
                "Current Pod ID %s indicates a cluster size greater than the expected %s.",
                ourPodId,
                expectedClusterSize);
        String serviceName = podTemplateMatcher.group("service");
        String namespace = podTemplateMatcher.group("namespace");
        return IntStream.range(0, expectedClusterSize)
                .mapToObj(podId -> getPodHostname(serviceName, podId, namespace))
                .collect(Collectors.toList());
    }

    private String getPodHostname(String serviceName, int podId, String namespace) {
        return String.format(POD_HOSTNAME_TEMPLATE, serviceName, podId, serviceName, namespace);
    }

    private Matcher getHostnameComponents() {
        String canonicalHostName = currentHostnameSupplier.get();
        log.info("Using hostname.", SafeArg.of("hostname", canonicalHostName));
        Matcher matcher = POD_HOSTNAME_TEMPLATE_REGEX.matcher(canonicalHostName);
        com.palantir.logsafe.Preconditions.checkState(matcher.matches(), "Not running in a k8s stateful set.");
        return matcher;
    }
}
