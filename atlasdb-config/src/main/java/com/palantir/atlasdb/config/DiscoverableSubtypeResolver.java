/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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
//CHECKSTYLE:OFF
/*
* Copyright 2010-2013 Coda Hale and Yammer, Inc., 2014-2016 Dropwizard Team
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.palantir.atlasdb.config;

import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Jackson subtype resolver which discovers subtypes via the META-INF/services directory
 *  used by the {@link java.util.ServiceLoader}.
 */
class DiscoverableSubtypeResolver extends StdSubtypeResolver {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoverableSubtypeResolver.class);

    private final ImmutableList<Class<?>> discoveredSubtypes;

    DiscoverableSubtypeResolver(String rootClassName) {
        final ImmutableList.Builder<Class<?>> subtypes = ImmutableList.builder();
        for (Class<?> klass : discoverServices(rootClassName)) {
            for (Class<?> subtype : discoverServices(klass.getName())) {
                subtypes.add(subtype);
                registerSubtypes(subtype);
            }
        }
        this.discoveredSubtypes = subtypes.build();
    }

    ImmutableList<Class<?>> getDiscoveredSubtypes() {
        return discoveredSubtypes;
    }

    protected ClassLoader getClassLoader() {
        return this.getClass().getClassLoader();
    }

    private List<Class<?>> discoverServices(String className) {
        final List<Class<?>> serviceClasses = new ArrayList<>();
        try {
            // use classloader that loaded this class to find the service descriptors on the classpath
            // better than ClassLoader.getSystemResources() which may not be the same classloader if ths app
            // is running in a container (e.g. via maven exec:java)
            final Enumeration<URL> resources = getClassLoader().getResources("META-INF/services/" + className);
            while (resources.hasMoreElements()) {
                final URL url = resources.nextElement();
                try (InputStream input = url.openStream();
                        InputStreamReader streamReader = new InputStreamReader(input, StandardCharsets.UTF_8);
                        BufferedReader reader = new BufferedReader(streamReader)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        try {
                            serviceClasses.add(getClassLoader().loadClass(line.trim()));
                        } catch (ClassNotFoundException e) {
                            LOGGER.info("Unable to load {}", line);
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Unable to load META-INF/services/{}", className, e);
        }
        return serviceClasses;
    }
}
