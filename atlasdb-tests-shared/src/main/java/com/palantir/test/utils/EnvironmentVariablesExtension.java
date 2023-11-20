/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.test.utils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Assumes the given key will be set once. */
public class EnvironmentVariablesExtension implements AfterEachCallback {

    private final Map<String, String> testValues = new HashMap<>();
    private final Map<String, String> originalValues = new HashMap<>();

    public void set(String key, String value) {
        testValues.put(key, value);
        if (System.getenv().containsKey(key)) {
            originalValues.put(key, System.getenv(key));
        }

        if (value == null) {
            removeEnvironmentVariable(key);
        } else {
            setEnvironmentVariable(key, value);
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        testValues.forEach((key, value) -> {
            if (originalValues.containsKey(key)) {
                setEnvironmentVariable(key, originalValues.get(key));
            } else {
                removeEnvironmentVariable(key);
            }
        });
    }

    private void setEnvironmentVariable(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Field field = env.getClass().getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.put(key, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to set environment variable", e);
        }
    }

    private void removeEnvironmentVariable(String key) {
        try {
            Map<String, String> env = System.getenv();
            Field field = env.getClass().getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.remove(key);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to remove environment variable", e);
        }
    }
}
