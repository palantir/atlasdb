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
package com.palantir.util;

import javax.management.MXBean;

/**
 * Statistics for the {@link MBeanCache}.
 *
 */
@MXBean
public interface CacheStatsMBean {

    long getMissCount();

    long getPutCount();

    long getHitCount();

    int getSize();

    long getForcedGcCount();

    String getName();

    long getInverseHits();

    long getInverseMisses();

    void clearCacheAndStats();

    long getLoadTimeFromMisses();

    long getLoadTimeForCacheKey();

    float getCacheHitPercentage();

    String getCacheClass();

    int getMaxCacheSize();

    void setMaxCacheSize(int size);
}
