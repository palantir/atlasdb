// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.property;

import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;

public class InMemoryAtlasSystemPropertyManager implements AtlasSystemPropertyManager {
    private final Map<AtlasSystemProperty, String> manager = Maps.newConcurrentMap();

    @Override
    public String getSystemProperty(AtlasSystemProperty property) {
        return MoreObjects.firstNonNull(manager.get(property), property.getDefaultValue());
    }

    @Override
    public long getSystemPropertyLong(AtlasSystemProperty property) {
        return Long.parseLong(getSystemProperty(property));
    }

    @Override
    public int getSystemPropertyInteger(AtlasSystemProperty property) {
        return Integer.parseInt(getSystemProperty(property));
    }

    @Override
    public boolean getSystemPropertyBoolean(AtlasSystemProperty property) {
        return Boolean.parseBoolean(getSystemProperty(property));
    }

    @Override
    public String getCachedSystemProperty(AtlasSystemProperty property) {
        return getSystemProperty(property);
    }

    @Override
    public long getCachedSystemPropertyLong(AtlasSystemProperty property) {
        return getSystemPropertyLong(property);
    }

    @Override
    public int getCachedSystemPropertyInteger(AtlasSystemProperty property) {
        return getSystemPropertyInteger(property);
    }

    @Override
    public boolean getCachedSystemPropertyBoolean(AtlasSystemProperty property) {
        return getSystemPropertyBoolean(property);
    }

    @Override
    public void storeSystemProperty(AtlasSystemProperty property, String value) {
        manager.put(property, value);
    }

    @Override
    public void deleteSystemProperty(AtlasSystemProperty property) {
        manager.remove(property);
    }
}
