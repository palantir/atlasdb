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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;

public class FullQuery {
    private final String query;
    private final List<Object> args;

    public static final class Builder {
        private final StringBuilder queryBuilder = new StringBuilder(100);
        private final ImmutableList.Builder<Object> argsBuilder = ImmutableList.builder();

        private Builder() {}

        public Builder append(String query) {
            queryBuilder.append(query);
            return this;
        }

        public Builder append(long number) {
            queryBuilder.append(number);
            return this;
        }

        public Builder append(String query, Object arg) {
            queryBuilder.append(query);
            argsBuilder.add(arg);
            return this;
        }

        public Builder append(String query, Object arg1, Object arg2) {
            queryBuilder.append(query);
            argsBuilder.add(arg1);
            argsBuilder.add(arg2);
            return this;
        }

        public Builder append(String query, Object arg1, Object arg2, Object arg3) {
            queryBuilder.append(query);
            argsBuilder.add(arg1);
            argsBuilder.add(arg2);
            argsBuilder.add(arg3);
            return this;
        }

        public Builder addArg(Object arg) {
            argsBuilder.add(arg);
            return this;
        }

        public Builder addAllArgs(Iterable<?> args) {
            argsBuilder.addAll(args);
            return this;
        }

        public FullQuery build() {
            // TODO(gbonik): remove new ArrayList<> once we get rid of withArgs methods
            return new FullQuery(queryBuilder.toString(), new ArrayList<>(argsBuilder.build()));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private FullQuery(String query, List<Object> args) {
        this.query = query;
        this.args = args;
    }

    /** @deprecated Use builder() instead.
     */
    @Deprecated
    public FullQuery(String query) {
        this.query = query;
        this.args = new ArrayList<>();
    }

    /** @deprecated Use builder() instead.
     */
    @Deprecated
    public FullQuery withArg(Object arg) {
        this.args.add(arg);
        return this;
    }

    /** @deprecated Use builder() instead.
     */
    @Deprecated
    public FullQuery withArgs(Iterable<?> newArgs) {
        Iterables.addAll(args, newArgs);
        return this;
    }

    /** @deprecated Use builder() instead.
     */
    @Deprecated
    public FullQuery withArgs(Object arg1, Object arg2) {
        this.args.add(arg1);
        this.args.add(arg2);
        return this;
    }

    /** @deprecated Use builder() instead.
     */
    @Deprecated
    public FullQuery withArgs(Object arg1, Object arg2, Object arg3) {
        this.args.add(arg1);
        this.args.add(arg2);
        this.args.add(arg3);
        return this;
    }

    public String getQuery() {
        return query;
    }

    public Object[] getArgs() {
        return args.toArray();
    }
}
