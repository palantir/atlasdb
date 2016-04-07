package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class FullQuery {
    private final String query;
    private final List<Object> args = Lists.newArrayList();

    public FullQuery(String query) {
        this.query = query;
    }

    public FullQuery withArgs(Iterable<? extends Object> newArgs) {
        Iterables.addAll(args, newArgs);
        return this;
    }

    public FullQuery withArg(Object arg) {
        this.args.add(arg);
        return this;
    }

    public FullQuery withArgs(Object arg1, Object arg2) {
        this.args.add(arg1);
        this.args.add(arg2);
        return this;
    }

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
