package com.palantir.atlasdb.sql.grammar;

import java.util.EnumSet;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

enum RelationalOp {
    EQ("="), LTH("<"), GTH(">"), LET("<="), GET(">="), NOT_EQ("<>", "!=");

    private final String[] strReps;

    RelationalOp(String... strReps) {
        Preconditions.checkArgument(strReps.length > 0);
        this.strReps = strReps;
    }

    public String[] getStringReps() {
        return strReps;
    }

    public RelationalOp flip() {
        return flipLookup.get(this);
    }

    public static RelationalOp from(String str) {
        if (lookup.containsKey(str)) {
            return lookup.get(str);
        }
        throw new IllegalArgumentException("unknown relational operation for operation " + str);
    }

    private static final Map<String,RelationalOp> lookup = Maps.newHashMap();
    static {
        for(RelationalOp op : EnumSet.allOf(RelationalOp.class)) {
            for (String strRep : op.getStringReps()) {
                lookup.put(strRep, op);
            }
        }
    }

    private static final Map<RelationalOp, RelationalOp> flipLookup =
            ImmutableMap.<RelationalOp, RelationalOp>builder()
                    .put(EQ, EQ)
                    .put(LTH, GTH)
                    .put(GTH, LTH)
                    .put(LET, GET)
                    .put(GET, LET)
                    .put(NOT_EQ, NOT_EQ)
                    .build();

}
