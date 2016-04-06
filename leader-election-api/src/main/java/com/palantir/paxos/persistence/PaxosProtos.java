/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.paxos.persistence;

import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import com.palantir.common.base.Throwables;
import com.palantir.paxos.persistence.generated.PaxosPersistence.ExceptionProto;
import com.palantir.paxos.persistence.generated.PaxosPersistence.StackTraceElementProto;

public class PaxosProtos {
    private PaxosProtos() {
        // No instances.
    }

    public static ExceptionProto toProto(Throwable real) {
        return toProto(real, Sets.<Throwable>newHashSet(real));
    }

    private static ExceptionProto toProto(Throwable real, Set<Throwable> seen) {
        ExceptionProto.Builder builder = ExceptionProto.newBuilder();
        builder.setType(real.getClass().getName());
        if (real.getMessage() != null) {
            builder.setMessage(real.getMessage());
        }

        for (StackTraceElement element : real.getStackTrace()) {
            builder.addStackTrace(toProto(element));
        }

        if (real.getCause() != null && !seen.add(real.getCause())) {
            builder.setCause(toProto(real.getCause(), seen));
        }
        return builder.build();
    }

    private static StackTraceElementProto toProto(StackTraceElement real) {
        StackTraceElementProto.Builder builder = StackTraceElementProto.newBuilder();
        builder.setDeclaringClass(real.getClassName());
        builder.setMethodName(real.getMethodName());
        if (real.getFileName() != null) {
            builder.setFileName(real.getFileName());
        }
        builder.setLineNumber(real.getLineNumber());
        return builder.build();
    }

    public static Throwable fromProto(ExceptionProto proto) {
        return fromProto(proto, Sets.<ExceptionProto>newHashSet(proto));
    }

    private static Throwable fromProto(ExceptionProto proto, Set<ExceptionProto> seen) {
        String type = proto.getType();
        @Nullable String message = null;
        if (proto.hasMessage()) {
            message = proto.getMessage();
        }
        ExceptionProto cause = proto.getCause();
        Throwable t;
        try {
            Class<?> eClass = Class.forName(type);
            if (cause == null || !seen.add(cause)) {
                t = (Throwable) eClass.getConstructor(String.class).newInstance(message);
            } else {
                t = (Throwable) eClass.getConstructor(String.class, Throwable.class).newInstance(message, fromProto(cause, seen));
            }
        } catch (Exception e1) {
            if (cause == null || !seen.add(cause)) {
                t = new RuntimeException(type + " - " + message);
            } else {
                t = new RuntimeException(type + " - " + message, fromProto(cause, seen));
            }
        }
        StackTraceElement[] trace = new StackTraceElement[proto.getStackTraceCount()];
        for (int i = 0; i < proto.getStackTraceCount(); i++) {
            trace[i] = fromProto(proto.getStackTrace(i));
        }
        t.setStackTrace(trace);
        return Throwables.rewrap(t);
    }

    private static StackTraceElement fromProto(StackTraceElementProto proto) {
        return new StackTraceElement(proto.getDeclaringClass(),
                proto.getMethodName(),
                proto.hasFileName() ? proto.getFileName() : null,
                proto.getLineNumber());
    }
}
