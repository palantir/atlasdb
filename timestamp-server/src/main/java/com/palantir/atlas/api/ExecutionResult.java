package com.palantir.atlas.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"output", "compilation_errors", "execution_errors"})
public class ExecutionResult {
    private final String output;
    private final String compilationErrors;
    private final String executionErrors;

    public ExecutionResult(@JsonProperty("output") String output,
                           @JsonProperty("compilation_errors") String compilationErrors,
                           @JsonProperty("execution_errors") String executionErrors) {
        this.output = output;
        this.compilationErrors = compilationErrors;
        this.executionErrors = executionErrors;
    }

    public String getOutput() {
        return output;
    }

    public String getCompilationErrors() {
        return compilationErrors;
    }

    public String getExecutionErrors() {
        return executionErrors;
    }

    @Override
    public String toString() {
        return "ExecutionResult [output=" + output + ", compilationErrors=" + compilationErrors
                + ", executionErrors=" + executionErrors + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((compilationErrors == null) ? 0 : compilationErrors.hashCode());
        result = prime * result + ((executionErrors == null) ? 0 : executionErrors.hashCode());
        result = prime * result + ((output == null) ? 0 : output.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ExecutionResult other = (ExecutionResult) obj;
        if (compilationErrors == null) {
            if (other.compilationErrors != null) {
                return false;
            }
        } else if (!compilationErrors.equals(other.compilationErrors)) {
            return false;
        }
        if (executionErrors == null) {
            if (other.executionErrors != null) {
                return false;
            }
        } else if (!executionErrors.equals(other.executionErrors)) {
            return false;
        }
        if (output == null) {
            if (other.output != null) {
                return false;
            }
        } else if (!output.equals(other.output)) {
            return false;
        }
        return true;
    }
}
