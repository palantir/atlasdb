package com.palantir.atlasdb.shell;

/**
 * This is disgusting. protobuf-java-format produces bad JSON. It needs to be cleaned up. There are
 * lots of backslashes. Enjoy in moderation.
 */
public class ProtobufJavaFormatWorkaround {
    public static String cleanupJsonWithInvalidEscapes(String json) {
        json = json.replaceAll("((?:^|[^\\\\])(?:\\\\\\\\)*)\\\\v", "$1\\\\u000b");
        json = json.replaceAll("((?:^|[^\\\\])(?:\\\\\\\\)*)\\\\a", "$1\\\\u0007");
        json = json.replaceAll("((?:^|[^\\\\])(?:\\\\\\\\)*)\\\\'", "$1\\\\u0027");
        return json;
    }
}
