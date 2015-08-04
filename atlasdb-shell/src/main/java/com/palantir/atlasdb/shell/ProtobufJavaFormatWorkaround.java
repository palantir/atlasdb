/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
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
