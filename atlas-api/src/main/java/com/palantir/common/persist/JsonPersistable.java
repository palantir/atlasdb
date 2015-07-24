// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.common.persist;

/**
 * Classes implementing {@link JsonPersistable} must also have a static field called <code>JSON_HYDRATOR</code>
 * that implements the {@link JsonPersistable.Hydrator} interface. It is also recommened that each
 * {@link JsonPersistable} class is a final class.
 */
public interface JsonPersistable extends Persistable {
    public static final String HYDRATOR_NAME = "JSON_HYDRATOR";

    public interface Hydrator<T> {
        T hydrateFromJson(String input);
    }

    String persistToJson();
}
