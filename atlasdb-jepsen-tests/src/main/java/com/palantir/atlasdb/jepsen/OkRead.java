/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.jepsen;

public class OkRead extends Event {
    Long time;
    Integer process;
    Long value;

    public OkRead(Long time, Integer process, Long value) {
        this.time = time;
        this.process = process;
        this.value = value;
    }

    public Long time() {
        return time;
    }

    public Integer process() {
        return process;
    }

    public Long value() {
        return value;
    }

    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return String.format("Successful Read Response: Time=%d Process=%d Value=%d", time, process, value);
    }
}
