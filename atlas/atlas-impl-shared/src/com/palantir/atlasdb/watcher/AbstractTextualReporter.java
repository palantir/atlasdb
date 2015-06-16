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

package com.palantir.atlasdb.watcher;

import com.palantir.atlasdb.watcher.ProgressWatcher.Reporter;

abstract class AbstractTextualReporter implements Reporter {
    @Override
    public void start(String name) {
        output("start", name, "started");
    }

    @Override
    public void step(String name, int steps, int maxSteps, String itemsName) {
        if (maxSteps == 0) {
            output("step", name, "%s %s completed", steps, itemsName);
        } else if (steps * 50 / maxSteps > (steps - 1) * 50 / maxSteps) {
            output("step", name, "%s/%s %s completed", steps, maxSteps, itemsName);
        }
    }

    @Override
    public void stop(String name, long durationMillis) {
        output("stop", name, "finished after %s ms", durationMillis);
    }

    @Override
    public void total(String name, long durationMillis) {
        output("total", name, "spent %s ms total", durationMillis);
    }

    @Override
    public void say(String name, String format, Object... args) {
        output("note", name, format, args);
    }

    private void output(String tag, String name, String format, Object... args) {
        output(String.format("%-5s %s %s", tag.toUpperCase(), name, String.format(format, args)));
    }

    abstract protected void output(String msg);
}
