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
package com.palantir.atlasdb.shell.audit;



/**
 * This is a dummy implementation of the AtlasShellAuditLogger,
 * used in instances of AtlasShell which do not need audit logging.
 * Swallows the events whole, so we don't have to worry about them.
 *
 * @author dxiao
 */
public class DummyAtlasShellAuditLogger implements AtlasShellAuditLogger {

    @Override
    public void userExecutedScriptlet(long sessionId, String scriptlet) {
        // *gulp*
    }

    @Override
    public void logError(long sessionId, String message) {
        // *gulp*
    }

    @Override
    public void logOutput(long sessionId, String message) {
        // *gulp*
    }

    @Override
    public void logInput(long sessionId, String message) {
        // *gulp*
    }

}
