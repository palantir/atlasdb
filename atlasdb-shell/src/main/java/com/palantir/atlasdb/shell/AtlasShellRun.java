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

import java.awt.GraphicsEnvironment;
import java.awt.HeadlessException;
import java.io.File;

import javax.swing.SwingUtilities;

import com.palantir.atlasdb.shell.audit.AuditLoggingConnection;
import com.palantir.common.concurrent.PTExecutors;

/**
 * Main entry point for an AtlasDB Shell.
 */
public class AtlasShellRun {
    private final AtlasShellContextFactory atlasShellContextFactory;
    private final AuditLoggingConnection auditLogger = AuditLoggingConnection.loggingDisabledCreateDummyConnection();

    public AtlasShellRun(AtlasShellContextFactory atlasShellContextFactory) {
        this.atlasShellContextFactory = atlasShellContextFactory;
    }

    /**
     * Run the GUI version
     *
     * @throws HeadlessException if GraphicsEnvironment.isHeadless() returns true.
     */
    public void runHeaded() throws HeadlessException {
        initTrustStore();
        if (GraphicsEnvironment.isHeadless()) {
            throw new HeadlessException();
        }
        SwingUtilities.invokeLater(PTExecutors.wrap(new Runnable() {
            @Override
            public void run() {
                new AtlasShellMainWindow(auditLogger, atlasShellContextFactory);
            }
        }));
    }

    /**
     * Run the terminal version
     *
     * @param scriptlet run this initialization Ruby code
     */
    @SuppressWarnings("deprecation")
    public void runHeadless(String scriptlet) {
        initTrustStore();
        AtlasShellRubyScriptlet atlasShellRubyScriptlet = new AtlasShellRubyScriptlet(scriptlet);
        AtlasShellRuby atlasShellRuby = AtlasShellRuby.createInteractive(
                atlasShellRubyScriptlet,
                System.in,
                System.out,
                System.err,
                new AtlasShellInterruptCallback(),
                auditLogger,
                atlasShellContextFactory);
        BiasedProcCompletor.hookIntoRuntime(atlasShellRuby.getRuby());
        atlasShellRuby.hookIntoHeadlessReadline();
        atlasShellRuby.start();
        try {
            atlasShellRuby.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void initTrustStore() {
        // Set required system security properties if they aren't already set.
        if (System.getProperty("javax.net.ssl.trustStore") == null) {
            File clientTrustoreFile = new File("security/Client_Truststore");
            if (clientTrustoreFile.exists()) {
                System.setProperty("javax.net.ssl.trustStore", clientTrustoreFile.getAbsolutePath());
                System.clearProperty("javax.net.ssl.trustStorePassword");
            }
        }
    }
}
