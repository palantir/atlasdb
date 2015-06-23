package com.palantir.atlasdb.shell;

import java.awt.GraphicsEnvironment;
import java.awt.HeadlessException;
import java.io.File;

import javax.script.ScriptException;

import com.palantir.atlasdb.shell.audit.AuditLoggingConnection;
import com.palantir.common.swing.PTSwingRunnables;
import com.palantir.util.MacConfig;

/**
 * Main entry point for an Atlas Shell.
 */
public class AtlasShellRun {
    private final AtlasShellContextFactory atlasShellContextFactory;
    private final AuditLoggingConnection auditLogger = AuditLoggingConnection.createConnectionUsingPrefsFile();

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
        new MacConfig().usingGlobalMenu().appTitle("AtlasShell").apply();
        PTSwingRunnables.invokeLater(new Runnable() {
            @Override
            public void run() {
                new AtlasShellMainWindow(auditLogger, atlasShellContextFactory);
            }
        });
    }

    /**
     * Run the terminal version
     *
     * @param scriptlet run this initialization Ruby code
     * @throws ScriptException
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
