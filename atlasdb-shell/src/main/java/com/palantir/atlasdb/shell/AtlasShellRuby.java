package com.palantir.atlasdb.shell;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;

import javax.script.ScriptException;

import org.jruby.Ruby;
import org.jruby.RubyModule;
import org.jruby.RubyString;
import org.jruby.demo.TextAreaReadline;
import org.jruby.embed.LocalContextScope;
import org.jruby.embed.LocalVariableBehavior;
import org.jruby.embed.ScriptingContainer;
import org.jruby.ext.Readline;
import org.jruby.runtime.Arity;
import org.jruby.runtime.Block;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.callback.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.shell.audit.AuditLoggingConnection;
import com.palantir.atlasdb.shell.audit.AuditLoggingSession;
import com.palantir.atlasdb.shell.audit.AuditingInputStream;
import com.palantir.atlasdb.shell.audit.AuditingOutputStream;
import com.palantir.atlasdb.table.description.TableMetadata;

final public class AtlasShellRuby {
    private static final Logger log = LoggerFactory.getLogger(AtlasShellRuby.class);

    /**
     * Start a Ruby interpreter for interactive use, i.e., running IRB after it is done with the
     * scriptlet
     *
     * @param scriptlet initial scriptlet to execute
     * @param inputStream stdin
     * @param outputStream stdout
     * @param errorStream stderr
     * @param auditLogger a connection to an audit logging server
     * @param atlasShellContextFactory
     * @return an {@link AtlasShellRuby} object representing an interpreter that is not yet running
     *         (you must call start() for that)
     */
    public static AtlasShellRuby createInteractive(AtlasShellRubyScriptlet scriptlet,
                                                   InputStream inputStream,
                                                   OutputStream outputStream,
                                                   OutputStream errorStream,
                                                   AtlasShellInterruptCallback interruptCallback,
                                                   AuditLoggingConnection auditLogger,
                                                   AtlasShellContextFactory atlasShellContextFactory) {
        return new AtlasShellRuby(
                scriptlet,
                inputStream,
                outputStream,
                errorStream,
                interruptCallback,
                true,
                auditLogger.getNewSession(),
                atlasShellContextFactory);
    }

    /**
     * Start a Ruby interpreter for non-interactive use, i.e. not running IRB, just the scriptlet
     *
     * @param atlasShellConnectionFactory will be bound inside ruby and used to make connections
     * @param scriptlet initial scriptlet to execute
     * @param inputStream stdin
     * @param outputStream stdout
     * @param errorStream stderr
     * @param auditLogger a connection to an audit logging server
     * @param atlasShellContextFactory
     * @return an {@link AtlasShellRuby} object representing an interpreter that is not yet running
     *         (you must call start() for that)
     */
    public static AtlasShellRuby create(AtlasShellRubyScriptlet scriptlet,
                                        InputStream inputStream,
                                        OutputStream outputStream,
                                        OutputStream errorStream,
                                        AtlasShellInterruptCallback interruptCallback,
                                        AuditLoggingConnection auditLogger,
                                        AtlasShellContextFactory atlasShellContextFactory) {
        return new AtlasShellRuby(
                scriptlet,
                inputStream,
                outputStream,
                errorStream,
                interruptCallback,
                false,
                auditLogger.getNewSession(),
                atlasShellContextFactory);
    }

    private final PrintStream errorStream;
    private final PrintStream noAuditErrorStream;
    private final ScriptingContainer scriptingContainer;
    private final Thread thread;
    private final AtlasShellInterruptCallback interruptCallback;
    private final AuditLoggingSession auditLogger;

    private Object returnValue = null;
    private Set<File> rubyGemDirectories = Sets.newHashSet();

    private AtlasShellRuby(final AtlasShellRubyScriptlet scriptlet,
                           InputStream inputStream,
                           OutputStream outputStream,
                           OutputStream errorStream,
                           AtlasShellInterruptCallback interruptCallback,
                           final boolean interactive,
                           final AuditLoggingSession auditLogger,
                           AtlasShellContextFactory atlasShellContextFactory) {
        assert scriptlet != null;
        this.noAuditErrorStream = new PrintStream(errorStream);
        this.auditLogger = auditLogger;
        // (dxiao) to the best of my understanding, input and error streams are never actually
        // used by the ruby console, but we'll keep it here in case at some point they do.
        inputStream = new AuditingInputStream(inputStream, auditLogger.getInputLogger());
        outputStream = new AuditingOutputStream(outputStream, auditLogger.getOutputLogger());
        errorStream = new AuditingOutputStream(errorStream, auditLogger.getErrorLogger());
        this.errorStream = new PrintStream(errorStream);
        this.scriptingContainer = new ScriptingContainer(
                LocalContextScope.SINGLETHREAD,
                LocalVariableBehavior.TRANSIENT);
        this.scriptingContainer.setInput(inputStream);
        this.scriptingContainer.setOutput(new PrintStream(outputStream));
        this.scriptingContainer.setError(this.errorStream);
        this.scriptingContainer.put("$atlas_shell_ruby", this);
        this.scriptingContainer.put(
                "$atlas_shell_connection_factory",
                new AtlasShellConnectionFactory(atlasShellContextFactory));
        this.scriptingContainer.put("$atlas_shell_gui_callback", new AtlasShellGuiCallback() {
            @Override
            public void graphicalView(TableMetadata tableMetadata,
                             String tableName,
                             List<String> columns,
                             List<RowResult<byte[]>> rows,
                             boolean limitedResults) {
                throw new UnsupportedOperationException("Graphical table view not available in this implementation.");
            }

            @Override
            public boolean isGraphicalViewEnabled() {
                return false;
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException("Clear graphicial view not available in this implementation.");
            }
        });
        this.scriptingContainer.put("$atlas_shell_interrupt_callback", interruptCallback);
        this.thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    String script = generateRubyScript(scriptlet.preprocess(), interactive);
                    messageWithoutAuditing("%s\n", scriptlet.getRawScriptlet());
                    auditLogger.userExecutedScriptlet(scriptlet.getRawScriptlet());
                    returnValue = scriptingContainer.runScriptlet(script);
                    auditLogger.flushStreamLoggers();
                } catch (IOException e) {
                    error(e);
                } catch (RuntimeException e) {
                    error(e);
                } finally {
                    messageWithoutAuditing("DONE\n");
                }
            }
        });
        this.interruptCallback = interruptCallback;
    }

    public void addRubyGemDirectory(File rubyGemDirectory) {
        rubyGemDirectories.add(rubyGemDirectory);
    }

    private String generateRubyScript(final String scriptlet, final boolean interactive)
            throws IOException {
        List<String> scriptletLines = Lists.newArrayList();
        for (File rubyGemDirectory : rubyGemDirectories) {
            for (File file : rubyGemDirectory.listFiles()) {
                scriptletLines.add("$LOAD_PATH.unshift '" + file.getCanonicalPath() + "/lib'");
            }
        }
        scriptletLines.add("load 'atlas_shell.rb'");
        scriptletLines.add(scriptlet);
        if (interactive) {
            scriptletLines.add("ARGV << '--readline' << '--prompt' << 'inf-ruby'");
            scriptletLines.add("IRB.start");
        }
        return Joiner.on("\n").join(scriptletLines);
    }

    /**
     * Hacks into Ruby runtime to have Ruby input routed through the audit logger in Headless instances.
     *
     * Call after creating this AtlasShellRuby.
     */
    public void hookIntoHeadlessReadline() {
        Ruby runtime = getRuby();
        runtime.getLoadService().require("readline");
        RubyModule readline = runtime.fastGetModule("Readline");

        readline.defineModuleFunction("readline", new Callback() {
            @Override
            public IRubyObject execute(IRubyObject recv, IRubyObject[] args, Block block) {
                auditLogger.flushStreamLoggers();
                IRubyObject line = Readline.s_readline(recv, args[0], args[1]);
                auditLogger.logInput(line.toString());
                return line;
            }
            @Override
            public Arity getArity() { return Arity.TWO_ARGUMENTS; }
        });
    }

    /**
     * Hacks into Ruby runtime to have Ruby input routed through the audit logger in Headed instances.
     *
     * Call after creating this AtlasShellRuby.
     */
    public void hookIntoGUIReadline(final TextAreaReadline textArea) {
        final Ruby runtime = getRuby();
        runtime.getLoadService().require("readline");
        RubyModule readline = runtime.fastGetModule("Readline");

        readline.defineModuleFunction("readline", new Callback() {
            @Override
            public IRubyObject execute(IRubyObject recv, IRubyObject[] args, Block block) {
                auditLogger.flushStreamLoggers();
                String line = textArea.readLine(args[0].toString());
                if (line != null) {
                    auditLogger.logInput(line.toString());
                    return RubyString.newUnicodeString(runtime, line);
                } else {
                    auditLogger.logInput("");
                    return runtime.getNil();
                }
            }
            @Override
            public Arity getArity() { return Arity.TWO_ARGUMENTS; }
        });
    }

    /**
     * Start the interpreter running.
     */
    public void start() {
        thread.start();
    }

    /**
     * Wait for the interpreter to finish
     *
     * @return return value from interpreter (it's just the last Ruby expression evaluated)
     * @throws InterruptedException
     */
    public Object join() throws InterruptedException {
        thread.join();
        return returnValue;
    }

    /**
     * Set a custom atlasShellTableViewer if you want something nontrivial to happen when the use
     * does table.view
     *
     * @param atlasShellTableViewer the viewer to use
     */
    public void setAtlasShellTableViewer(AtlasShellGuiCallback atlasShellTableViewer) {
        this.scriptingContainer.put("$atlas_shell_gui_callback", atlasShellTableViewer);
    }

    /**
     * Call a Ruby method on a Ruby object
     *
     * @param object object to call method on
     * @param method name of the method
     * @param args arguments to pass
     * @return whatever the Ruby method returned
     * @throws NoSuchMethodException if the method is not found
     * @throws ScriptException if a Ruby exception happened along the way
     */
    public Object call(Object object, String method, Object[] args) {
        return scriptingContainer.callMethod(object, method, args);
    }

    /**
     * Insert an exception trace into the interpreter's stderr stream
     *
     * @param throwable exception to display
     */
    public void error(Throwable throwable) {
        errorStream.print("\n");
        throwable.printStackTrace(errorStream); // (authorized)
        errorStream.print("\n\n");
        errorStream.flush();
    }

    /**
     * Insert a formatted message into the interpreter's stderr stream
     *
     * @param fmt format string
     * @param args arguments to substitute
     */
    public void message(String fmt, Object... args) {
        errorStream.printf(fmt, args);
        errorStream.flush();
    }

    /*
     * Insert a formatted message into the interpreter's stderr stream,
     * but without first routing it through audit logging.
     *
     * Should only be used for system errors, such as the audit logging server being down.
     */
    private void messageWithoutAuditing(String fmt, Object... args) {
        noAuditErrorStream.printf(fmt, args);
        noAuditErrorStream.flush();
    }

    /**
     * This method allows way too tight a coupling, but you may need it if you want to link this up
     * to a ReadlineTextarea ;-(
     *
     * @deprecated
     */
    @Deprecated
    public Ruby getRuby() {
        return scriptingContainer.getProvider().getRuntime();
    }

    public AtlasShellInterruptCallback getInterruptCallback() {
        return interruptCallback;
    }

}
