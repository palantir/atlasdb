package com.palantir.atlas.impl;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlas.api.ExecutionResult;

public class ExecutionService {

    public static ExecutionResult exec(String code) {
        InMemoryClassLoader classLoader = new InMemoryClassLoader();
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        JavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        fileManager = new InMemoryJavaFileManager(fileManager, classLoader);
        Writer writer = new StringWriter();
        CompilationTask task = compiler.getTask(
                writer,
                fileManager,
                null,
                null,
                null,
                ImmutableList.of(new StringJavaFile("com.palantir.Main", code)));
        if (!task.call()) {
            return new ExecutionResult(null, writer.toString(), null);
        }
        try {
            @SuppressWarnings("unchecked")
            Callable<String> customTask = (Callable<String>) classLoader.loadClass("com.palantir.Main").newInstance();
            String results = customTask.call();
            return new ExecutionResult(results, null, null);
        } catch (Exception e) {
            Writer error = new StringWriter();
            e.printStackTrace(new PrintWriter(error)); // (authorized)
            return new ExecutionResult(null, null, error.toString());
        }
    }

    private static class InMemoryJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {
        private final InMemoryClassLoader classLoader;

        public InMemoryJavaFileManager(JavaFileManager delegate, InMemoryClassLoader classLoader) {
            super(delegate);
            this.classLoader = classLoader;
        }

        @Override
        public JavaFileObject getJavaFileForOutput(Location location,
                                                   String className,
                                                   Kind kind,
                                                   FileObject sibling) {
            return classLoader.addClass(new ByteArrayJavaClass(className));
        }
    }

    private static class StringJavaFile extends SimpleJavaFileObject {
        private final String sourceCode;

        public StringJavaFile(String className, String sourceCode) {
            super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.sourceCode = sourceCode;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return sourceCode;
        }
    }

    private static class ByteArrayJavaClass extends SimpleJavaFileObject {
        private final ByteArrayOutputStream stream = new ByteArrayOutputStream();

        public ByteArrayJavaClass(String name) {
            super(URI.create("bytes:///" + name), Kind.CLASS);
        }

        @Override
        public OutputStream openOutputStream() {
            return stream;
        }

        public byte[] getBytes() {
            return stream.toByteArray();
        }
    }

    private static class InMemoryClassLoader extends ClassLoader {
        private final Map<String, ByteArrayJavaClass> classes = Maps.newHashMap();

        public ByteArrayJavaClass addClass(ByteArrayJavaClass javaClass) {
            classes.put(javaClass.getName().substring(1), javaClass);
            return javaClass;
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            ByteArrayJavaClass javaClass = classes.get(name);
            if (javaClass == null) {
                throw new ClassNotFoundException("Class " + name + " is undefined");
            }
            byte[] bytes = javaClass.getBytes();
            return defineClass(name, bytes, 0, bytes.length);
        }
    }
}
