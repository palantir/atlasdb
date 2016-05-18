package com.palantir.atlasdb

import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

class AtlasPluginTests extends Specification {

    @Rule
    TemporaryFolder temporaryFolder = new TemporaryFolder()

    File projectDir
    File buildFile

    def 'task creates cli startup script' () {
        given:
        buildFile << '''
            plugins {
                id 'com.palantir.atlasdb'
            }
            atlasdb {
                atlasVersion '0.3.34'
            }
        '''.stripIndent()

        when:
        BuildResult buildResult = run('build', 'createAtlasScripts').build()

        then:
        buildResult.task(':build').outcome == TaskOutcome.SUCCESS
        buildResult.task(':createAtlasScripts').outcome == TaskOutcome.SUCCESS

        new File(projectDir, 'build/scripts/atlas').exists()
        exec('build/scripts/atlas').contains('Could not find or load main class')
    }

    def 'adds atlas script to existing SLS distribution' () {
        given:
        buildFile << '''
            plugins {
                id 'com.palantir.atlasdb'
                id 'com.palantir.java-distribution'
                id 'java'
            }
            atlasdb {
                atlasVersion '0.5.0'
            }
            version '0.1'
            distribution {
                serviceName 'service-name'
                mainClass 'test.Test'
                defaultJvmOpts '-Xmx4M', '-Djavax.net.ssl.trustStore=truststore.jks'
            }
            sourceCompatibility = '1.7'
            // most convenient way to untar the dist is to use gradle
            task untar (type: Copy) {
                from tarTree(resources.gzip("${buildDir}/distributions/service-name-0.1.tgz"))
                into "${projectDir}/dist"
                dependsOn distTar
            }
        '''.stripIndent()

        temporaryFolder.newFolder('src', 'main', 'java', 'test')
        temporaryFolder.newFile('src/main/java/test/Test.java') << '''
        package test;
        public class Test {
            public static void main(String[] args) throws InterruptedException {
                System.out.println("Test started");
                while(true);
            }
        }
        '''.stripIndent()

        when:
        BuildResult buildResult = run('build', 'distTar', 'untar').build()

        then:
        buildResult.task(':build').outcome == TaskOutcome.SUCCESS
        buildResult.task(':distTar').outcome == TaskOutcome.SUCCESS
        buildResult.task(':untar').outcome == TaskOutcome.SUCCESS

        projectDir.eachFileRecurse { it -> println it}

        new File(projectDir, 'dist/service-name-0.1/service/bin/atlas').exists()
        exec('dist/service-name-0.1/service/bin/atlas').contains('usage:')
    }

    def 'plugin fails without atlasVersion' () {
        given:
        buildFile << '''
            plugins {
                id 'com.palantir.atlasdb'
            }
        '''.stripIndent()

        when:
        Exception ex;
        try {
           run('build', 'createAtlasScripts').build()
        } catch (e) {
            ex = e
        }

        then:
        ex.toString().contains('atlasVersion')
    }

    private GradleRunner run(String... tasks) {
        GradleRunner.create()
                .withPluginClasspath()
                .withProjectDir(projectDir)
                .withArguments(tasks)
                .withDebug(true)
    }

    private String exec(String... tasks) {
        StringBuffer sout = new StringBuffer(), serr = new StringBuffer()
        Process proc = new ProcessBuilder().command(tasks).directory(projectDir).start()
        proc.consumeProcessOutput(sout, serr)
        proc.waitFor()
        sleep 1000 // wait for the Java process to actually run
        return sout.toString() + serr.toString()
    }

    def setup() {
        projectDir = temporaryFolder.root
        buildFile = temporaryFolder.newFile('build.gradle')
    }

}
