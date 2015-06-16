package com.palantir.gradle.ecj

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.JavaExec;
import java.util.Properties
import java.io.File

class EcjCompilerPlugin implements Plugin<Project> {

	final ECJ_MAIN_CLASS = "org.eclipse.jdt.internal.compiler.batch.Main"

	private Project project;
	private EcjCompilerPluginExtension ecjExtension;
	private String javaExecutable;

	@Override
	public void apply(Project project) {
		this.project = project

		this.ecjExtension = project.extensions.create(EcjCompilerPluginExtension.NAME, EcjCompilerPluginExtension, this)
		
		// Find java executable
		this.project.task([type: JavaExec], "ecjPluginJavaExec", {})
		this.javaExecutable = this.project.tasks.ecjPluginJavaExec.getExecutable()
		this.project.tasks.remove(this.project.tasks.ecjPluginJavaExec)
		
		// Set each JavaCompile task to use ECJ
		this.project.tasks.withType(JavaCompile) {
			ext {
				compilerArgs = []
				compilerArgs << "-warn:none" // Let the user decide warnings (needs to be first so it doesn't override)
			}
			doFirst {
				if (project.buildscript.configurations.findByName("ecj") == null) {
					project.buildscript.configurations.create("ecj")
					project.buildscript.dependencies.add("ecj", ecjExtension.ecjDependency)
				}

				if (ecjExtension.log != null) {
					compilerArgs << "-log" << ecjExtension.log
				}

				options.fork executable: javaExecutable, jvmArgs: ["-cp", project.buildscript.configurations.ecj.asPath, ECJ_MAIN_CLASS]
				options.define compilerArgs: (compilerArgs)
			}
		}
	}

}
