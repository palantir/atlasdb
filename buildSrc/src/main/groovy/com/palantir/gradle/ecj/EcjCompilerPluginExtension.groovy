package com.palantir.gradle.ecj

import org.gradle.api.artifacts.Dependency
import java.io.File

class EcjCompilerPluginExtension {
	
	public static final String NAME = "ptecj"

	private EcjCompilerPlugin ecjPlugin
	
	private Object ecjDependency = "org.eclipse.jdt.core.compiler:ecj:4.2.2";
	private Object log = null;

	public EcjCompilerPluginExtension(EcjCompilerPlugin ecjPlugin) {
		this.ecjPlugin = ecjPlugin
	}

	public void setEcjDependency (Object ecjDependency) {
		this.ecjDependency = ecjDependency
	}

	public Object getEcjDependency () {
		return this.ecjDependency
	}

	public void setLog(Object log) {
		this.log = log
	}

	public Object getLog() {
		return this.log
	}
}
