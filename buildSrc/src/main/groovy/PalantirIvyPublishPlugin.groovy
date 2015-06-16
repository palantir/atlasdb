import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.publish.ivy.IvyPublication
import org.gradle.api.tasks.bundling.Jar

class PalantirIvyPublishPlugin implements Plugin<Project> {
    void apply(Project gp) {
        gp.plugins.apply('ivy-publish')

        gp.tasks.create('sourceJar', SourceJarTask)

        gp.publishing {
            publications {
                ivy(IvyPublication) {
                    from gp.components.java
                    artifact(gp.tasks.sourceJar) {
                        type 'source'
                        conf 'runtime'
                    }
                }
            }
            repositories {
                ivy {
                    url publishUrl(gp)
                    layout 'pattern', {
                        artifact '[organisation]/[module]/[revision]/[type]s/[artifact](-[classifier]).[ext]'
                        ivy      '[organisation]/[module]/[revision]/[type]s/[artifact](-[classifier]).[ext]'
                    }
                    if (gp.hasProperty('palantirPublish.username') && gp.hasProperty('palantirPublish.password')) {
                        credentials {
                            username gp['palantirPublish.username']
                            password gp['palantirPublish.password']
                        }
                    }
                }
            }
        }
    }

    def hasCredentials(Project p) {
        return p.hasProperty('palantirPublish.username') && p.hasProperty('palantirPublish.password')
    }

    def isSnapshot(Project p) {
        return p.version =~ /-SNAPSHOT$/ || p.version =~ /-dev/ || p.version =~ /\+/
    }

    def publishUrl(Project p) {
        if (hasCredentials(p)) {
            return isSnapshot(p) ? p['palantirPublish.snapshotUrl'] : p['palantirPublish.releaseUrl']
        }
        return "file://${System.getProperty("user.home")}/.ivy2/local"
    }
}

class SourceJarTask extends Jar {
    public SourceJarTask() {
        super()
        from project.sourceSets.main.java
        classifier 'sources'
    }
}

