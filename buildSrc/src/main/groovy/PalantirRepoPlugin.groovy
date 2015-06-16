import org.gradle.api.Project
import org.gradle.api.Plugin

class PalantirRepoPlugin implements Plugin<Project> {
    void apply(Project project) {
        project.repositories.ext.ptrepo = { ptrepo(project.repositories) }
        project.buildscript.repositories.ext.ptrepo = { ptrepo(project.buildscript.repositories) }
    }

    public static void ptrepo(def scope) {
        scope.ivy {
            name 'artifactoryRelease'
            url  'http://ivy.yojoe.local/artifactory/ivy-release'
            layout 'pattern', {
                artifact '[organisation]/[module]/[revision]/[type]s/[artifact](-[classifier]).[ext]'
                ivy      '[organisation]/[module]/[revision]/[type]s/[artifact].[ext]'
            }
        }
        scope.ivy {
            name 'artifactorySnapshot'
            url  'http://ivy.yojoe.local/artifactory/ivy-snapshot'
            layout 'pattern', {
                artifact '[organisation]/[module]/[revision]/[type]s/[artifact](-[classifier]).[ext]'
                ivy      '[organisation]/[module]/[revision]/[type]s/[artifact].[ext]'
            }
        }
        scope.maven {
            name 'artifactoryThirdParty'
            url = 'http://ivy.yojoe.local/artifactory/third-party'
        }
        scope.ivy {
            name 'artifactoryThirdPartyIvy'
            url 'http://ivy.yojoe.local/artifactory/third-party'
            layout 'pattern', {
                artifact '[organisation]/[module]/[revision]/[type]s/[artifact](-[classifier]).[ext]'
                artifact '[organisation]/[module]/[revision]/[type]s/[artifact]-[revision](-[classifier]).[ext]'
                artifact '[organisation]/[module]/[revision]/[artifact].[ext]'
                ivy '[organisation]/[module]/[revision]/[type]s/[artifact](-[classifier]).[ext]'
                ivy '[organisation]/[module]/[revision]/[type]s/[artifact]-[revision](-[classifier]).[ext]'
                ivy '[organisation]/[module]/[revision]/ivy-[revision].xml'
            }
        }
    }
}

