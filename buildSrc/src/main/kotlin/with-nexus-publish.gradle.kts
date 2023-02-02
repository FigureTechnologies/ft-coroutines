import org.gradle.api.publish.PublishingExtension
import org.gradle.plugins.signing.SigningExtension

plugins {
    `java-library`
    `maven-publish`
    publishing
    signing
    id("io.github.gradle-nexus.publish-plugin")
}

group = rootProject.group
version = rootProject.version

val nexusUser = findProperty("nexusUser")?.toString() ?: System.getenv("NEXUS_USER")
val nexusPass = findProperty("nexusPass")?.toString() ?: System.getenv("NEXUS_PASS")

configure<io.github.gradlenexus.publishplugin.NexusPublishExtension> {
    repositories {
        sonatype {
            nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
            snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
            username.set(findProject("ossrhUsername")?.toString() ?: System.getenv("OSSRH_USERNAME"))
            password.set(findProject("ossrhPassword")?.toString() ?: System.getenv("OSSRH_PASSWORD"))
            stagingProfileId.set("858b6e4de4734a") // prevents querying for the staging profile id, performance optimization
        }
    }
}

subprojects {
    apply {
        plugin("maven-publish")
        plugin("publishing")
        plugin("signing")
        plugin("with-publish")
    }

    configure<PublishingExtension> {
        configure<SigningExtension> {
            setRequired {
                // signing is only required if the artifacts are to be published
                gradle.taskGraph.allTasks.any { it is PublishToMavenRepository }
            }

            sign(publications["maven"])
        }
    }
}