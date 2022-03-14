package util

import org.gradle.api.Project
import org.gradle.api.tasks.bundling.Jar
import org.gradle.kotlin.dsl.creating
import org.gradle.kotlin.dsl.get
import org.gradle.kotlin.dsl.getValue

/**
 * Generates a javadoc JAR using dokka for the given project
 */
fun projectJavadocJar(project: Project): Jar {
    val javadocJar by project.tasks.creating(Jar::class) {
        from(project.tasks.get("dokkaJavadoc"))
        // classifier = "javadoc" (deprecated)
        archiveClassifier.convention("javadoc")
        archiveClassifier.set("javadoc")
    }
    return javadocJar
}
