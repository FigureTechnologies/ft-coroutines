plugins {
    kotlin("jvm")
    `java-library`
    `maven-publish`
}

val artifactName = if (name.startsWith("ft-coroutines")) name else "ft-coroutines-$name"
val projectVersion = version.toString()

java {
    withSourcesJar()
    withJavadocJar()
}

configure<PublishingExtension> {
    publications {
        create<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = artifactName
            version = projectVersion

            from(components["java"])

            pom {
                name.set("Kafka Coroutine Implementation")
                description.set("Library for reading from and writing to Kafka from Kotlin coroutines")
                url.set("https://www.figure.tech/")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }

                developers {
                    developer {
                        id.set("mtps")
                        name.set("Phil Story")
                        email.set("phil@figure.com")
                    }

                    developer {
                        id.set("wbaker-figure")
                        name.set("Wyatt Baker")
                        email.set("wbaker@figure.com")
                    }

                    developer {
                        id.set("rchaing-figure")
                        name.set("Robert Chaing")
                        email.set("rchaing@figure.com")
                    }

                    developer {
                        id.set("jhorecny-figure")
                        name.set("Josh Horecny")
                        email.set("jhorecny@figure.com")
                    }
                }

                scm {
                    developerConnection.set("git@github.com:FigureTechnologies/ft-coroutines.git")
                    connection.set("https://github.com/FigureTechnologies/ft-coroutines.git")
                    url.set("https://github.com/FigureTechnologies/ft-coroutines")
                }
            }
        }
    }
}
