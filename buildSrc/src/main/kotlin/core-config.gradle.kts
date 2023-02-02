import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

/**
 * "Convention plugin" for common plugins, dependencies, etc.
 *
 * Ongoing use of the `subprojects` DSL construct is discouraged.
 *
 * See https://docs.gradle.org/current/samples/sample_building_kotlin_applications_multi_project.html
 */
plugins {
    // Gradle plugins.
    kotlin("jvm")
    id("java-library")

    // Internal plugins.
    id("with-docs")
    id("with-linter")
}

group = rootProject.group
version = rootProject.version

repositories {
    mavenCentral()
}

dependencies {
    // Align versions of all Kotlin components.
    // See https://medium.com/@gabrielshanahan/a-deep-dive-into-an-initial-kotlin-build-gradle-kts-8950b81b214
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    implementation("org.jetbrains.kotlin", "kotlin-reflect")
    implementation("org.jetbrains.kotlin", "kotlin-stdlib")
    implementation("org.jetbrains.kotlin", "kotlin-stdlib-jdk8")
    implementation("org.jetbrains.kotlinx", "kotlinx-datetime", Versions.Kotlinx.DateTime)
    implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-core", Versions.Kotlinx.Core)
    implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-core-jvm", Versions.Kotlinx.Core)
    implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-jdk8", Versions.Kotlinx.Core)
    implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-reactive", Versions.Kotlinx.Core)

    implementation("io.github.microutils", "kotlin-logging-jvm", Versions.KotlinLogging)

    testImplementation("org.jetbrains.kotlinx", "kotlinx-coroutines-test", Versions.Kotlinx.Core)
    testImplementation("io.kotest", "kotest-runner-junit5", Versions.Kotest)

}

// Compilation:
tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict", "-Xopt-in=kotlin.RequiresOptIn")
        jvmTarget = "11"
    }
}

tasks.withType<JavaCompile> {
    sourceCompatibility = JavaVersion.VERSION_11.toString()
    targetCompatibility = JavaVersion.VERSION_11.toString()
}

// Set the java version
configure<JavaPluginExtension> {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}



// Testing:
tasks.test {
    useJUnitPlatform()
}
