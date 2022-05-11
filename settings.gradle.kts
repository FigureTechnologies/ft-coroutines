rootProject.name = "pb-coroutines"

pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

// Adapted from https://stackoverflow.com/a/60456440
gradle.rootProject {
    /**
     * This library and all subprojects are set to the same shared version.
     * This is the only place the version should be updated:
     */
    val libraryVersion =
        rootProject.property("libraryVersion") ?: error("Missing libraryVersion - check gradle.properties")
    allprojects {
        group = "io.provenance.coroutines"
        version = libraryVersion
        description =
            "Library for reading from and writing to Kafka from Kotlin coroutines"
    }
}

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")
enableFeaturePreview("VERSION_CATALOGS")

include("cli")
include("pb-coroutines-core")
include("pb-coroutines-retry")
include("pb-coroutines-kafka")
include("pb-coroutines-kafka-retry")
