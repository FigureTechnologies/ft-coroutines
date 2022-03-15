plugins {
    kotlin("jvm")
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(projects.kafkaCoroutinesCore)
    implementation(libs.bundles.logback)
    implementation(libs.bundles.kotlinxCoroutines)

    implementation(libs.kafka)
    implementation(libs.kotlinLogging)
    implementation(libs.kotlinx.cli)
}

application {
    mainClass.set("io.provenance.kafka.cli.MainKt")
}
