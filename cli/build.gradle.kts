plugins {
    kotlin("jvm")
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(projects.pbCoroutinesCore)
    implementation(projects.pbCoroutinesKafka)
    implementation(projects.pbCoroutinesRetry)

    implementation(libs.bundles.logback)
    implementation(libs.bundles.kotlinxCoroutines)

    implementation(libs.kafka)
    implementation(libs.kotlinLogging)
    implementation(libs.kotlinx.cli)
}

application {
    mainClass.set("io.provenance.kafka.cli.MainKt")
}
