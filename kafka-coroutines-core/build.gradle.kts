plugins {
    kotlin("jvm")

    `java-library`
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.kafka)
    implementation(libs.kotlinLogging)
    implementation(libs.bundles.logback)
    implementation(libs.bundles.kotlinxCoroutines)
}
