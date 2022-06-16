plugins {
    id("core-config")
}

dependencies {
    implementation(libs.kafka)
    implementation(libs.kotlinLogging)
    implementation(libs.bundles.logback)
    implementation(libs.bundles.kotlinxCoroutines)
}
