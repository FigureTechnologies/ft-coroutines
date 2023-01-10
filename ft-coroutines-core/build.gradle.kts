plugins {
    id("core-config")
}

dependencies {
    implementation(libs.kotlinLogging)
    implementation(libs.bundles.logback)
    implementation(libs.slf4j.api)

    testImplementation(libs.kotlinx.coroutines.core)
}
