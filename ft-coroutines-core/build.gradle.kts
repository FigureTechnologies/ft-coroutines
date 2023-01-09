plugins {
    id("core-config")
}

dependencies {
    implementation(libs.kotlinLogging)
    implementation(libs.bundles.logback)
    implementation("org.slf4j:slf4j-api:2.0.5")
}
