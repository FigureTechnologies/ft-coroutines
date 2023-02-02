plugins {
    id("core-config")
}

dependencies {
    implementation(libs.kafka)

    implementation(projects.ftCoroutinesCore)
    implementation(projects.ftCoroutinesKafka)
    implementation(projects.ftCoroutinesRetry)
}
