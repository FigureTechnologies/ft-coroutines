dependencies {
    implementation(libs.kafka)

    implementation(projects.pbCoroutinesCore)
    implementation(projects.pbCoroutinesKafka)
    implementation(projects.pbCoroutinesRetry)
}
