plugins {
    id("core-config")
}

dependencies {
    implementation(projects.pbCoroutinesCore)
    implementation(projects.pbCoroutinesKafka)
}
