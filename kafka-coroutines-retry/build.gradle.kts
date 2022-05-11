plugins {
    kotlin("jvm")
    `java-library`
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    implementation(libs.kafka)
    implementation(libs.kotlinLogging)
    implementation(libs.bundles.logback)
    implementation(libs.bundles.kotlinxCoroutines)

    implementation(projects.kafkaCoroutinesCore)
}
