plugins {
    kotlin("jvm")
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    implementation(projects.pbCoroutinesCore)
    implementation(projects.pbCoroutinesRetry)
    implementation(projects.pbCoroutinesKafka)
    implementation(projects.pbCoroutinesKafkaRetry)

    implementation(libs.bundles.logback)
    implementation(libs.bundles.kotlinxCoroutines)

    implementation(libs.kafka)
    implementation(libs.kotlinLogging)
    implementation(libs.kotlinx.cli)
}

application {
    mainClass.set("io.provenance.kafka.cli.MainKt")
}
