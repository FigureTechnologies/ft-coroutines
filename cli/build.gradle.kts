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

    implementation(projects.ftCoroutinesCore)
    implementation(projects.ftCoroutinesRetry)
    implementation(projects.ftCoroutinesKafka)
    implementation(projects.ftCoroutinesKafkaRetry)

    implementation(libs.bundles.logback)
    implementation(libs.bundles.kotlinxCoroutines)

    implementation(libs.kafka)
    implementation(libs.kotlinLogging)
    implementation(libs.kotlinx.cli)
}

application {
    mainClass.set("tech.figure.kafka.cli.MainKt")
}
