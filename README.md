# Kotlin Coroutines Libraries

A collection of kotlin coroutines flow based projects to create channels and flows around various infrastructure ideas.

## Status

[![Latest Release][release-badge]][release-latest]
[![Maven Central][maven-badge]][maven-url]
[![Apache 2.0 License][license-badge]][license-url]
[![LOC][loc-badge]][loc-report]

[license-badge]: https://img.shields.io/github/license/provenance-io/kafka-coroutines.svg
[license-url]: https://github.com/provenance-io/kafka-coroutines/blob/main/LICENSE
[maven-badge]: https://maven-badges.herokuapp.com/maven-central/io.provenance/pb-coroutines-core/badge.svg
[maven-url]: https://maven-badges.herokuapp.com/maven-central/io.provenance.coroutines/pb-coroutines-core
[release-badge]: https://img.shields.io/github/tag/provenance-io/pb-coroutines.svg
[release-latest]: https://github.com/provenance-io/pb-coroutines/releases/latest
[loc-badge]: https://tokei.rs/b1/github/provenance-io/pb-coroutines
[loc-report]: https://github.com/provenance-io/pb-coroutines

## Installation

### Maven

```xml
<dependencies>
    <dependency>
        <groupId>io.provenance.coroutines</groupId>
        <artifactId>pb-coroutines-core</artifactId>
        <version>${version}</version>
    </dependency>
    <dependency>
        <groupId>io.provenance.coroutines</groupId>
        <artifactId>pb-coroutines-kafka</artifactId>
        <version>${version}</version>
    </dependency>
    <dependency>
        <groupId>io.provenance.coroutines</groupId>
        <artifactId>pb-coroutines-retry</artifactId>
        <version>${version}</version>
    </dependency>
</dependencies>
```

### Gradle

#### Groovy

In `build.gradle`:

```groovy
implementation 'io.provenance.coroutines:pb-coroutines-core:${version}'
implementation 'io.provenance.coroutines:pb-coroutines-kafka:${version}'
implementation 'io.provenance.coroutines:pb-coroutines-retry:${version}'
```

#### Kotlin

In `build.gradle.kts`:

```kotlin
implementation("io.provenance.coroutines", "pb-coroutines-core", version)
implementation("io.provenance.coroutines", "pb-coroutines-kafka", version)
implementation("io.provenance.coroutines", "pb-coroutines-retry", version)
```

## Usage

