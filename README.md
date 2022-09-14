# Kotlin Coroutines Libraries

A collection of kotlin coroutines flow based projects to create channels and flows around various infrastructure ideas.

## Status

[![Latest Release][release-badge]][release-latest]
[![Maven Central][maven-badge]][maven-url]
[![Apache 2.0 License][license-badge]][license-url]
[![LOC][loc-badge]][loc-report]

[license-badge]: https://img.shields.io/github/license/FigureTechnologies/ft-coroutines.svg
[license-url]: https://github.com/FigureTechnologies/ft-coroutines/blob/main/LICENSE
[maven-badge]: https://maven-badges.herokuapp.com/maven-central/FigureTechnologies/ft-coroutines-core/badge.svg
[maven-url]: https://maven-badges.herokuapp.com/maven-central/tech.figure.coroutines/ft-coroutines-core
[release-badge]: https://img.shields.io/github/tag/FigureTechnologies/ft-coroutines.svg
[release-latest]: https://github.com/FigureTechnologies/ft-coroutines/releases/latest
[loc-badge]: https://tokei.rs/b1/github/FigureTechnologies/ft-coroutines
[loc-report]: https://github.com/FigureTechnologies/ft-coroutines

## Installation

### Maven

```xml
<dependencies>
    <dependency>
        <groupId>tech.figure.coroutines</groupId>
        <artifactId>ft-coroutines-core</artifactId>
        <version>${version}</version>
    </dependency>
    <dependency>
        <groupId>tech.figure.coroutines</groupId>
        <artifactId>ft-coroutines-kafka</artifactId>
        <version>${version}</version>
    </dependency>
    <dependency>
        <groupId>tech.figure.coroutines</groupId>
        <artifactId>ft-coroutines-retry</artifactId>
        <version>${version}</version>
    </dependency>
</dependencies>
```

### Gradle

#### Groovy

In `build.gradle`:

```groovy
implementation 'tech.figure.coroutines:ft-coroutines-core:${version}'
implementation 'tech.figure.coroutines:ft-coroutines-kafka:${version}'
implementation 'tech.figure.coroutines:ft-coroutines-retry:${version}'
```

#### Kotlin

In `build.gradle.kts`:

```kotlin
implementation("tech.figure.coroutines", "ft-coroutines-core", version)
implementation("tech.figure.coroutines", "ft-coroutines-kafka", version)
implementation("tech.figure.coroutines", "ft-coroutines-retry", version)
```

## Usage

