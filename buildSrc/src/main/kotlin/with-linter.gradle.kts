import org.gradle.kotlin.dsl.configure

/**
 * A plugin for adding ktlinter support for code linting.
 */

plugins {
    id("org.jlleitschuh.gradle.ktlint")
}

// Linting:
configure<org.jlleitschuh.gradle.ktlint.KtlintExtension> {
    verbose.set(true)
    disabledRules.set(setOf("filename", "import-ordering"))
}
