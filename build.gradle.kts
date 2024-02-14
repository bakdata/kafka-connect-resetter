description = "An application to reset the state of Kafka Connect connectors"

plugins {
    `java-library`
    id("net.researchgate.release") version "3.0.2"
    id("com.bakdata.sonar") version "1.1.11"
    id("com.bakdata.sonatype") version "1.1.11"
    id("org.hildan.github.changelog") version "2.2.0"
    id("com.google.cloud.tools.jib") version "3.4.0"
    id("io.freefair.lombok") version "8.4"
}

allprojects {
    group = "com.bakdata.kafka"

    tasks.withType<Test> {
        maxParallelForks = 1
        useJUnitPlatform()
    }

    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven/")
    }
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

configure<com.bakdata.gradle.SonatypeSettings> {
    developers {
        developer {
            name.set("Philipp Schirmer")
            id.set("philipp94831")
        }
    }
}

configure<org.hildan.github.changelog.plugin.GitHubChangelogExtension> {
    githubUser = "bakdata"
    githubRepository = "kafka-connect-resetter"
    futureVersionTag = findProperty("changelog.releaseVersion")?.toString()
    sinceTag = findProperty("changelog.sinceTag")?.toString()
}

dependencies {
    val kafkaVersion: String by project
    implementation(group = "org.apache.kafka", name = "connect-json", version = kafkaVersion)
    implementation(group = "info.picocli", name = "picocli", version = "4.7.5")
    implementation(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
    implementation(group = "com.bakdata.seq2", name = "seq2", version = "1.0.11")
    val log4jVersion = "2.22.1"
    implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)

    val junitVersion = "5.10.1"
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.25.1")
    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = "3.5.0") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    testImplementation(group = "org.apache.kafka", name = "connect-file", version = kafkaVersion)
}
