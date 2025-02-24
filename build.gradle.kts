description = "An application to reset the state of Kafka Connect connectors"

plugins {
    `java-library`
    id("com.bakdata.release") version "1.7.1"
    id("com.bakdata.sonar") version "1.7.1"
    id("com.bakdata.sonatype") version "1.7.1"
    id("com.bakdata.jib") version "1.7.1"
    id("io.freefair.lombok") version "8.11"
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
        languageVersion = JavaLanguageVersion.of(21)
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

dependencies {
    val kafkaVersion: String by project
    implementation(group = "org.apache.kafka", name = "connect-json", version = kafkaVersion)
    implementation(group = "info.picocli", name = "picocli", version = "4.7.6")
    implementation(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
    implementation(group = "com.bakdata.seq2", name = "seq2", version = "1.0.12")
    val log4jVersion = "2.24.3"
    implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)

    val junitVersion = "5.11.4"
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.27.2")
    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = "3.6.0") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    testImplementation(group = "org.apache.kafka", name = "connect-file", version = kafkaVersion)
}
