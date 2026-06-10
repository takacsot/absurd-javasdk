plugins {
    `java-library`
    `maven-publish`
}

group = "io.absurd"
version = "0.3.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
    withJavadocJar()
}

repositories {
    mavenCentral()
}

dependencies {
    api("org.jdbi:jdbi3-core:3.45.4")
    api("org.jdbi:jdbi3-postgres:3.45.4")
    api("org.jdbi:jdbi3-jackson2:3.45.4")
    api("com.fasterxml.jackson.core:jackson-databind:2.17.2")

    implementation("org.postgresql:postgresql:42.7.4")
    implementation("org.slf4j:slf4j-api:2.0.16")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.testcontainers:postgresql:1.20.4")
    testImplementation("org.testcontainers:junit-jupiter:1.20.4")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
    testImplementation("com.zaxxer:HikariCP:6.2.1")
}

tasks.test {
    useJUnitPlatform()
}
