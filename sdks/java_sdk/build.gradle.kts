plugins {
    `java-library`
    `maven-publish`
}

group = "io.absurd"
version = "0.3.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
    withJavadocJar()
}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.projectlombok:lombok:1.18.34")
    annotationProcessor("org.projectlombok:lombok:1.18.34")
    testCompileOnly("org.projectlombok:lombok:1.18.34")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.34")

    api("org.jdbi:jdbi3-core:3.45.4")
    api("org.jdbi:jdbi3-postgres:3.45.4")
    api("org.jdbi:jdbi3-jackson2:3.45.4")
    api("com.fasterxml.jackson.core:jackson-databind:2.17.2")

    implementation("org.postgresql:postgresql:42.7.4")
    implementation("org.slf4j:slf4j-api:2.0.16")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("io.zonky.test:embedded-postgres:2.1.0")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation("ch.qos.logback:logback-classic:1.5.12")
    testImplementation("com.zaxxer:HikariCP:6.2.1")
}

tasks.test {
    useJUnitPlatform()
}
