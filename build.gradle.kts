plugins {
    id("java")
}

repositories {
    mavenCentral()
}

subprojects {
    group = "org.kafka.demos"
    version = "1.0-SNAPSHOT"
    apply(plugin = "java")
    apply(plugin = "java-library")

    repositories {
        mavenCentral()
    }

    dependencies {
        // Kafka 3.5.1 클라이언트
        implementation("org.apache.kafka:kafka-clients:3.5.1")
        // SLF4J API LOG 확인용
        implementation("org.slf4j:slf4j-api:2.0.7")
        implementation("org.slf4j:slf4j-simple:2.0.7")
        // Log
        testImplementation(platform("org.junit:junit-bom:5.9.1"))
        testImplementation("org.junit.jupiter:junit-jupiter")
    }
    tasks.test {
        useJUnitPlatform()
    }
}

tasks.test {
    useJUnitPlatform()
}