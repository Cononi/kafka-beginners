plugins {
    id("java")
}

group = "org.kafka.demos"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Kafka 3.5.1 클라이언트
    implementation("org.apache.kafka:kafka-clients:3.5.1")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}