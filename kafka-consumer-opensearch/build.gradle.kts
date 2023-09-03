plugins {
    id("java")
}

group = "org.kafka.demos"
version = "unspecified"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    // https://search.maven.org/artifact/org.opensearch.client/opensearch-rest-high-level-client/1.2.4/jar
    implementation("org.opensearch.client:opensearch-rest-high-level-client:2.9.0")

    // https://search.maven.org/artifact/com.google.code.gson/gson/2.9.0/jar
    implementation("com.google.code.gson:gson:2.10.1")
}

tasks.test {
    useJUnitPlatform()
}