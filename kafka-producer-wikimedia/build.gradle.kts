plugins {
    id("java")
}

group = "org.kafka.demos"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // OKhttp
    implementation("com.squareup.okhttp3:okhttp:4.9.3")

    // OKhttp - eventsource
    implementation("com.launchdarkly:okhttp-eventsource:2.5.0")


    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    implementation(project(":common"))
}

tasks.test {
    useJUnitPlatform()
}