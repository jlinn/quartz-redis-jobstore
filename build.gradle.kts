plugins {
    java
    id("com.palantir.git-version") version "0.15.0"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.quartz-scheduler:quartz:2.3.2")
    implementation("org.quartz-scheduler:quartz-jobs:2.3.2")
    implementation("redis.clients:jedis:4.2.3")
    implementation("com.fasterxml.jackson.core:jackson-core:2.13.1")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.13.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.1")
    implementation("org.slf4j:slf4j-api:1.7.36")
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.hamcrest:hamcrest-all:1.3")
    testImplementation("org.mockito:mockito-all:1.10.19")
    testImplementation("com.google.guava:guava-io:r03")
    testImplementation("commons-io:commons-io:2.11.0")
    testImplementation("com.github.kstyrc:embedded-redis:0.6")
    testImplementation("net.jodah:concurrentunit:0.4.6")
}

val gitVersion: groovy.lang.Closure<Any> by extra

group = "com.staffbase.quartz-redis-jobstore"
version = gitVersion()

java.sourceCompatibility = JavaVersion.VERSION_1_8

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}
