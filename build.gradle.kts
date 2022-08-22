plugins {
    `java-library`
    id("me.champeau.mrjar") version "0.1"
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(19))
}

multiRelease {
    targetVersions(8, 19)
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("io.airlift:airbase:112"))
    testAnnotationProcessor(platform("io.airlift:airbase:112"))
    testImplementation("com.google.guava:guava")
    testImplementation("org.xerial.snappy:snappy-java:1.1.8.4")
    testImplementation("org.testng:testng")
    testImplementation("com.google.inject:guice")
    testImplementation("javax.inject:javax.inject")
    testImplementation("org.openjdk.jmh:jmh-core")
    testAnnotationProcessor("org.openjdk.jmh:jmh-generator-annprocess")
    testImplementation("org.lz4:lz4-java:1.8.0")
    testImplementation("org.lz4:lz4-java:1.8.0")
    testImplementation("com.github.luben:zstd-jni:1.5.2-1")
    testImplementation("org.anarres.lzo:lzo-hadoop:1.0.6") {
        exclude("org.apache.hadoop", "hadoop-core")
        exclude("com.google.code.findbugs", "jsr305")
        exclude("com.google.code.findbugs", "annotations")
    }
    compileOnly("io.trino.hadoop:hadoop-apache:3.2.0-17") // provided
    testCompileOnly("io.trino.hadoop:hadoop-apache:3.2.0-17") // provided
    testImplementation("org.iq80.snappy:snappy:0.4")
    testImplementation("org.assertj:assertj-core")
}

tasks.test {
    useTestNG {
        parallel = "methods"
        threadCount = 4
    }
    maxHeapSize = "2G"
}
