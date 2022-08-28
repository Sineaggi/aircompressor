plugins {
    `java-library`
    id("me.champeau.mrjar") version "0.1"
}

multiRelease {
    // todo: include 14, 15, 16, 17, 18 + 20
    targetVersions(8, 17, 19)
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
    testImplementation("org.opentest4j:opentest4j:1.2.0") // optional dep for assertj
}

tasks.test {
    useTestNG {
        parallel = "methods"
        threadCount = 4
    }
    maxHeapSize = "2G"
}

tasks.withType<Test>().configureEach {
    useTestNG {
        parallel = "methods"
        threadCount = 4
    }
    maxHeapSize = "2G"
}

tasks.named<JavaCompile>("compileJava17Java") {
    options.compilerArgs = listOf("--enable-preview", "--add-modules", "jdk.incubator.foreign")
}

tasks.named<Test>("java17Test") {
    jvmArgs("--enable-preview", "--add-modules", "jdk.incubator.foreign")
}

tasks.named<JavaCompile>("compileJava19Java") {
    options.compilerArgs = listOf("--enable-preview")
}

tasks.named<Test>("java19Test") {
    jvmArgs("--enable-preview")
}
