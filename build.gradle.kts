plugins {
    application
    id("io.freefair.lombok") version "5.1.0"
    id("com.github.johnrengelman.shadow") version "6.0.0"
}

group = "com.centrica.poc"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_11

repositories {
    mavenCentral()
    jcenter()
}

val log4Version="2.13.3"
val junitVersion="5.6.2"
val assertjVersion="3.16.1"
val mongoVersion="4.0.4"
val picocliVersion="4.3.2"
val reactorVersion="3.3.6.RELEASE"

dependencies {
//    implementation(kotlin("stdlib-jdk8"))
    implementation("org.mongodb:mongodb-driver-reactivestreams:$mongoVersion")
    implementation("info.picocli:picocli:$picocliVersion")
    implementation("io.projectreactor:reactor-core:$reactorVersion")
    annotationProcessor("info.picocli:picocli-codegen:$picocliVersion")
    implementation(platform("org.apache.logging.log4j:log4j-bom:$log4Version"))
    implementation("org.apache.logging.log4j:log4j-core")
    implementation("org.apache.logging.log4j:log4j-api")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl")

    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testImplementation("org.assertj:assertj-core:$assertjVersion")
    testImplementation("io.projectreactor:reactor-test:$reactorVersion")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

//tasks.withType<JavaCompile> {
//    val compilerArgs = options.compilerArgs
//    compilerArgs.add("-Aproject=${project.group}/${project.name}")
//}

application {
    mainClassName = "com.centrica.poc.Application"
}