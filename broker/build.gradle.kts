plugins {
    application
}

application {
    mainClass.set("io.minikafka.broker.Main")
}

sourceSets {
    create("integrationTest") {
        java.srcDir("src/integrationTest/java")
        resources.srcDir("src/integrationTest/resources")
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += sourceSets.main.get().output
    }
}

val integrationTestImplementation by configurations.getting {
    extendsFrom(configurations.testImplementation.get())
}
val integrationTestRuntimeOnly by configurations.getting {
    extendsFrom(configurations.testRuntimeOnly.get())
}

dependencies {
    implementation(project(":protocol"))
    implementation(project(":log"))
    implementation(project(":raft"))

    implementation("ch.qos.logback:logback-classic:1.5.12")

    testImplementation(project(":client"))

    integrationTestImplementation(project(":client"))
    integrationTestImplementation(project(":protocol"))
    integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    integrationTestImplementation("org.testcontainers:testcontainers:1.20.4")
    integrationTestImplementation("org.testcontainers:junit-jupiter:1.20.4")
}

val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs the Docker Compose cluster-formation integration tests."
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.test)
}
