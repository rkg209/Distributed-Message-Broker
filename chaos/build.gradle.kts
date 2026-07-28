sourceSets {
    create("chaosTest") {
        java.srcDir("src/chaosTest/java")
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += sourceSets.main.get().output
    }
}

val chaosTestImplementation by configurations.getting {
    extendsFrom(configurations.testImplementation.get())
}
val chaosTestRuntimeOnly by configurations.getting {
    extendsFrom(configurations.testRuntimeOnly.get())
}

dependencies {
    implementation(project(":client"))
    implementation(project(":protocol"))

    implementation("ch.qos.logback:logback-classic:1.5.12")
    implementation("org.testcontainers:testcontainers:2.0.5")

    testImplementation("org.testcontainers:testcontainers:2.0.5")

    chaosTestImplementation(project(":client"))
    chaosTestImplementation(project(":protocol"))
    chaosTestImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    chaosTestImplementation("org.testcontainers:testcontainers:2.0.5")
}

val chaosTest = tasks.register<Test>("chaosTest") {
    description = "Runs the Docker-backed fault-injection harness. Never part of `test`."
    group = "verification"
    testClassesDirs = sourceSets["chaosTest"].output.classesDirs
    classpath = sourceSets["chaosTest"].runtimeClasspath
    outputs.upToDateWhen { false }
    useJUnitPlatform()
    systemProperty("chaos.crashes", providers.gradleProperty("crashes").getOrElse("20"))
    systemProperty("chaos.messages", providers.gradleProperty("messages").getOrElse("50000"))
    systemProperty("chaos.producers", providers.gradleProperty("producers").getOrElse("8"))
    systemProperty("chaos.consumers", providers.gradleProperty("consumers").getOrElse("3"))
    systemProperty("chaos.partitions", providers.gradleProperty("partitions").getOrElse("3"))
    systemProperty("chaos.seed", providers.gradleProperty("seed").getOrElse("0"))
    testLogging {
        showStandardStreams = true
    }
    shouldRunAfter(tasks.test)
}
