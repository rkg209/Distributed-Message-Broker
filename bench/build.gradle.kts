plugins {
    application
    id("me.champeau.jmh") version "0.7.2"
}

application {
    mainClass.set("io.minikafka.bench.LoadGenerator")
}

dependencies {
    implementation(project(":client"))
    implementation(project(":protocol"))

    implementation("ch.qos.logback:logback-classic:1.5.12")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")

    jmhImplementation(project(":client"))
    jmhImplementation(project(":protocol"))
}

val rf = providers.gradleProperty("rf").orElse("3")

jmh {
    fork.set(providers.gradleProperty("jmhFork").map { it.toInt() }.orElse(1))
    warmupIterations.set(providers.gradleProperty("jmhWarmupIterations").map { it.toInt() }.orElse(2))
    iterations.set(providers.gradleProperty("jmhIterations").map { it.toInt() }.orElse(5))
    timeOnIteration.set(providers.gradleProperty("jmhTimeOnIteration").orElse("5s"))
    warmupBatchSize.set(1)
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("results/jmh/results.json"))
    threads.set(providers.gradleProperty("bench.threads").map { it.toInt() }.orElse(8))

    // rf is a JMH @Param (not just a system property) so it's self-describing in results.json —
    // BenchResultsWriter reads which RF a result measured back off this, not off the file name.
    benchmarkParameters.put("rf", objects.listProperty(String::class).apply { add(rf) })

    val bootstrap = providers.gradleProperty("bench.bootstrap").orElse("localhost:9092")
    val topic = providers.gradleProperty("bench.topic").orElse("bench")
    val partitions = providers.gradleProperty("bench.partitions").orElse("3")
    val payloadBytes = providers.gradleProperty("bench.payloadBytes").orElse("1024")

    jvmArgs.set(
        listOf(
            "-Dbench.bootstrap=${bootstrap.get()}",
            "-Dbench.topic=${topic.get()}",
            "-Dbench.partitions=${partitions.get()}",
            "-Dbench.payloadBytes=${payloadBytes.get()}",
        )
    )
}

tasks.named("jmh") {
    doLast {
        val resultsJson = layout.buildDirectory.file("results/jmh/results.json").get().asFile
        if (resultsJson.exists()) {
            resultsJson.copyTo(
                layout.buildDirectory.file("results/jmh/results-rf${rf.get()}.json").get().asFile,
                overwrite = true,
            )
        }
    }
}

val composeDir = rootProject.file("docker")

val benchClusterUp = tasks.register<Exec>("benchClusterUp") {
    description = "Brings up the 3-broker Docker Compose cluster with the bench overlay for -Prf."
    group = "verification"
    workingDir = composeDir
    val rf = providers.gradleProperty("rf").getOrElse("3")
    val overlay = if (rf == "1") "docker-compose.bench-rf1.yml" else "docker-compose.bench.yml"
    commandLine(
        "docker", "compose",
        "-f", "docker-compose.yml",
        "-f", overlay,
        "up", "-d", "--wait", "--build",
    )
    onlyIf { !project.hasProperty("skipClusterUp") }
}

tasks.register<Exec>("benchClusterDown") {
    description = "Tears down the bench Docker Compose cluster."
    group = "verification"
    workingDir = composeDir
    val rf = providers.gradleProperty("rf").getOrElse("3")
    val overlay = if (rf == "1") "docker-compose.bench-rf1.yml" else "docker-compose.bench.yml"
    commandLine(
        "docker", "compose",
        "-f", "docker-compose.yml",
        "-f", overlay,
        "down", "-v",
    )
}

tasks.named("jmh") {
    dependsOn(benchClusterUp)
}

tasks.register<JavaExec>("loadGen") {
    description = "Runs the standalone multi-threaded LoadGenerator against a running cluster."
    group = "application"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("io.minikafka.bench.LoadGenerator")
    args = (providers.gradleProperty("args").getOrElse("")).split(" ").filter { it.isNotBlank() }
}

tasks.register<JavaExec>("writeResults") {
    description = "Merges bench/build/results/jmh/results.json (RF=1 and RF=3) into docs/results.md."
    group = "verification"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("io.minikafka.bench.BenchResultsWriter")
    args = listOf(
        layout.buildDirectory.file("results/jmh/results-rf1.json").get().asFile.path,
        layout.buildDirectory.file("results/jmh/results-rf3.json").get().asFile.path,
        rootProject.file("docs/results.md").path,
    )
    doFirst {
        val existing = args!!.dropLast(1).filter { File(it).exists() }
        if (existing.isEmpty()) {
            throw GradleException(
                "No JMH results found under bench/build/results/jmh/ — run `./gradlew :bench:jmh -Prf=3` " +
                    "and `-Prf=1` first."
            )
        }
    }
}
