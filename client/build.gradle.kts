plugins {
    application
}

application {
    mainClass.set("io.minikafka.client.Main")
}

dependencies {
    implementation(project(":protocol"))
}
