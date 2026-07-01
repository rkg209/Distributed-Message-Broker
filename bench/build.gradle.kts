plugins {
    id("me.champeau.jmh") version "0.7.2"
}

dependencies {
    implementation(project(":client"))

    implementation("ch.qos.logback:logback-classic:1.5.12")
}
