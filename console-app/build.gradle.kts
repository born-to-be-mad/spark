plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    implementation("com.google.guava:guava:32.1.1-jre")
    implementation("org.apache.spark:spark-core_2.13:3.5.3")
    implementation("org.apache.spark:spark-sql_2.13:3.5.3")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

application {
    mainClass.set("by.dma.SparkApp")
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens", "java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens", "java.base/java.io=ALL-UNNAMED"
    )
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
