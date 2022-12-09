/*
 * Copyright (C) 2023 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.github.jengelman.gradle.plugins.shadow.ShadowPlugin
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `java-library`
  `maven-publish`
  id("com.diffplug.spotless")
  id("com.github.johnrengelman.shadow")
  Utilities
}

repositories { mavenCentral() }

applyShadowJar()

dependencies {
  api("com.google.guava:guava:31.1-jre")
  api("org.slf4j:log4j-over-slf4j:1.7.36")
  api("ch.qos.logback:logback-classic:1.2.11")
  api("ch.qos.logback:logback-core:1.2.11")
  api("info.picocli:picocli:4.7.0")
  api("org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0")
  api("org.apache.iceberg:iceberg-dell:1.1.0")
  api("org.apache.hadoop:hadoop-common:3.2.4")
  api("org.apache.hadoop:hadoop-aws:3.2.4")
  api("com.amazonaws:aws-java-sdk:1.7.4")

  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.engine)
}

group = "org.projectnessie"

version = file("version.txt").readText().trim()

description = "iceberg-catalog-migrator"

java.sourceCompatibility = JavaVersion.VERSION_1_8

val processResources =
  tasks.named<ProcessResources>("processResources") {
    inputs.property("projectVersion", project.version)
    filter(
      org.apache.tools.ant.filters.ReplaceTokens::class,
      mapOf("tokens" to mapOf("projectVersion" to project.version))
    )
  }

tasks.named<Test>("test") { systemProperty("expectedCLIVersion", project.version) }

fun Project.applyShadowJar() {
  plugins.apply(ShadowPlugin::class.java)

  plugins.withType<ShadowPlugin>().configureEach {
    val shadowJar =
      tasks.named<ShadowJar>("shadowJar") {
        isZip64 = true // as the package has more than 65535 files
        outputs.cacheIf { false } // do not cache uber/shaded jars
        archiveClassifier.set("")
        mergeServiceFiles()
      }

    tasks.named<Jar>("jar") {
      dependsOn(shadowJar)
      archiveClassifier.set("raw")
    }
  }
}

val mainClassName = "org.projectnessie.tools.catalog.migration.CatalogMigrationCLI"

extra["versionGoogleJavaFormat"] = libs.versions.googleJavaFormat.get()

val shadowJar = tasks.named<ShadowJar>("shadowJar")

val unixExecutable by
  tasks.registering {
    group = "build"
    description = "Generates the Unix executable"

    dependsOn(shadowJar)
    val dir = buildDir.resolve("executable")
    val executable = dir.resolve("iceberg-catalog-migrator")
    inputs.files(shadowJar.get().archiveFile).withPathSensitivity(PathSensitivity.RELATIVE)
    outputs.files(executable)
    outputs.cacheIf { false } // very big file
    doFirst {
      dir.mkdirs()
      executable.outputStream().use { out ->
        projectDir.resolve("src/exec/exec-preamble.sh").inputStream().use { i -> i.transferTo(out) }
        shadowJar.get().archiveFile.get().asFile.inputStream().use { i -> i.transferTo(out) }
      }
      executable.setExecutable(true)
    }
  }

shadowJar {
  manifest { attributes["Main-Class"] = mainClassName }
  finalizedBy(unixExecutable)
}
