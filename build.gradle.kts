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

repositories {
  if (System.getProperty("withMavenLocal").toBoolean()) {
    mavenLocal()
  }
  mavenCentral()
}

applyShadowJar()

dependencies {
  api(libs.guava)
  api(libs.slf4j)
  api(libs.picocli)
  api(libs.logback.classic)
  api(libs.logback.core)
  api(libs.iceberg.spark.runtime)
  api(libs.iceberg.dell)
  api(libs.hadoop.aws)
  api(libs.hadoop.common)
  api(libs.aws.sdk)

  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.engine)
  // for integration tests
  testImplementation(
    "org.apache.iceberg:iceberg-hive-metastore:${libs.versions.iceberg.get()}:tests"
  )
  testImplementation("org.apache.hive:hive-metastore:2.3.8") {
    exclude("org.apache.avro", "avro")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.pentaho") // missing dependency
    exclude("org.apache.hbase")
    exclude("org.apache.logging.log4j")
    exclude("co.cask.tephra")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("com.tdunning", "json")
    exclude("javax.transaction", "transaction-api")
    exclude("com.zaxxer", "HikariCP")
  }
  testImplementation("org.apache.hive:hive-exec:2.3.8:core") {
    exclude("org.apache.avro", "avro")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.pentaho") // missing dependency
    exclude("org.apache.hive", "hive-llap-tez")
    exclude("org.apache.logging.log4j")
    exclude("com.google.protobuf", "protobuf-java")
    exclude("org.apache.calcite")
    exclude("org.apache.calcite.avatica")
    exclude("com.google.code.findbugs", "jsr305")
  }
  testImplementation("org.apache.hadoop:hadoop-mapreduce-client-core:${libs.versions.hadoop.get()}")
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
