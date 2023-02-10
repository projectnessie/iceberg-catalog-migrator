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
  alias(libs.plugins.nessie.run)
  BuildSupport
}

applyShadowJar()

testTasks()

dependencies {
  implementation(libs.guava)
  implementation(libs.slf4j)
  implementation(libs.picocli)
  implementation(libs.logback.classic)
  implementation(libs.logback.core)
  implementation(libs.iceberg.spark.runtime)
  implementation(libs.iceberg.dell)
  implementation(libs.hadoop.aws)
  implementation(libs.hadoop.common)
  implementation(libs.aws.sdk)

  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.engine)
  testImplementation(libs.assertj)
  testImplementation(libs.system.lambda)

  // for integration tests
  testImplementation(
    "org.apache.iceberg:iceberg-hive-metastore:${libs.versions.iceberg.get()}:tests"
  )
  testImplementation("org.apache.hive:hive-metastore:${libs.versions.hive.get()}") {
    // these are taken from iceberg repo configurations
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
  testImplementation("org.apache.hive:hive-exec:${libs.versions.hive.get()}:core") {
    // these are taken from iceberg repo configurations
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

  nessieQuarkusServer("org.projectnessie:nessie-quarkus:${libs.versions.nessie.get()}:runner")
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

nessieQuarkusApp {
  includeTask(tasks.named<Test>("intTest"))
  environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
}

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

fun Project.testTasks() {
  if (projectDir.resolve("src/test").exists()) {
    tasks.withType<Test>().configureEach {
      useJUnitPlatform {}
      val testJvmArgs: String? by project
      if (testJvmArgs != null) {
        jvmArgs((testJvmArgs as String).split(" "))
      }

      systemProperty("file.encoding", "UTF-8")
      systemProperty("user.language", "en")
      systemProperty("user.country", "US")
      systemProperty("user.variant", "")
      systemProperty("test.log.level", testLogLevel())
      environment("TESTCONTAINERS_REUSE_ENABLE", "true")
      filter {
        isFailOnNoMatchingTests = false
        when (name) {
          "test" -> {
            includeTestsMatching("*Test")
            includeTestsMatching("Test*")
            excludeTestsMatching("Abstract*")
            excludeTestsMatching("IT*")
          }
          "intTest" -> includeTestsMatching("IT*")
        }
      }
      if (name != "test") {
        mustRunAfter(tasks.named<Test>("test"))
      }
    }
    val intTest =
      tasks.register<Test>("intTest") {
        group = "verification"
        description = "Runs the integration tests."
      }
    tasks.named("check") { dependsOn(intTest) }
  }
}

fun testLogLevel() = System.getProperty("test.log.level", "WARN")
