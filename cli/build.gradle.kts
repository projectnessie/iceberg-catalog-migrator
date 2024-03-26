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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `java-library`
  `maven-publish`
  signing
  alias(libs.plugins.nessie.run)
  `build-conventions`
}

java.sourceCompatibility = JavaVersion.VERSION_1_8

applyShadowJar()

dependencies {
  implementation(project(":iceberg-catalog-migrator-api"))
  implementation(libs.guava)
  implementation(libs.slf4j)
  runtimeOnly(libs.logback.classic)
  implementation(libs.picocli)
  implementation(libs.iceberg.spark.runtime)
  implementation(libs.hadoop.aws) { exclude("com.amazonaws", "aws-java-sdk-bundle") }
  // AWS dependencies based on https://iceberg.apache.org/docs/latest/aws/#enabling-aws-integration
  runtimeOnly(libs.aws.sdk.apache.client)
  runtimeOnly(libs.aws.sdk.auth)
  runtimeOnly(libs.aws.sdk.glue)
  runtimeOnly(libs.aws.sdk.s3)
  runtimeOnly(libs.aws.sdk.dynamo)
  runtimeOnly(libs.aws.sdk.kms)
  runtimeOnly(libs.aws.sdk.lakeformation)
  runtimeOnly(libs.aws.sdk.sts)
  runtimeOnly(libs.aws.sdk.url.connection.client)

  // needed for Hive catalog
  runtimeOnly("org.apache.hive:hive-metastore:${libs.versions.hive.get()}") {
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
  runtimeOnly("org.apache.hive:hive-exec:${libs.versions.hive.get()}:core") {
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
  runtimeOnly("org.apache.hadoop:hadoop-mapreduce-client-core:${libs.versions.hadoop.get()}")

  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.engine)
  testImplementation(libs.assertj)
  testImplementation(libs.logcaptor)

  testImplementation(project(":iceberg-catalog-migrator-api-test"))

  // for integration tests
  testImplementation(
    "org.apache.iceberg:iceberg-hive-metastore:${libs.versions.iceberg.get()}:tests"
  )
  // this junit4 dependency is needed for above Iceberg's TestHiveMetastore
  testRuntimeOnly("junit:junit:4.13.2")

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

  nessieQuarkusServer(
    "org.projectnessie.nessie:nessie-quarkus:${libs.versions.nessie.get()}:runner"
  )
}

nessieQuarkusApp {
  javaVersion.set(17)
  includeTask(tasks.named<Test>("intTest"))
}

tasks.named<Test>("test") { systemProperty("expectedCLIVersion", project.version) }

val processResources =
  tasks.named<ProcessResources>("processResources") {
    inputs.property("projectVersion", project.version)
    filter(
      org.apache.tools.ant.filters.ReplaceTokens::class,
      mapOf("tokens" to mapOf("projectVersion" to project.version))
    )
  }

val mainClassName = "org.projectnessie.tools.catalog.migration.cli.CatalogMigrationCLI"

val shadowJar = tasks.named<ShadowJar>("shadowJar") { isZip64 = true }

shadowJar { manifest { attributes["Main-Class"] = mainClassName } }
