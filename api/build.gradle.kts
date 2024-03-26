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

plugins {
  `java-library`
  `maven-publish`
  signing
  alias(libs.plugins.nessie.run)
  `build-conventions`
}

dependencies {
  implementation(libs.guava)
  implementation(libs.slf4j)
  implementation(libs.iceberg.spark.runtime)
  implementation(libs.iceberg.dell)
  implementation(libs.hadoop.common) {
    exclude("org.apache.avro", "avro")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("javax.servlet", "servlet-api")
    exclude("com.google.code.gson", "gson")
    exclude("commons-beanutils")
  }

  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  testRuntimeOnly(libs.logback.classic)
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
