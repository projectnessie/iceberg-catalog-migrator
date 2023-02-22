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

import org.gradle.api.JavaVersion
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.external.javadoc.CoreJavadocOptions
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.repositories
import org.gradle.kotlin.dsl.withType

plugins { id("org.projectnessie.buildsupport.spotless") }

repositories {
  mavenCentral()
  if (System.getProperty("withMavenLocal").toBoolean()) {
    mavenLocal()
  }
}

testTasks()

tasks.withType<Jar>().configureEach {
  manifest {
    attributes["Implementation-Title"] = "iceberg-catalog-migrator"
    attributes["Implementation-Version"] = project.version
  }
}

tasks.withType<JavaCompile>().configureEach {
  options.encoding = "UTF-8"
  options.release.set(8)
}

tasks.withType<Javadoc>().configureEach {
  val opt = options as CoreJavadocOptions
  // don't spam log w/ "warning: no @param/@return"
  opt.addStringOption("Xdoclint:-reference", "-quiet")
}

plugins.withType<JavaPlugin>().configureEach {
  configure<JavaPluginExtension> {
    withJavadocJar()
    withSourcesJar()
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    modularity.inferModulePath.set(true)
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
