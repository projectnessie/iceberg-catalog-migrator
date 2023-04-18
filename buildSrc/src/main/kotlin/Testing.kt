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

import org.gradle.api.Project
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.provideDelegate
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType

fun Project.configureTestTasks() {
  tasks.withType<Test>().configureEach {
    useJUnitPlatform {}
    val testJvmArgs: String? by project
    val testHeapSize: String? by project
    if (testJvmArgs != null) {
      jvmArgs((testJvmArgs as String).split(" "))
    }

    systemProperty("file.encoding", "UTF-8")
    systemProperty("user.language", "en")
    systemProperty("user.country", "US")
    systemProperty("user.variant", "")
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

    if (testHeapSize != null) {
      setMinHeapSize(testHeapSize)
      setMaxHeapSize(testHeapSize)
    }
  }
  val intTest =
    tasks.register<Test>("intTest") {
      group = "verification"
      description = "Runs the integration tests."
    }
  tasks.named("check") { dependsOn(intTest) }
}
