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
import org.gradle.api.Project
import org.gradle.api.artifacts.VersionCatalogsExtension
import org.gradle.api.tasks.bundling.Jar
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.withType

fun Project.libsRequiredVersion(name: String): String {
  val libVer =
    extensions.getByType<VersionCatalogsExtension>().named("libs").findVersion(name).get()
  val reqVer = libVer.requiredVersion
  check(reqVer.isNotEmpty()) {
    "libs-version for '$name' is empty, but must not be empty, version. strict: ${libVer.strictVersion}, required: ${libVer.requiredVersion}, preferred: ${libVer.preferredVersion}"
  }
  return reqVer
}

fun Project.applyShadowJar() {
  plugins.apply(ShadowPlugin::class.java)

  plugins.withType<ShadowPlugin>().configureEach {
    val shadowJar =
      tasks.named<ShadowJar>("shadowJar") {
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
