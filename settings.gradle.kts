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

val baseVersion = file("version.txt").readText().trim()

rootProject.name = "iceberg-catalog-migrator"

gradle.beforeProject {
  group = "org.projectnessie.tools.catalog.migration"
  version = baseVersion
  description =
    when (name) {
      "api" -> "Iceberg catalog migrator - api implementation"
      "api-test" -> "Iceberg catalog migrator - common test implementation"
      "cli" -> "Iceberg catalog migrator - CLI implementation"
      else -> name
    }
}

fun catalogMigratorProject(name: String) {
  include("iceberg-catalog-migrator-$name")
  project(":iceberg-catalog-migrator-$name").projectDir = file(name)
}

catalogMigratorProject("api")

catalogMigratorProject("api-test")

catalogMigratorProject("cli")

catalogMigratorProject("bom")
