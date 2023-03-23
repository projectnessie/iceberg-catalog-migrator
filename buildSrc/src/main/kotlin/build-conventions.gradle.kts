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

val hasSrcMain = projectDir.resolve("src/main").exists()
val hasSrcTest = projectDir.resolve("src/test").exists()

apply<PublishingHelperPlugin>()

configureIde()

configureSpotless()

configureJandex()

configureJava()

apply<CodeCoveragePlugin>()

if (hasSrcMain || hasSrcTest) {
  configureCheckstyle()

  configureErrorprone()

  if (hasSrcTest) {
    configureTestTasks()
  }
}
