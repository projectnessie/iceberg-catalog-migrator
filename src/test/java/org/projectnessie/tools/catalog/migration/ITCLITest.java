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
package org.projectnessie.tools.catalog.migration;

import org.junit.jupiter.api.Test;

public class ITCLITest {

  @Test
  public void testCustom() {
    // sample code assuming HADOOP and NESSIE catalogs is up and running.

    // RunCLI run =
    //     RunCLI.run(
    //         "HADOOP",
    //         "warehouse=/tmp/warehouseHadoop,type=hadoop",
    //         "NESSIE",
    //         "uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouseNessie");
    //
    // System.out.println(run.getOut());
    //
    // Assertions.assertTrue(run.getOut().contains("Summary: "));
  }
}
