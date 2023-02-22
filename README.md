# Objective 
Iceberg supports managing the iceberg tables using the following  Iceberg Catalogs:
* CUSTOM (By plugging in the jar and providing implementation class name)  
* DYNAMODB
* ECS
* GLUE
* HADOOP
* HIVE
* JDBC
* NESSIE (Arctic)
* REST

Users may want to move away from one catalog and use the other catalog with their existing Iceberg tables for the following reasons:
* They were using hadoop catalog and later realized that it is not production recommended. So, they want to move tables to other production ready catalogs.
* They just heard about the awesome Arctic catalog (or Nessie) and want to move their existing iceberg tables to Dremio Arctic.
* They had an on-premise Hive catalog, but want to move tables to a cloud-based catalog as part of their cloud migration strategy.

Before the `1.1.0` Iceberg release, the only way to achieve this was **by copying the data** using `insert into catalog1.db.tableName as select * from catalog2.db.tableName`.
After the iceberg `1.1.0` release, all Iceberg Catalogs supports register table with the `catalog#registerTable()` API.
However, custom code is needed to migrate all the tables in bulk. 
**Here we introduce a CLI tool to migrate Iceberg tables in bulk from one Iceberg Catalog to another without a data copy.**

# Iceberg-catalog-migrator
A CLI tool to bulk migrate Iceberg tables from one catalog to another without a data copy.

Need to have java installed in your machine(JDK11 or later version) to use this CLI tool.

Below is the CLI syntax:
```
$ java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar -h
Usage: register [-hV] [--delete-source-tables] [--dry-run] [--identifiers-from-file=<identifiersFromFile>] [--identifiers-regex=<identifiersRegEx>]
                [--output-dir=<outputDirPath>] --source-catalog-type=<sourceCatalogType> [--source-custom-catalog-impl=<sourceCustomCatalogImpl>]
                --target-catalog-type=<targetCatalogType> [--target-custom-catalog-impl=<targetCustomCatalogImpl>] [--identifiers=<identifiers>[,
                <identifiers>...]]... [--source-catalog-hadoop-conf=<String=String>[,<String=String>...]]...
                --source-catalog-properties=<String=String>[,<String=String>...] [--source-catalog-properties=<String=String>[,
                <String=String>...]]... [--target-catalog-hadoop-conf=<String=String>[,<String=String>...]]...
                --target-catalog-properties=<String=String>[,<String=String>...] [--target-catalog-properties=<String=String>[,<String=String>...]]...

Bulk register the iceberg tables from source catalog to target catalog without data copy.

      --source-catalog-type=<sourceCatalogType>
                  source catalog type. Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]
      --source-catalog-properties=<String=String>[,<String=String>...]
                  source catalog properties (like uri, warehouse, etc)
      --source-catalog-hadoop-conf=<String=String>[,<String=String>...]
                  optional source catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when using an Iceberg FileIO.
      --source-custom-catalog-impl=<sourceCustomCatalogImpl>
                  optional fully qualified class name of the custom catalog implementation of the source catalog. Required when the catalog type is
                    CUSTOM.
      --target-catalog-type=<targetCatalogType>
                  target catalog type. Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]
      --target-catalog-properties=<String=String>[,<String=String>...]
                  target catalog properties (like uri, warehouse, etc)
      --target-catalog-hadoop-conf=<String=String>[,<String=String>...]
                  optional target catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when using an Iceberg FileIO.
      --target-custom-catalog-impl=<targetCustomCatalogImpl>
                  optional fully qualified class name of the custom catalog implementation of the target catalog. Required when the catalog type is
                    CUSTOM.
      --identifiers=<identifiers>[,<identifiers>...]
                  optional selective list of identifiers to register. If not specified, all the tables will be registered. Use this when there are
                    few identifiers that need to be registered. For a large number of identifiers, use the `--identifiers-from-file` or
                    `--identifiers-regex` option.
      --identifiers-from-file=<identifiersFromFile>
                  optional text file path that contains a list of table identifiers (one per line) to register. Should not be used with
                    `--identifiers` or `--identifiers-regex` option.
      --identifiers-regex=<identifiersRegEx>
                  optional regular expression pattern used to register only the tables whose identifiers match this pattern. Should not be used with
                    `--identifiers` or '--identifiers-from-file' option.
      --dry-run   optional configuration to simulate the registration without actually registering. Can learn about a list of the tables that will be
                    registered by running this.
      --delete-source-tables
                  optional configuration to delete the table entry from source catalog after successfully registering it to target catalog.
      --output-dir=<outputDirPath>
                  optional local output directory path to write CLI output files like `failed_identifiers.txt`, `failed_to_delete_at_source.txt`,
                    `dry_run_identifiers.txt`. Uses the present working directory if not specified.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
```

> :warning: **It is recommended to use this CLI tool when there is no in-progress commits for the tables in the source catalog.**
In-progress commits may not make it into the target catalog if used.

> :warning: By default this tool just registers the table. 
Which means the table will be present in both the catalogs after registering.
Operating same table from more than one catalog can lead to missing updates, loss of data and table corruption. 
So, it is recommended to use the '--delete-source-tables' option in CLI to automatically delete the table from source catalog after registering 
or avoid operating tables from the source catalog after registering if '--delete-source-tables' option is not used.

# Sample Inputs
## Bulk migrating all the tables from Hadoop catalog to Nessie catalog (main branch)
```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--delete-source-tables
```

## Register all the tables from Hadoop catalog to Arctic catalog (main branch)

```shell
export PAT=xxxxxxx
export SECRETKEY=xxxxxxx
export ACCESSKEY=xxxxxxx
```

```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE \
--target-catalog-properties uri=https://nessie.test1.dremio.site/v1/repositories/8158e68a-5046-42c6-a7e4-c920d9ae2475,ref=main,warehouse=/tmp/warehouse,authentication.type=BEARER,authentication.token=$PAT \
--target-catalog-hadoop-conf fs.s3a.secret.key=$SECRETKEY,fs.s3a.access.key=$ACCESSKEY
```

## Migrate selected tables (t1,t2 in namespace foo) from Arctic catalog (main branch) to Hadoop catalog.

```shell
export PAT=xxxxxxx
export SECRETKEY=xxxxxxx
export ACCESSKEY=xxxxxxx
```

```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
--source-catalog-type NESSIE \
--source-catalog-properties uri=https://nessie.test1.dremio.site/v1/repositories/8158e68a-5046-42c6-a7e4-c920d9ae2475,ref=main,warehouse=/tmp/warehouse,authentication.type=BEARER,authentication.token=$PAT \
--target-catalog-type HADOOP \
--target-catalog-properties warehouse=/tmp/warehouse,type=hadoop --source-catalog-hadoop-conf fs.s3a.secret.key=$SECRETKEY,fs.s3a.access.key=$ACCESSKEY \
--identifiers foo.t1,foo.t2 \
--delete-source-tables
```

# Scenarios
## A. User need to try out new catalog
They can use a new catalog with a fresh table to explore the capabilities of the new catalog. 
No need for a catalog migration tool.

## B. Users need to move away from one catalog (example: Hadoop) to another (example: Nessie) with all the tables.

### B.1) uses a `--dry-run` option to see what all the tables will be migrated.

Sample input:
```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--dry-run
```

All the inputs will be validated and a list of identified table identifiers for migration will be printed on the console
along with the total count. Output will be written to _dry_run.txt_ file. 
which can be used for actual migration using the `--identifiers-from-file` option without listing tables again from the catalog.

### B.2) executes the migration of all 1000 tables and all the tables are successfully migrated.

Sample input:
```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--delete-source-tables
```

Once the input validations are done, users will be prompted with this message.
They have an ability to abort or continue the operation.

```
Configured source catalog: HADOOP

Configured target catalog: NESSIE

[WARNING]
a) Executing catalog migration when the source catalog has some in-progress commits
can lead to a data loss as the in-progress commit will not be considered for migration.
So, while using this tool please make sure there are no in-progress commits for the source catalog

b) After the migration, successfully migrated tables will be deleted from the source catalog 
and can only be accessed from the target catalog.
Have you read the above warnings and are you sure you want to continue? (yes/no):
```

Once the user wants to continue, other information will be printed on the console.

```
Continuing...

User has not specified the table identifiers. Selecting all the tables from all the namespaces from the source catalog.
Collecting all the namespaces from source catalog...
Collecting all the tables from all the namespaces of source catalog...

Identified 1000 tables for migration.
Started migration ...

Attempted Migration for 100 tables out of 1000
Attempted Migration for 200 tables out of 1000
.
.
.
Attempted Migration for 900 tables out of 1000
Attempted Migration for 1000 tables out of 1000

Finished migration ...

Summary:
- Successfully migrated 1000 tables from HADOOP catalog to NESSIE catalog.

Details:
- Successfully migrated these tables:
  [foo.tbl-1, foo.tbl-2, bar.tbl-4, bar.tbl-3, …, …,bar.tbl-1000]
```

Note: a log file will also be generated which prints “successfully migrated table X” on every table migration. It also captures table level failures if there are any.


### B.3) executes the migration and out of 1000 tables 10 tables have failed to migrate because the target catalog had the same table and namespace (maybe different schema).Remaining 990 tables were successfully migrated.

Sample input:
```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--delete-source-tables
```

Console output will be same as B.2) till summary because even in case of failure,
all the identified tables will be attempted for migration.

```
Summary:
- Successfully migrated 990 tables from HADOOP catalog to NESSIE catalog.
- Failed to migrate 10 tables from HADOOP catalog to NESSIE catalog. Please check the `catalog_migration.log` file for the failure reason.
  Failed Identifiers are written to `failed_identifiers.txt`. Retry with that file using the `--identifiers-from-file` option if the failure is because of network/connection timeouts.

Details:
- Successfully migrated these tables:
  [foo.tbl-1, foo.tbl-2, bar.tbl-4, bar.tbl-3, …, …,bar.tbl-1000]
- Failed to migrate these tables:
  [bar.tbl-201, foo.tbl-202, …, …,bar.tbl-210]
```

Note:
A log file will also be generated which prints “successfully migrated table X” on every table migration. It also captures table level failures if there are any.
So from the details or from _failed_identifiers.txt_ file, users can get the failed table names and search in the log. 
It will have a 10 stacktrace with `TableAlreadyExists` exception for 10 tables. Which gives an idea for the user about why it failed.
Users can rename the tables in the source catalog and migrate only these 10 tables by using any one of the identifier options in the argument.


### B.4) executes the migration and out of 1000 tables 900 tables have failed to migrate because the target/source catalog connection went off. Only 100 tables were successfully migrated.

Sample input:
```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--delete-source-tables
```

Console output will be same as B.2) till summary because even in case of failure,
all the identified tables will be attempted for migration.

```
Summary:
- Successfully migrated 100 tables from HADOOP catalog to NESSIE catalog.
- Failed to migrate 900 tables from HADOOP catalog to NESSIE catalog. Please check the `catalog_migration.log` file for the failure reason.
  Failed Identifiers are written to `failed_identifiers.txt`. Retry with that file using the `--identifiers-from-file` option if the failure is because of network/connection timeouts.

Details:
- Successfully migrated these tables:
  [foo.tbl-1, foo.tbl-2,…,bar.tbl-100]
- Failed to migrate these tables:
  [bar.tbl-201, foo.tbl-202, …, …,bar.tbl-1000]
```

Note:
A log file will also be generated which prints “successfully migrated table X” on every table migration. It also captures table level failures if there are any.
So from the details or from _failed_identifiers.txt_ file, users can get the failed table names and search in the log.
It will have a 900 stack trace with `ConnectionTimeOut` exception for 900 tables. Which gives an idea for the user about why it failed.
As these were timeout exceptions, users can retry migration of only these 900 tables using the `--identifiers-from-file` option with _failed_identifiers.txt_.

### B.5)  executes the migration and out of 1000 tables. Where all the 1000 tables were migrated successfully but deletion of 200 tables from the source catalog has failed due to network issues.

Sample input:
```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--delete-source-tables
```

Console output will be same as B.2) till summary because even in case of failure,
all the identified tables will be attempted for migration. 
These failed to delete tables are stored in _failed_to_delete.txt_ and the user has to delete them manually or stop using them from the source catalog. (console will print this warning)

```
Summary:
- Successfully migrated 1000 tables from HADOOP catalog to NESSIE catalog.
- 200 tables were failed to delete from the source catalog due the reason captured in the logs. These table names are written into the `failed_to_delete.txt` file. Do not operate these tables from the source catalog.

Details:
- Successfully migrated these tables:
  [foo.tbl-1, foo.tbl-2,…,bar.tbl-1000]
- [WARNING] Failed to delete these tables from source catalog:
  [bar.tbl-201, foo.tbl-202, …, …,bar.tbl-400]
```

Users should manually drop the table entry from the source catalog in this case or stop using these tables from the source catalog.


### B.6)  executes the migration and out of 1000 tables. But manually aborts the migration by killing the process.

User has to go through the log to figure out how many tables have migrated so far. 
Users can also do `listTables()` at the target catalog to see how many tables migrated. 
There can be a chance that tables that are migrated to the target catalog may not be cleaned in the source catalog due to abort. 
Users should not operate them from source catalog and can manually drop them from source catalog. 
Users can also try bulk migration again, which will attempt to migrate all the tables in the source catalog.

### B.7) Users need to move away from one catalog to another with selective tables (maybe want to move only the production tables, test tables, etc)

Users can provide the selective list of identifiers to migrate using any of these 3 options
`--identifiers`, `--identifiers-from-file`, `--identifier-regex` and it can be used along with the dry-run option too.

Sample input: (only migrate tables that starts with "foo.")
```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--delete-source-tables \
--identifiers-regex ^foo\..*
```

Sample input: (migrate all tables in the file ids.txt where each entry is delimited by newline)
```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--delete-source-tables \
--identifiers-from-file ids.txt
```

Sample input: (migrate only two tables foo.tbl1, foo.tbl2)
```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--delete-source-tables \
--identifiers foo.tbl1,foo.tbl2
```

Console will clearly print that only these identifiers are used for table migration.
Rest of the behavior will be the same as mentioned in the previous sections.
