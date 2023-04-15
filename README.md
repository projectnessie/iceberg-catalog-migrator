# Objective 
Introduce a command-line tool that enables bulk migration of Iceberg tables from one catalog to another without the need to copy the data.

There are various reasons why users may want to move their Iceberg tables to a different catalog. For instance,
* They were using hadoop catalog and later realized that it is not production recommended. So, they want to move tables to other production ready catalogs.
* They just heard about the awesome Arctic catalog (or Nessie) and want to move their existing iceberg tables to Dremio Arctic.
* They had an on-premise Hive catalog, but want to move tables to a cloud-based catalog as part of their cloud migration strategy.

The CLI tool should support two commands
* migrate - To bulk migrate the iceberg tables from source catalog to target catalog without data copy. 
Table entries from source catalog will be deleted after the successful migration to the target catalog.
* register - To bulk register the iceberg tables from source catalog to target catalog without data copy. 

> :warning: `register` command just registers the table.
Which means the table will be present in both the catalogs after registering.
**Operating same table from more than one catalog can lead to missing updates, loss of data and table corruption.
So, it is recommended to use the 'migrate' command in CLI to automatically delete the table from source catalog after registering
or avoid operating tables from the source catalog after registering if 'migrate' command is not used.**

> :warning: **It is recommended to use this CLI tool when there is no in-progress commits for the tables in the source catalog.
In-progress commits may not make it into the target catalog if used. Which can lead to missing updates, loss of data and table corruption.**

# Iceberg-catalog-migrator
Need to have Java installed in your machine(JDK11 is recommended) to use this CLI tool.

Below is the CLI syntax:
```
$ java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar -h        
Usage: iceberg-catalog-migrator [-hV] [COMMAND]
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  migrate   Bulk migrate the iceberg tables from source catalog to target catalog without data copy. Table entries from source catalog will be
              deleted after the successful migration to the target catalog.
  register  Bulk register the iceberg tables from source catalog to target catalog without data copy.
```

```
$ java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar migrate -h
Usage: iceberg-catalog-migrator migrate [-hV] [--disable-safety-prompts] [--dry-run] [--stacktrace] [--output-dir=<outputDirPath>]
                                        (--source-catalog-type=<type> --source-catalog-properties=<String=String>[,<String=String>...]
                                        [--source-catalog-properties=<String=String>[,<String=String>...]]...
                                        [--source-catalog-hadoop-conf=<String=String>[,<String=String>...]]...
                                        [--source-custom-catalog-impl=<customCatalogImpl>]) (--target-catalog-type=<type>
                                        --target-catalog-properties=<String=String>[,<String=String>...] [--target-catalog-properties=<String=String>
                                        [,<String=String>...]]... [--target-catalog-hadoop-conf=<String=String>[,<String=String>...]]...
                                        [--target-custom-catalog-impl=<customCatalogImpl>]) [--identifiers=<identifiers>[,<identifiers>...]
                                        [--identifiers=<identifiers>[,<identifiers>...]]... | --identifiers-from-file=<identifiersFromFile> |
                                        --identifiers-regex=<identifiersRegEx>]
Bulk migrate the iceberg tables from source catalog to target catalog without data copy. Table entries from source catalog will be deleted after the
successful migration to the target catalog.
      --output-dir=<outputDirPath>
                     Optional local output directory path to write CLI output files like `failed_identifiers.txt`, `failed_to_delete_at_source.txt`,
                       `dry_run_identifiers.txt`. If not specified, uses the present working directory.
                     Example: --output-dir /tmp/output/
                              --output-dir $PWD/output_folder
      --dry-run      Optional configuration to simulate the registration without actually registering. Can learn about a list of tables that will be
                       registered by running this.
      --disable-safety-prompts
                     Optional configuration to disable safety prompts which needs console input.
      --stacktrace   Optional configuration to enable capturing stacktrace in logs in case of failures.
  -h, --help         Show this help message and exit.
  -V, --version      Print version information and exit.
Source catalog options:
      --source-catalog-type=<type>
                     Source catalog type. Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST].
                     Example: --source-catalog-type GLUE
                              --source-catalog-type NESSIE
      --source-catalog-properties=<String=String>[,<String=String>...]
                     Iceberg catalog properties for source catalog (like uri, warehouse, etc).
                     Example: --source-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouseNessie
      --source-catalog-hadoop-conf=<String=String>[,<String=String>...]
                     Optional source catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when using an Iceberg FileIO.
                     Example: --source-catalog-hadoop-conf fs.s3a.secret.key=$SECRETKEY,fs.s3a.access.key=$ACCESSKEY
      --source-custom-catalog-impl=<customCatalogImpl>
                     Optional fully qualified class name of the custom catalog implementation of the source catalog. Required when the catalog type
                       is CUSTOM.
                     Example: --source-custom-catalog-impl org.apache.iceberg.AwesomeCatalog
Target catalog options:
      --target-catalog-type=<type>
                     Target catalog type. Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST].
                     Example: --target-catalog-type GLUE
                              --target-catalog-type NESSIE
      --target-catalog-properties=<String=String>[,<String=String>...]
                     Iceberg catalog properties for target catalog (like uri, warehouse, etc).
                     Example: --target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouseNessie
      --target-catalog-hadoop-conf=<String=String>[,<String=String>...]
                     Optional target catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when using an Iceberg FileIO.
                     Example: --target-catalog-hadoop-conf fs.s3a.secret.key=$SECRETKEY,fs.s3a.access.key=$ACCESSKEY
      --target-custom-catalog-impl=<customCatalogImpl>
                     Optional fully qualified class name of the custom catalog implementation of the target catalog. Required when the catalog type
                       is CUSTOM.
                     Example: --target-custom-catalog-impl org.apache.iceberg.AwesomeCatalog
Identifier options:
      --identifiers=<identifiers>[,<identifiers>...]
                     Optional selective set of identifiers to register. If not specified, all the tables will be registered. Use this when there are
                       few identifiers that need to be registered. For a large number of identifiers, use the `--identifiers-from-file` or
                       `--identifiers-regex` option.
                     Example: --identifiers foo.t1,bar.t2
      --identifiers-from-file=<identifiersFromFile>
                     Optional text file path that contains a set of table identifiers (one per line) to register. Should not be used with
                       `--identifiers` or `--identifiers-regex` option.
                     Example: --identifiers-from-file /tmp/files/ids.txt
      --identifiers-regex=<identifiersRegEx>
                     Optional regular expression pattern used to register only the tables whose identifiers match this pattern. Should not be used
                       with `--identifiers` or '--identifiers-from-file' option.
                     Example: --identifiers-regex ^foo\..*
```

Note: Options for register command is exactly same as migrate command.

# Sample Inputs
## Bulk migrating all the tables from Hadoop catalog to Nessie catalog (main branch)
```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar migrate \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse
```

## Register all the tables from Hadoop catalog to Arctic catalog (main branch)

```shell
export PAT=xxxxxxx
export AWS_ACCESS_KEY_ID=xxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxx
export AWS_S3_ENDPOINT=xxxxxxx
```

```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar register \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE \
--target-catalog-properties uri=https://nessie.test1.dremio.site/v1/repositories/8158e68a-5046-42c6-a7e4-c920d9ae2475,ref=main,warehouse=/tmp/warehouse,authentication.type=BEARER,authentication.token=$PAT
```

## Migrate selected tables (t1,t2 in namespace foo) from Arctic catalog (main branch) to Hadoop catalog.

```shell
export PAT=xxxxxxx
export AWS_ACCESS_KEY_ID=xxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxx
export AWS_S3_ENDPOINT=xxxxxxx
```

```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar migrate \
--source-catalog-type NESSIE \
--source-catalog-properties uri=https://nessie.test1.dremio.site/v1/repositories/8158e68a-5046-42c6-a7e4-c920d9ae2475,ref=main,warehouse=/tmp/warehouse,authentication.type=BEARER,authentication.token=$PAT \
--target-catalog-type HADOOP \
--identifiers foo.t1,foo.t2
```

## Migrate all tables from GLUE catalog to Arctic catalog (main branch)
```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar migrate \
--source-catalog-type GLUE \
--source-catalog-properties warehouse=s3a://some-bucket/wh/,io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--target-catalog-type NESSIE \
--target-catalog-properties uri=https://nessie.test1.dremio.site/v1/repositories/612a4560-1178-493f-9c14-ab6b33dc31c5,ref=main,warehouse=s3a://some-other-bucket/wh/,io-impl=org.apache.iceberg.aws.s3.S3FileIO,authentication.type=BEARER,authentication.token=$PAT
```

## Migrate all tables from HIVE catalog to Arctic catalog (main branch)
```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar migrate \
--source-catalog-type HIVE \
--source-catalog-properties warehouse=s3a://some-bucket/wh/,io-impl=org.apache.iceberg.aws.s3.S3FileIO,uri=thrift://localhost:9083 \
--target-catalog-type NESSIE \
--target-catalog-properties uri=https://nessie.test1.dremio.site/v1/repositories/612a4560-1178-493f-9c14-ab6b33dc31c5,ref=main,warehouse=s3a://some-other-bucket/wh/,io-impl=org.apache.iceberg.aws.s3.S3FileIO,authentication.type=BEARER,authentication.token=$PAT
```

## Migrate all tables from DYNAMODB catalog to Arctic catalog (main branch)
```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar migrate \
--source-catalog-type DYNAMODB \
--source-catalog-properties warehouse=s3a://some-bucket/wh/,io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--target-catalog-type NESSIE \
--target-catalog-properties uri=https://nessie.test1.dremio.site/v1/repositories/612a4560-1178-493f-9c14-ab6b33dc31c5,ref=main,warehouse=s3a://some-other-bucket/wh/,io-impl=org.apache.iceberg.aws.s3.S3FileIO,authentication.type=BEARER,authentication.token=$PAT
```

## Migrate all tables from JDBC catalog to Arctic catalog (main branch)
```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar migrate \ 
--source-catalog-type JDBC \
--source-catalog-properties warehouse=/tmp/warehouseJdbc,jdbc.user=root,jdbc.password=pass,uri=jdbc:mysql://localhost:3306/db1,name=catalogName \
--target-catalog-type NESSIE \
--target-catalog-properties uri=https://nessie.test1.dremio.site/v1/repositories/612a4560-1178-493f-9c14-ab6b33dc31c5,ref=main,warehouse=/tmp/nessiewarehouse,authentication.type=BEARER,authentication.token=$PAT
```

# Scenarios
## A. User need to try out new catalog
Users can use a new catalog by creating a fresh table to test the new catalog's capabilities, without requiring a tool to migrate the catalog.

## B. Users need to move away from one catalog (example: Hadoop) to another (example: Nessie) with all the tables.

### B.1) uses a `--dry-run` option to see what all the tables will be migrated.

Sample input:
```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar register \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--dry-run
```

After validating all inputs, the console will display a list of table identifiers that have been identified for migration along with the total count. 
This information will also be written to a file called `dry_run.txt`, 
which can be used for actual migration using the `--identifiers-from-file` option, thus eliminating the need to list tables from the catalog again.

### B.2) executes the migration of all 1000 tables and all the tables are successfully migrated.

Sample input:
```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar migrate \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse
```

After input validation, users will receive a prompt message with the option to either abort or continue the operation.

```
WARN  - User has not specified the table identifiers. Will be selecting all the tables from all the namespaces from the source catalog.
INFO  - Configured source catalog: SOURCE_CATALOG_HADOOP
INFO  - Configured target catalog: TARGET_CATALOG_NESSIE
WARN  - 
	a) Executing catalog migration when the source catalog has some in-progress commits 
	can lead to a data loss as the in-progress commits will not be considered for migration. 
	So, while using this tool please make sure there are no in-progress commits for the source catalog.

	b) After the migration, successfully migrated tables will be deleted from the source catalog 
	and can only be accessed from the target catalog.
INFO  - Are you certain that you wish to proceed, after reading the above warnings? (yes/no):
```

If the user chooses to continue, additional information will be displayed on the console.

```
INFO  - Continuing...
INFO  - Identifying tables for migration ...
INFO  - Identified 1000 tables for migration.
INFO  - Started migration ...
INFO  - Attempted Migration for 100 tables out of 1000 tables.
INFO  - Attempted Migration for 200 tables out of 1000 tables.
.
.
.
INFO  - Attempted Migration for 900 tables out of 1000 tables.
INFO  - Attempted Migration for 1000 tables out of 1000 tables.
INFO  - Finished migration ...
INFO  - Summary:
INFO  - Successfully migrated 1000 tables from HADOOP catalog to NESSIE catalog.
INFO  - Details:
INFO  - Successfully migrated these tables:
[foo.tbl-1, foo.tbl-2, bar.tbl-4, bar.tbl-3, …, …,bar.tbl-1000]
```

Please note that a log file will be created, which will print "successfully migrated table X" for every table migration, 
and also log any table level failures, if present.

### B.3) executes the migration and out of 1000 tables 10 tables have failed to migrate because of some error. Remaining 990 tables were successfully migrated.

Sample input:
```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar migrate \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--stacktrace
```

Console output will be same as B.2) till summary because even in case of failure,
all the identified tables will be attempted for migration.

```
INFO  - Summary:
INFO  - Successfully migrated 990 tables from HADOOP catalog to NESSIE catalog.
ERROR - Failed to migrate 10 tables from HADOOP catalog to NESSIE catalog. Please check the `catalog_migration.log` file for the failure reason.
Failed Identifiers are written to `failed_identifiers.txt`. Retry with that file using the `--identifiers-from-file` option if the failure is because of network/connection timeouts.
INFO  - Details:
INFO  - Successfully migrated these tables:
[foo.tbl-1, foo.tbl-2, bar.tbl-4, bar.tbl-3, …, …,bar.tbl-1000]
ERROR  - Failed to migrate these tables:
[bar.tbl-201, foo.tbl-202, …, …,bar.tbl-210]
```

Please note that a log file will be generated, which will print "successfully migrated table X" for every table migration and log any table-level failures in the `failed_identifiers.txt` file.
Users can use this file to identify failed tables and search for them in the log, which will contain the exception stacktrace for those 10 tables. 
This can help users understand why the migration failed. 
* If the migration of those tables failed with `TableAlreadyExists` exception, users can rename the tables in the source catalog and migrate only those 10 tables using any of the identifier options available in the argument.
* If the migration of those tables failed with `ConnectionTimeOut` exception, users can retry migrating only those 10 tables using the `--identifiers-from-file` option with the `failed_identifiers.txt` file.
* If the migration is successful but deletion of some tables form source catalog is failed, summary will mention that these table names were written into the `failed_to_delete.txt` file and logs will capture the failure reason.
Do not operate these tables from the source catalog and user will have to delete them manually.

### B.4)  executes the migration and out of 1000 tables. But manually aborts the migration by killing the process.

To determine the number of migrated tables, the user can either review the log or use the listTables() function in the target catalog. 
In the event of an abort, migrated tables may not be deleted from the source catalog, and users should avoid manipulating them from there. 
If necessary, users can manually remove these tables from the source catalog or attempt a bulk migration to transfer all tables from the source catalog.

### B.5) Users need to move away from one catalog to another with selective tables (maybe want to move only the production tables, test tables, etc)

Users can provide the selective list of identifiers to migrate using any of these 3 options
`--identifiers`, `--identifiers-from-file`, `--identifier-regex` and it can be used along with the dry-run option too.

Sample input: (only migrate tables that starts with "foo.")
```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar migrate \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--identifiers-regex ^foo\..*

```

Sample input: (migrate all tables in the file ids.txt where each entry is delimited by newline)
```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar migrate \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--identifiers-from-file ids.txt
```

Sample input: (migrate only two tables foo.tbl1, foo.tbl2)
```shell
java -jar iceberg-catalog-migrator-cli-0.1.0-SNAPSHOT.jar migrate \
--source-catalog-type HADOOP \
--source-catalog-properties warehouse=/tmp/warehouse,type=hadoop \
--target-catalog-type NESSIE  \
--target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--identifiers foo.tbl1,foo.tbl2
```

Console will clearly print that only these identifiers are used for table migration.
Rest of the behavior will be the same as mentioned in the previous sections.