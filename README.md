# catalog-migrator
A CLI tool to bulk migrate Iceberg tables from one catalog to another without a data copy.

Need to have java installed in your machine(JDK11 or later version) to use this CLI tool.

Below is the CLI syntax:
```
$ java -jar iceberg-catalog-migrator-0.1.0-SNAPSHOT.jar --help
Usage: register [-hV] [--delete-source-tables] [--identifiers-from-file=<identifiersFromFile>]
                [--source-custom-catalog-impl=<sourceCustomCatalogImpl>] [-T=<maxThreadPoolSize>]
                [--target-custom-catalog-impl=<targetCustomCatalogImpl>] [-I=<identifiers>[,<identifiers>...]]...
                [--source-catalog-hadoop-conf=<String=String>[,<String=String>...]]... [--target-catalog-hadoop-conf=<String=String>[,
                <String=String>...]]... <sourceCatalogType> [<String=String>[,<String=String>...]] <targetCatalogType> [<String=String>[,
                <String=String>...]]

Bulk register the iceberg tables from source catalog to target catalog without data copy.

      <sourceCatalogType>   source catalog type. Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]
      [<String=String>[,<String=String>...]]
                            source catalog properties
      <targetCatalogType>   target catalog type. Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]
      [<String=String>[,<String=String>...]]
                            target catalog properties
      --source-catalog-hadoop-conf=<String=String>[,<String=String>...]
                            optional source catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when using an Iceberg
                              FileIO.
      --target-catalog-hadoop-conf=<String=String>[,<String=String>...]
                            optional target catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when using an Iceberg
                              FileIO.
  -I, --identifiers=<identifiers>[,<identifiers>...]
                            optional selective list of identifiers to register. If not specified, all the tables will be registered.Use this when
                              there are few identifiers needs to be registered. For large number of identifiers, use `--identifiers-from-file` option.
      --identifiers-from-file=<identifiersFromFile>
                            optional text file path that contains list of table identifiers (one per line) to register. Should not be used with
                              `--identifiers` option.
  -T, --thread-pool-size=<maxThreadPoolSize>
                            optional size of the thread pool used for register tables. Tables are migrated sequentially if not specified.
      --source-custom-catalog-impl=<sourceCustomCatalogImpl>
                            optional fully qualified class name of the custom catalog implementation of the source catalog. Required when the catalog
                              type is CUSTOM.
      --target-custom-catalog-impl=<targetCustomCatalogImpl>
                            optional fully qualified class name of the custom catalog implementation of the target catalog. Required when the catalog
                              type is CUSTOM.
      --delete-source-tables
                            Optional configuration to delete the tables entry from source catalog after successfully registering it to target catalog.
  -h, --help                Show this help message and exit.
  -V, --version             Print version information and exit.
```

> :warning: By default this tool just registers the table. 
Which means the table will be present in both the catalogs after registering.
Operating same table from more than one catalog can lead to missing updates, loss of data and table corruption. 
So, it is recommended to use the '--delete-source-tables' option in CLI to automatically delete the table from source catalog after registering 
or avoid operating tables from the source catalog after registering if '--delete-source-tables' option is not used.

> :warning: **It is recommended to use this CLI tool when there is no in-progress commits for the tables in the source catalog.**
In-progress commits may not make it into the target catalog if used.

### Example command for bulk migrating tables between Hadoop catalog and Arctic catalog

```shell
export PAT=xxxxxxx
export SECRETKEY=xxxxxxx
export ACCESSKEY=xxxxxxx
```

##### Register all the tables from Hadoop catalog to Arctic catalog (main branch)

```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
HADOOP \
warehouse=/tmp/warehouse,type=hadoop \
NESSIE \
uri=https://nessie.test1.dremio.site/v1/repositories/8158e68a-5046-42c6-a7e4-c920d9ae2475,ref=main,warehouse=/tmp/warehouse,authentication.type=BEARER,authentication.token=$PAT \
--target-catalog-hadoop-conf fs.s3a.secret.key=$SECRETKEY,fs.s3a.access.key=$ACCESSKEY
```

##### Migrate selected tables (t1,t2 in namespace foo) from Arctic catalog (main branch) to Hadoop catalog.
```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
NESSIE \
uri=https://nessie.test1.dremio.site/v1/repositories/8158e68a-5046-42c6-a7e4-c920d9ae2475,ref=main,warehouse=/tmp/warehouse,authentication.type=BEARER,authentication.token=$PAT \
HADOOP \
warehouse=/tmp/warehouse,type=hadoop --source-catalog-hadoop-conf fs.s3a.secret.key=$SECRETKEY,fs.s3a.access.key=$ACCESSKEY \
-I foo.t1,foo.t2 \
--delete-source-tables
```

### Example command for bulk migrating tables from Hadoop catalog to Nessie catalog (main branch)
```shell
java -jar catalog-migration-tool-1.0-SNAPSHOT.jar \
HADOOP \
warehouse=/tmp/warehouse,type=hadoop \
NESSIE  \
uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouse \
--delete-source-tables
```
