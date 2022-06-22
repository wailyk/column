<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->
<a href="http://asterixdb.apache.org"><img src="http://asterixdb.apache.org/img/asterixdb_tm.png" height=100></img></a>

# Columnar Formats for Schemaless LSM-based Document Stores

This code was made available as expected for [VLDB](https://vldb.org/pvldb/vol15) publications.

The published code is based on Apache AsterixDB and contains modifications to support the columnar format *AMAX* 
as well as the code generation framework.

### This is only a prototype and a cleaner and a reviewed version will be made available soon at https://github.com/apache/asterixdb

## Installation Requirements

For easier installation experience, you first need to install the following:

* [Maven](https://maven.apache.org/download.cgi)
  </br> Apache Maven is needed to compile AsterixDB. You need Maven 3.3.9 or newer
* [Ansible](https://nightlies.apache.org/asterixdb/ansible.html)
  </br> The link above shows how to use Ansible to deploy AsterixDB cluster. Using Ansible would be the easiest way to
  use the code in this repository as one of Ansible's YAML files has been modified to accommodate using GraalVM.
* [GraalVM](https://www.graalvm.org/downloads/)
  </br> Speaking of which, you need to install GraalVM for the code generation part. If you have Java, it is probably
  HotSpotVM and not GraalVM. GraalVM is required to compile the generated codes from SQL++ queries. Using Java HotSpotVM
  means the generated code will be executed in interpreter-mode.
  </br> **Please download GraalVM v21.3 as this was the one we used and we know it works :-)**
  </br> **You only need to download and unzip GraalVM. You do NOT have to make it your default JVM.**

## Compiling and Configuring AsterixDB

After cloning this repo:

    $ git clone https://github.com/wailyk/column.git

Run the following commands:

    $ cd asterixdb
    $ mvn clean package -DskipTests

Now, copy the compiled AsterixDB code to your <code>HOME</code> folder:

    $ cp asterixdb/asterix-server/target/asterix-server-0.9.8-SNAPSHOT-binary-assembly.zip ~/

Unzip <code>asterix-server-0.9.8-SNAPSHOT-binary-assembly.zip</code>

    $ cd ~
    $ unzip asterix-server-0.9.8-SNAPSHOT-binary-assembly.zip

Good! You need now to modify one file. Using you favorite editor, open:

    apache-asterixdb-0.9.8-SNAPSHOT/opt/ansible/yaml/instance_start.yml

You need to change <code><PATH_TO_GRAALVM></code> to the location of GraalVM, for example:

      environment:
        JAVA_HOME: /home/wail/vms/graalvm-ce-java11-21.3.2

**Make sure you do that for the two commented YAML blocks (i.e., the one under <code>[ncs]</code>
and <code>[cc]</code>)**

Save and close `instance_start.yml`

## Running AsterixDB

run the following commands:

    $ cd ~/apache-asterixdb-0.9.8-SNAPSHOT/opt/ansible/bin
    $ ./deploy.sh

Make sure you can see the folder <code>asterixdb_column</code> under your <code>HOME</code> folder. This confirms that
AsterixDB was deployed successfully in your machine.

Start AsterixDB:

    $ ./start.sh

Using your favorite browser, open the following link: [http://localhost:19006](http://localhost:19006). You should see
AsterixDB's web console.

## Start using AsterixDB with Columnar format and Code Generation

### DDL

Perfect! You can now start using AsterixDB. Let's start to load some data and try a few queries. First, run the
following DDLs:

    DROP DATAVERSE ColumnTest IF EXISTS;
    CREATE DATAVERSE ColumnTest;
    USE ColumnTest;

    CREATE TYPE OpenType AS {
      uid: uuid
    };

    -- Create a Dataset using AsterixDB's default row format
    CREATE DATASET RowDataset (OpenType)
    PRIMARY KEY uid AUTOGENERATED;

    -- Create a Dataset using our proposed columnar format
    CREATE DATASET ColumnDataset (OpenType)
    PRIMARY KEY uid AUTOGENERATED WITH {
      -- Tells AsterixDB to store the records as columns
      "dataset-format":{"format":"column"}
    };

We provided a sample dataset with the code as well. It is a smaller version of the <code>*sensors*</code> dataset from
the paper. Let's load the data into the two declared datasets <code>RowDataset</code> and <code>ColumnDataset</code>:

    USE ColumnTest;
    LOAD DATASET RowDataset USING localfs (
      ("path" = "localhost:///<HOME>/apache-asterixdb-0.9.8-SNAPSHOT/opt/data/sensors.json"),
      ("format" = "json")
    );

    LOAD DATASET ColumnDataset USING localfs (
      ("path" = "localhost:///<HOME>/apache-asterixdb-0.9.8-SNAPSHOT/opt/data/sensors.json"),
      ("format" = "json")
    );

**Make sure to change `<HOME>` to the location of your `<HOME>` folder. For example, mine is `home/wail`. So the full path will be:

    localhost:///home/wail/apache-asterixdb-0.9.8-SNAPSHOT/opt/data/sensors.json


### QUERY

After loading the dataset, let us run the following queries:

#### Q1:

Row

    USE ColumnTest;
    SELECT VALUE COUNT(*)
    FROM RowDataset;

Column

    USE ColumnTest;
    SELECT VALUE COUNT(*)
    FROM ColumnDataset;

Column with Code Generation

    USE ColumnTest;
    SET `compiler.codegen` "true";
    SELECT VALUE COUNT(*)
    FROM ColumnDataset;

#### Q2:

Row

    USE ColumnTest;
    SELECT VALUE MAX(r.temp)
    FROM RowDataset s, s.readings r;

Column

    USE ColumnTest;
    SELECT VALUE MAX(r.temp)
    FROM ColumnDataset s, s.readings r;

Column with Code Generation

    USE ColumnTest;
    -- Enable code generation
    SET `compiler.codegen` "true";
    SELECT VALUE MAX(r.temp)
    FROM ColumnDataset s, s.readings r;

## Query Plans

From the web console, you can inspect the query plans. To have a detailed plan, change `PLAN FORMAT` to `STRING`.
The dropdown menu for changing the plan format is located on top of the query box (where you write queries). 

After executing a query, click `PLAN`, located in the result box, to show the query plan.

For example `Q2`'s plan with for the column + code generation
looks like:

    distribute result [$$45]
    -- DISTRIBUTE_RESULT  |UNPARTITIONED|
      exchange
      -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
        aggregate [$$45] <- [agg-global-sql-max($$48)]
        -- AGGREGATE  |UNPARTITIONED|
          exchange
          -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
             data-scan [$$48]<-[$$44, $$s, $$46, $$r, $$41, $$48] <- ColumnTest.ColumnDataset code >>
              01| //reader0: {readings:[{temp:any}]}
              02| function ColumnDataset0Func (cursor, resultWriter, reader0) {
              03|    var0 = NULL;
              04|    while (cursor.next()) {
              05|       reader0.next();
              06|       while (!reader0.isEndOfArray()) {
              07|          var1 = reader0.getValue();
              08|          var0 = var0 /\ var1;
              09|          reader0.next();
              10|       }
              11|    }
              12|    append(resultWriter, var0);
              13|    flush(resultWriter);
              14| }
    
              -- DATASOURCE_SCAN  |PARTITIONED|
              exchange
              -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                empty-tuple-source
                -- EMPTY_TUPLE_SOURCE  |PARTITIONED|


## Stopping/Removing AsterixDB
To stop the cluster, run:

    $ ./stop.sh

To delete the cluster, run:

    $ ./erase.sh

## AsterixDB's Documentation
For more information about AsterixDB, refer to [AsterixDB's Documentation](https://nightlies.apache.org/asterixdb/index.html)

## Asterix Internal Language (AIL)
The implementation of the language (AIL) used in our code generation is a fork 
from Truffle's [Simple Language](https://github.com/graalvm/simplelanguage)

## Issue?
Please email me (my email is on the paper :-)). Or you can open an issue here on GitHub.
Again, please remember this is unreviewed code. It is only a research prototype and NOT meant to be used for production.

## Community support

- __Users__</br>
  maling list: [users@asterixdb.apache.org](mailto:users@asterixdb.apache.org)</br>
  Join the list by sending an email
  to [users-subscribe@asterixdb.apache.org](mailto:users-subscribe@asterixdb.apache.org)</br>
- __Developers and contributors__</br>
  mailing list:[dev@asterixdb.apache.org](mailto:dev@asterixdb.apache.org)</br>
  Join the list by sending an email to [dev-subscribe@asterixdb.apache.org](mailto:dev-subscribe@asterixdb.apache.org)

