# QueryTest

Contains examples of using the Couchbase Query API.

* Converting query results from Json to Pojo
* Working with results from a GROUP BY
* Getting [system information](http://docs.couchbase.com/developer/n1ql-dp4/topics/sysinfo.html)

This was developed using the following:

* Java 1.8
* [Couchbase Java Client 2.1.2](http://docs.couchbase.com/developer/java-2.1/java-intro.html)
* [GSON](https://code.google.com/p/google-gson/) 2.3.1
* Couchbase Server Version: 4.0.0-2213 DEV Edition (build-2213)


Related links:

* [Working with N1QL from Java](http://docs.couchbase.com/developer/java-2.1/querying-n1ql.html)

How to use:

1.  Create a Couchbase 4.0 cluster, one node is fine.  Be sure the cluster has the services Data, Index, and Query.

2.  Load the travel-sample bucket

3.  Create the primary index on the travel-sample bucket with cbq

        $ cd /opt/couchbase/bin
        $ ./cbq
        Couchbase query shell connected to http://localhost:8093/ . Type Ctrl-D to exit.
        cbq> create primary index on `travel-sample`;
        {
        "requestID": "de20a07f-9068-429b-b1de-18338302691b",
        "signature": null,
        "results": [
        ],
        "status": "success",
        "metrics": {
            "elapsedTime": "5.1873122s",
            "executionTime": "5.187164851s",
            "resultCount": 0,
            "resultSize": 0
        }
        }
        cbq>

4.  Put your cluster's host name or ip address into QueryTest.java
5.  Run the program

        =============== Welcome to QueryTest ===============
        About to open bucket...
        =============== Opened bucket travel-sample. ===============
        =============== About to get list of namespaces ===============
        The elapsed time of that query was:   7.98793ms
        The execution time of that query was: 7.869658ms
        Retrieved a list of namespaces and found that there are 1 namespaces.
       0 :    default
        =============== About to get list of datastores ===============
        The elapsed time of that query was:   1.439731ms
        The execution time of that query was: 1.281078ms
        Retrieved a list of datastores and found that there are 1 datastores.
       0 : http://127.0.0.1:8091
        =============== About to get list of airlines ===============
        The elapsed time of that query was:   2.228318389s
        The execution time of that query was: 2.225013034s
        Retrieved a list of airlines and found that there are 187 airlines.
        =============== About to get list of indexes ===============
        The elapsed time of that query was:   42.940193ms
        The execution time of that query was: 42.857211ms
        Name                  ID                   State           Keyspace        Using     
        --------------------  -------------------- --------------- --------------- ----------
                  #primary             #primary          online   travel-sample       view
        =============== About to get cardinality of the 'type' field ===============
        The elapsed time of that query was:   2.386003718s
        The execution time of that query was: 2.382465373s
        Key                        Value     
        -------------------------  ----------
                          route       24076
                        airline         187
                        airport        1968
                       landmark        5389
        Now leaving QueryTest.  Total run time was 5430ms. Goodbye.

