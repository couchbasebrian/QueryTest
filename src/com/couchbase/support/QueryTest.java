// QueryTest.java
//
// Brian Williams 
// April 14, 2015
//
// Example of using Couchbase Query from Java using 2.1.2 SDKs, available from here:
//       http://packages.couchbase.com/clients/java/2.1.2/Couchbase-Java-Client-2.1.2.zip
//
// Couchbase Java Client 2.1.2 javadoc is here:
//       http://docs.couchbase.com/sdk-api/couchbase-java-client-2.1.2/ 
//
// A good Couchbase Java Query reference is here:  
//		http://docs.couchbase.com/developer/java-2.1/querying-n1ql.html
// 
// Developed with Java SDK 1.8

package com.couchbase.support;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryMetrics;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryRow;
import com.google.gson.Gson;


class IndexInfo {

	String keyspace_id, using, namespace_id, datastore_id, id, state, name;
	String[] index_key;
	
	// Example of index info
	//		"index_key":[],
	//		"keyspace_id" :"gamesim-sample",
	//		"using"       :"gsi",
	//		"namespace_id":"default",
	//		"datastore_id":"http://127.0.0.1:8091",
	//		"name"        :"#primary",
	//		"id"          :"ddb6dc705166b195",
	//		"state"       :"online"
	
}

public class QueryTest {

	static final int SCREENCOLUMNS = 132;	// adjust to fit your terminal
	
	public static void printDecoration(int c, String s) {
		for (int i = 0; i < c; i++) { System.out.print(s); }
	}
	
	public static void printCenteredBanner(String s) {
		int numDecorations = ((SCREENCOLUMNS - (s.length() + 2)) / 2);
		printDecoration(numDecorations,"=");
		System.out.print(" " + s + " ");
		printDecoration(numDecorations,"=");		
		System.out.println();
	}
	
	// Get a list of namespaces from the system metadata
	public static List<QueryRow> getListOfNamespaces(Bucket b) {
		return queryOnSystem(b, "namespaces");
	}

	// Get a list of datastores from the system metadata
	public static List<QueryRow> getListOfDatastores(Bucket b) {
		return queryOnSystem(b, "datastores");
	}

	// This is for looking for certain types of system metadata
	// such as system:namespaces or system:datastores
	public static List<QueryRow> queryOnSystem(Bucket b, String subsystem) {
		return queryToListOfQueryRow(b,"SELECT * FROM system:"+ subsystem);
	}

	// This method assumes that the airline documents contain a certain 
	// attribute, "type", and that the value of that attribute is "airline".
	public static List<QueryRow> getListOfAirlines(Bucket b) {
		return getListByType(b, "airline");
	}
	
	// Assuming the document contain an attribute called type, this lets
	// you query the bucket for all documents of a certain type.
	public static List<QueryRow> getListByType(Bucket b, String typeValue) {		
		return queryToListOfQueryRow(b,"SELECT * FROM `" + b.name() + "` where type=\"" + typeValue + "\";");
	}

	// convenience method.  Given a bucket and a query string, which is assumed to be
	// a SELECT, execute the query, print timing information, and return the results
	// to the caller, not as a QueryResult, but as a list of QueryRows which the 
	// caller can iterate over and do things with.
	public static List<QueryRow> queryToListOfQueryRow(Bucket b, String queryString) {
		QueryResult queryResult = b.query(Query.simple(queryString));
		List<QueryRow> rval     = queryResult.allRows();
		QueryMetrics   qm       = queryResult.info();
		System.out.println("The elapsed time of that query was:   " + qm.elapsedTime());
		System.out.println("The execution time of that query was: " + qm.executionTime());
		return rval;
	}
	
	// Given a simple String and Integer Hashtable, print it out
	public static void printHashtable(Hashtable<String,Integer> siHashtable) {

		System.out.println("Key                        Value     ");
		System.out.println("-------------------------  ----------");
		
		Set<String> keyList = siHashtable.keySet();
		
		for (String eachKey : keyList) {
			System.out.printf("%25s  %10d\n", eachKey, siHashtable.get(eachKey));
		}
		
	}
	
	public static Hashtable<String,Integer> queryRowListToHashtable(List<QueryRow> listOfQueryRow, String keyColName, String valColName) {
		// Given query results and the name of the key column and the name of the
		// value column, create and return a Hashtable containing the keys and 
		// values
		Hashtable<String,Integer> h = new Hashtable<String, Integer>();

		String eachKey = null;
		int eachValue  = 0;
		
		JsonObject eachJsonObject = null;
		
		for (QueryRow qr : listOfQueryRow) {
			eachJsonObject = qr.value();
			eachKey = eachJsonObject.getString(keyColName);
			eachValue = eachJsonObject.getInt(valColName);
			h.put(eachKey, eachValue);
		}
			
		return h;
	}
	
	// Given a list of query rows which are known to contain index info, use Gson to
	// create the POJOs and put them into a list and return the list.
	public static List<IndexInfo> getIndexInfoListFromQueryRowList(List<QueryRow> listOfQueryRow) {
		
		List<IndexInfo> rval = new ArrayList<IndexInfo>();
		Gson gson = new Gson();
		IndexInfo eachIndexInfo = null;
		String iiString;
		JsonObject eachIndexInfoJsonObject;
		
		for (QueryRow qr : listOfQueryRow) {
			eachIndexInfoJsonObject = qr.value().getObject("indexes");	
			iiString = eachIndexInfoJsonObject.toString();
			// System.out.println("each index info string is:" + iiString);
			eachIndexInfo = gson.fromJson(iiString, IndexInfo.class);
			rval.add(eachIndexInfo);
		}

		return rval;
		
	}
	
	// Given a list of IndexInfo POJOs, print them out
	public static void printIndexInfoList(List<IndexInfo> indexInfoList) {

		System.out.println("Name                  ID                   State           Keyspace       ");
		System.out.println("--------------------  -------------------- --------------- ---------------");
		
		for (IndexInfo ii : indexInfoList) {
			System.out.printf("%20s %20s %15s %15s\n", ii.name, ii.id, ii.state, ii.keyspace_id);
		}
			
	}
	
	// Given a bucket, get a list of objects containing the information about the 
	// indexes in the bucket
	public static List<IndexInfo> getIndexInfoList(Bucket b) {
		 return getIndexInfoListFromQueryRowList(queryOnSystem(b, "indexes"));
	}
		
	// Given a list of query rows, print the raw json for each
	public static void printRawJson(List<QueryRow> listOfQueryRow) {
		JsonObject eachJsonObject = null;
		int i = 0;
		
		for (QueryRow qr : listOfQueryRow) {
			eachJsonObject = qr.value();
			System.out.printf("%2d : %10s\n", i, eachJsonObject);			
			i++;
		}
		
	}	
	
	// Assumes that each of the items in the list is a QueryRow
	// whose value is a JsonObject, and that that JsonObject contains
	// an attribute called "namespaces", which then contains another JsonObject
	// that has an attribute called "name"
	public static void printListOfNamespaces(List<QueryRow> namespaceList) {
		JsonObject eachJsonObject = null;
		JsonObject eachNamespaceObject = null;
		int i = 0;
		String eachNamespaceName = null;
		
		for (QueryRow qr : namespaceList) {
			eachJsonObject = qr.value();
			eachNamespaceObject = eachJsonObject.getObject("namespaces");
			eachNamespaceName = eachNamespaceObject.getString("name");
			System.out.printf("%2d : %10s\n", i, eachNamespaceName);			
			i++;
		}
		
	}

	// Same as above but makes different assumptions about the JsonObject
	// contained in each QueryRow in the list
	public static void printListOfDatastores(List<QueryRow> datastoreList) {
		JsonObject eachJsonObject = null;
		JsonObject eachDatastoreObject = null;
		int i = 0;
		String eachDatastoreId = null;
		
		for (QueryRow qr : datastoreList) {
			eachJsonObject = qr.value();
			eachDatastoreObject = eachJsonObject.getObject("datastores");
			eachDatastoreId = eachDatastoreObject.getString("id");
			System.out.printf("%2d : %10s\n", i, eachDatastoreId);			
			i++;
		}
		
	}
	
	// main() method for this class
	public static void main(String[] args) {

		long t1 = 0, t2 = 0;
		
		t1 = System.currentTimeMillis();
		
		printCenteredBanner("Welcome to QueryTest");
		
		final String hostName   = "192.168.46.101";
		final String bucketName = "travel-sample";
		
		Cluster cluster = null; 
		
		try {
			cluster = CouchbaseCluster.create(hostName);
		}
		catch (Exception e) {
			System.err.println("Exception while creating connection to Couchbase cluster at " + hostName + ".");
			System.exit(1);
		}
	
		if (cluster == null) {
			System.err.println("Failed to create a connection to Couchbase cluster at " + hostName + ".");
			System.exit(1);
		}
				
		System.err.println("About to open bucket...");
		
		Bucket bucket = null;

		try {
			bucket = cluster.openBucket(bucketName);
			
		}
		catch (Exception e) {
			System.err.println("Failed to open bucket.");
			System.exit(1);
		}

		printCenteredBanner("Opened bucket " + bucketName + ".");

		printCenteredBanner("About to get list of namespaces");			
		List<QueryRow> namespaceList = getListOfNamespaces(bucket);		
		System.out.println("Retrieved a list of namespaces and found that there are " + namespaceList.size() + " namespaces.");
		printListOfNamespaces(namespaceList);

		printCenteredBanner("About to get list of datastores");			
		List<QueryRow> datastoreList = getListOfDatastores(bucket);		
		System.out.println("Retrieved a list of datastores and found that there are " + datastoreList.size() + " datastores.");
		printListOfDatastores(datastoreList);

		printCenteredBanner("About to get list of airlines");			
		List<QueryRow> listOfAirlines = getListOfAirlines(bucket);
		System.out.println("Retrieved a list of airlines and found that there are " + listOfAirlines.size() + " airlines.");
		
		printCenteredBanner("About to get list of indexes");	
		printIndexInfoList(getIndexInfoList(bucket));		// example of using GSON

		printCenteredBanner("About to get cardinality of the 'type' field");	
		List<QueryRow> groupByResults = queryToListOfQueryRow(bucket, "select type,count(1) as count from `travel-sample` group by type;");
		Hashtable<String,Integer> resultsHashtable = queryRowListToHashtable(groupByResults, "type", "count");
		printHashtable(resultsHashtable);
		
		// printRawJson(queryToListOfQueryRow(bucket, "select type,count(1) as count from `travel-sample` group by type;"));
												
		// TODO:  How does performance change during a rebalance?
		// TODO:  How does performance change when an index is created?

		t2 = System.currentTimeMillis();

		System.out.println("Now leaving QueryTest.  Total run time was " + (t2 - t1 ) +  "ms. Goodbye.");

	}

}

// Example of an airline type document:
//
// {
//   "id": 10,
//   "type": "airline",
//   "name": "40-Mile Air",
//   "iata": "Q5",
//   "icao": "MLA",
//   "callsign": "MILE-AIR",
//   "country": "United States"
// }


// Example of a group by query from cbq:
//
// cbq> select type,count(1) as total from `travel-sample` group by type;
// {
//    "requestID": "cb122297-3489-4356-aa92-6457b4b28532",
//    "signature": {
//        "total": "number",
//        "type": "json"
//    },
//    "results": [
//        {
//            "total": 187,
//            "type": "airline"
//        },
//        {
//            "total": 5389,
//            "type": "landmark"
//        },
//        {
//            "total": 24076,
//            "type": "route"
//        },
//        {
//            "total": 1969,
//            "type": "airport"
//        }
//    ],
//    "status": "success",
//    "metrics": {
//        "elapsedTime": "34.330865212s",
//        "executionTime": "34.262024547s",
//        "resultCount": 4,
//        "resultSize": 271
//    }
// }

// EOF