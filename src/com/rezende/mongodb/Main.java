/**
 * 
 */
package com.rezende.mongodb;

import java.net.UnknownHostException;
import java.sql.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcernException;

/**
 * @author Rafael Rezende
 * 
 */
public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// Connecting to MongoDB
		MongoClient mongo = null;
		try {
			mongo = new MongoClient("localhost", 27017);
		} catch (UnknownHostException e) {
			System.out.println("Error connecting to the database.");
			System.exit(1);
		}

		// Get a database (automatically create if it does not exist)
		DB db = mongo.getDB("sandbox");

		// Get a collection/table from sandbox
		// Collection is automatically created if it does not exist
		DBCollection collection = db.getCollection("alarm");

		// Create objects with key and value
		BasicDBObject alarm1 = new BasicDBObject();
		alarm1.put("_id", 0);
		alarm1.put("owner", 1);
		alarm1.put("priv", false);
		alarm1.put("instant", new Date(0));

		BasicDBObject alarm2 = new BasicDBObject();
		alarm2.put("_id", 1);
		alarm2.put("owner", 1);
		alarm2.put("priv", true);
		alarm2.put("instant", new Date(0));

		// Insert object into the collection
		collection.insert(alarm1, alarm2);

		// TODO How does the auto-increment work in mongoDB?

		/*
		 * BasicDBObject is also used as reference for searching objects. The
		 * query will match the objects that have the same values as the
		 * reference one. This way...
		 */
		BasicDBObject alarmSearch = new BasicDBObject("_id", 0);

		// To perform the search for a specific object
		DBCursor cursor = collection.find(alarmSearch);

		// TODO What is the element does not exist?
		// TODO How to retrieve ALL the data?

		// Print results
		System.out.println("\nPrint object with id = 0");
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}

		// To replace just one value, use $set
		BasicDBObject oldAlarm = new BasicDBObject("_id", 1);
		BasicDBObject updatedAlarm = new BasicDBObject("owner", 999);
		BasicDBObject updaterAlarm = new BasicDBObject("$set", updatedAlarm);
		collection.update(oldAlarm, updaterAlarm);

		// To repeat the search and print
		System.out.println("\nPrint all objects");
		cursor = collection.find();
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}

		// To increment a value from an existing object, use $inc
		oldAlarm = new BasicDBObject("_id", 1);
		updatedAlarm = new BasicDBObject("owner", 1);
		updaterAlarm = new BasicDBObject("$inc", updatedAlarm);
		collection.update(oldAlarm, updaterAlarm);

		// To repeat the search and print
		System.out.println("\nPrint all objects");
		cursor = collection.find();
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}

		/*
		 * MongoDB does not allow changes on the document ID So, the commented
		 * code actually raises an exception for this.
		 */
		oldAlarm = new BasicDBObject("_id", 1);
		BasicDBObject newAlarm = new BasicDBObject("_id", 3);
		try {
			collection.update(oldAlarm, newAlarm);
		} catch (WriteConcernException e) {
			System.out.println("\nERROR: " + e.getMessage());
		}

		// To repeat the search and print
		System.out.println("\nPrint all objects");
		cursor = collection.find();
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}

		// TODO What is many objects match the provide key?

		// To delete an existing object
		collection.remove(oldAlarm);
		
		// To repeat the search and print
		System.out.println("\nPrint all objects");
		cursor = collection.find();
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}

		// To delete all objects
		cursor = collection.find();
		while (cursor.hasNext()) {
			DBObject dbObject = (DBObject) cursor.next();
			collection.remove(dbObject);
		}

		// TODO Is there any straightforward command to clean the
		// collection?

	}
}
