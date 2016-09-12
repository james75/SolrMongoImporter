package org.apache.solr.handler.dataimport;


import com.mongodb.*;
import com.mongodb.util.JSON;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import org.bson.conversions.Bson;

public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>> {
    private static final Logger LOG = LoggerFactory.getLogger(TemplateTransformer.class);
    private MongoCollection mongoCollection;
    private MongoDatabase mongoDatabase;
	private MongoClient mongoClient;
    private FindIterable findIterable;
    private static final String DATABASE = "database";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    @Override
    public void init(Context context, Properties initProps) {
        String databaseName = initProps.getProperty(DATABASE);
        String host = initProps.getProperty(HOST, "localhost");
        String port = initProps.getProperty(PORT, "27017");
		int portInt;
        String userName = initProps.getProperty(USERNAME);
        String password = initProps.getProperty(PASSWORD);

        if (databaseName == null) {
            throw new DataImportHandlerException(SEVERE, "Database must be supplied");
        }

		try {
			portInt = Integer.parseInt(port);
		} catch (NumberFormatException e) {
			LOG.error("The '" + PORT + "' property is not a valid integer. It is '" + port + ". Defaulting to 27017.", e);
			portInt = 27017;
		}

		if (userName != null) {
			// Manage the mongo db connection...
			List<ServerAddress> seeds = new ArrayList<ServerAddress>();
			seeds.add( new ServerAddress(host, portInt));
			List<MongoCredential> credentials = new ArrayList<MongoCredential>();
			credentials.add(
				MongoCredential.createScramSha1Credential(
					userName,
					databaseName,
					password.toCharArray()
				)
			);

			this.mongoClient = new MongoClient( seeds, credentials );
		} else {
			this.mongoClient = new MongoClient(host , portInt);
		}

		this.mongoClient.setReadPreference(ReadPreference.secondaryPreferred());

		this.mongoDatabase = mongoClient.getDatabase(databaseName);
    }

    @Override
    public Iterator<Map<String, Object>> getData(String query) {

        Bson queryObject = (Bson) JSON.parse(query);
        LOG.debug("Executing MongoQuery: " + query);

        long start = System.currentTimeMillis();
        findIterable = this.mongoCollection.find(queryObject);
        LOG.trace("Time taken for mongo :" + (System.currentTimeMillis() - start));

        return findIterable.iterator();
    }

    public Iterator<Map<String, Object>> getData(String query, String collection) {
        this.mongoCollection = this.mongoDatabase.getCollection(collection);
        return getData(query);
    }

    @Override
    public void close() {
		if (this.findIterable != null && this.findIterable.iterator() != null) {
			this.findIterable.iterator().close();
		}

        if (this.mongoClient != null) {
            this.mongoClient.close();
        }
    }
}

