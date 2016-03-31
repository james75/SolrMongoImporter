package org.apache.solr.handler.dataimport;


import com.mongodb.*;
import com.mongodb.util.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

/**
 * User: James
 * Date: 13/08/12
 * Time: 18:28
 * To change this template use File | Settings | File Templates.
 */


public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>> {

    private static final Logger LOG = LoggerFactory.getLogger(TemplateTransformer.class);

    private DBCollection mongoCollection;
    private DB mongoDb;
    private Mongo mongoConnection;

    private DBCursor mongoCursor;

    @Override
    public void init(Context context, Properties initProps) {
        String databaseName = initProps.getProperty(DATABASE);
        String host = initProps.getProperty(HOST, "localhost");
        String port = initProps.getProperty(PORT, "27017");
        String username = initProps.getProperty(USERNAME);
        String password = initProps.getProperty(PASSWORD);

        if (databaseName == null) {
            throw new DataImportHandlerException(SEVERE
                    , "Database must be supplied");
        }

        try {
            Mongo mongo = new Mongo(host, Integer.parseInt(port));
            mongo.setReadPreference(ReadPreference.secondaryPreferred());

            this.mongoConnection = mongo;
            this.mongoDb = mongo.getDB(databaseName);

            if (username != null) {
                if (this.mongoDb.authenticate(username, password.toCharArray()) == false) {
                    throw new DataImportHandlerException(SEVERE
                            , "Mongo Authentication Failed");
                }
            }

        } catch (UnknownHostException e) {
            throw new DataImportHandlerException(SEVERE
                    , "Unable to connect to Mongo");
        }
    }

    @Override
    public Iterator<Map<String, Object>> getData(String query) {

        DBObject queryObject = (DBObject) JSON.parse(query);
        LOG.debug("Executing MongoQuery: " + query.toString());

        long start = System.currentTimeMillis();
        mongoCursor = this.mongoCollection.find(queryObject);
        LOG.trace("Time taken for mongo :"
                + (System.currentTimeMillis() - start));

        ResultSetIterator resultSet = new ResultSetIterator(mongoCursor);
        return resultSet.getIterator();
    }

    public Iterator<Map<String, Object>> getData(String query, String collection) {
        this.mongoCollection = this.mongoDb.getCollection(collection);
        return getData(query);
    }

    private class ResultSetIterator {
        DBCursor MongoCursor;

        Iterator<Map<String, Object>> rSetIterator;

        public ResultSetIterator(DBCursor MongoCursor) {
            this.MongoCursor = MongoCursor;


            rSetIterator = new Iterator<Map<String, Object>>() {
                public boolean hasNext() {
                    return hasnext();
                }

                public Map<String, Object> next() {
                    return getARow();
                }

                public void remove() {/* do nothing */
                }
            };


        }

        public Iterator<Map<String, Object>> getIterator() {
            return rSetIterator;
        }

        private Map<String, Object> getARow() {
            DBObject mongoObject = getMongoCursor().next();

            Map<String, Object> result = new HashMap<String, Object>();
            Set<String> keys = mongoObject.keySet();
            Iterator<String> iterator = keys.iterator();


            while (iterator.hasNext()) {
                String key = iterator.next();
                Object innerObject = mongoObject.get(key);

                result.put(key, innerObject);
            }

            return result;
        }

        private boolean hasnext() {
            if (MongoCursor == null)
                return false;
            try {
                if (MongoCursor.hasNext()) {
                    return true;
                } else {
                    close();
                    return false;
                }
            } catch (MongoException e) {
                close();
                wrapAndThrow(SEVERE, e);
                return false;
            }
        }

        private void close() {
            try {
                if (MongoCursor != null)
                    MongoCursor.close();
            } catch (Exception e) {
                LOG.warn("Exception while closing result set", e);
            } finally {
                MongoCursor = null;
            }
        }
    }

    private DBCursor getMongoCursor() {
        return this.mongoCursor;
    }

    @Override
    public void close() {
        if (this.mongoCursor != null) {
            this.mongoCursor.close();
        }

        if (this.mongoConnection != null) {
            this.mongoConnection.close();
        }
    }


    public static final String DATABASE = "database";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

}

