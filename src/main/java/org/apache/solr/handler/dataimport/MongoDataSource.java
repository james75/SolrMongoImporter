package org.apache.solr.handler.dataimport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * User: James Date: 13/08/12 Time: 18:28
 */
public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>> {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDataSource.class);

  private MongoDatabase database;
  private MongoClient client;
  private MongoCollection<DBObject> mongoCollection;
  private MongoCursor<DBObject> mongoCursor;

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.dataimport.DataSource#init(org.apache.solr.handler.dataimport.Context,
   * java.util.Properties)
   */
  @Override
  @SuppressWarnings({ "PMD.CommentRequired", "PMD.AvoidDuplicateLiterals" })
  public void init(final Context context, final Properties initProps) {
    LOG.debug("init() - start");

    resolveVariables(context, initProps);

    final String databaseName = initProps.getProperty(DATABASE);
    LOG.debug("Using databaseName of " + databaseName);

    // The database name parameter is required, throw
    if (StringUtils.isEmpty(databaseName)) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
          "The " + DATABASE + " parameter is required by was not specified");
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Initialization properties:");
      for (@SuppressWarnings("rawtypes")
      final Map.Entry entry : initProps.entrySet()) {
        LOG.debug("  - " + entry.getKey() + " = " + entry.getValue());
      }
    }
    this.client = this.getClient(initProps);
    this.database = this.client.getDatabase(databaseName)
        .withReadPreference(ReadPreference.secondaryPreferred());

    LOG.debug("init() - end");
  }

  /**
   * Builds and returns the {@link MongoClient} used to connect to the Mongo instances specified in
   * {@code initProps} parameter. Supports seeding the client with muliple instances via
   * the use of a Mongo JDBC style URI.
   * 
   * @param initProps the initialization properties passed in from the SOLR DIH
   * @return the {@link MongoClient} to be used when querying the Mongo data store
   */
  public MongoClient getClient(final Properties initProps) {
    LOG.debug("getClient() - start");
    final String databaseName = initProps.getProperty(DATABASE);

    // Default to using a URL, which allows for all kinds of Mongo configurations including
    // standalone, replica sets, shards, etc.
    if (initProps.containsKey(URI)
        && !StringUtils.isEmpty(initProps.getProperty(URI))) {

      // Build the URI used to connect to Mongo
      final String uri = initProps.getProperty(URI);

      LOG.debug("Returning getClientFromURI()");
      return this.getClientFromURI(uri);

    } else {
      // Assume a single server is being used; if nothing is specified, then assume it is a
      // developer running Mongo on localhost.
      final String host = initProps.getProperty(HOST, "localhost");
      final Integer port = Integer.valueOf(initProps.getProperty(PORT, "27017"));
      final String username = initProps.getProperty(USERNAME);
      final String password = initProps.getProperty(PASSWORD);

      LOG.debug("Returning getClient() using host, port, datbaseName, username, and password");
      return this.getClient(host, port, databaseName, username, password);
    }
  }

  /**
   * Establish a connection to one or more Mongo servers using the {@link MongoClientURI} object.
   * 
   * @param uri the standard URI (JDBC style) to be used when connecting to the Mongo systems
   * @return a new {@link MongoClient} connection
   */
  private MongoClient getClientFromURI(final String uri) {
    LOG.debug("getClientFromURI() - start");

    if (StringUtils.isEmpty(uri)) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
          "Invalid Mongo URI specified when attempting connection");
    }
    // No explicit check for authentication as best practice is for the credentials to be part of
    // the URI. Additionally, the read preference should be part of the URI as well.
    final MongoClientURI clientUri = new MongoClientURI(uri);

    LOG.debug("Creating and returning new MongoClient() using " + uri);
    return new MongoClient(clientUri);
  }

  /**
   * Builds a new {@link MongoClient} connection using a single hostname, port and optional
   * credentials. The database name is only necessary when the username is specified, otherwise the
   * database name can be null.
   * 
   * @param host the mongo host to which the connection is made
   * @param port the port on which the Mongo host runs
   * @param database the name of the database to which the initial connection is made; only required
   *          if a username and password are specified; used to construct the
   *          {@link MongoCredential} object
   * @param username used when authentication is required; used to construct the
   *          {@link MongoCredential} object
   * @param password used when authentication is required; used to construct the
   *          {@link MongoCredential} object
   * @return a new {@link MongoClient} connection
   */
  private MongoClient getClient(final String host, final Integer port, final String database,
      final String username, String password) {

    LOG.debug("getClientFromURI(host, port, database, username, password) - start");

    final ServerAddress address = new ServerAddress(host, port);
    final List<MongoCredential> credentials = new ArrayList<MongoCredential>();
    if (!StringUtils.isEmpty(username)) {
      LOG.info("Authenticating to " + database + " as " + username);
      // Cannot call toCharArray() on a null object.
      if (password == null) {
        LOG.info("null value set for the password parameter, setting to an empty string");
        password = "";
      }
      LOG.debug("Configuring the MongoCredential list");
      credentials.add(MongoCredential.createCredential(username, database, password.toCharArray()));
    }

    LOG.debug("Creating and returning new MongoClient() for " + host + ":" + port.toString());
    return new MongoClient(address, credentials);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.dataimport.DataSource#getData(java.lang.String)
   */
  @Override
  @SuppressWarnings("PMD.CommentRequired")
  public Iterator<Map<String, Object>> getData(final String query) {
    LOG.debug("getData(String) - start");

    if (StringUtils.isEmpty(query)) {
      LOG.debug("Returning all documents in the collection");
      return new BsonDocumentRowIterator(this.mongoCollection.find().iterator());
    } else {
      LOG.debug("Executing query " + query);
      final Document doc = Document.parse(query);

      LOG.trace("Returning iterator()");
      return new BsonDocumentRowIterator(this.mongoCollection.find(doc).iterator());
    }
  }

  /**
   * Retrieves data from the specified collection using the specified query. If the query is null or
   * an empty string, then all objects in the collection are returned.
   * 
   * @param query the Mongo query to be executed
   * @param collection the name of the collection in the database from which results are pulled
   * @return an {@link Iterator<Document>} object containing the results of the query operation
   */
  public Iterator<Map<String, Object>> getData(final String query, final String collection) {
    LOG.debug("Executing Query: " + query + " on collection: " + collection);
    this.mongoCollection = this.database.getCollection(collection, DBObject.class);
    return this.getData(query);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.dataimport.DataSource#close()
   */
  @Override
  @SuppressWarnings("PMD.CommentRequired")
  public void close() {
    if (this.mongoCursor != null) {
      this.mongoCursor.close();
    }

    if (this.client != null) {
      this.client.close();
    }
  }

  /**
   * 
   * @author justin.spies
   *
   */
  private class BsonDocumentRowIterator implements Iterator<Map<String, Object>> {
    private Iterator<DBObject> bsonDocumentIterator;

    /**
     * 
     * @param bsonDocumentIterator
     */
    public BsonDocumentRowIterator(final Iterator<DBObject> bsonDocumentIterator) {
      this.bsonDocumentIterator = bsonDocumentIterator;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    @Override
    @SuppressWarnings("PMD.CommentRequired")
    public boolean hasNext() {
      return bsonDocumentIterator.hasNext();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    @SuppressWarnings("PMD.CommentRequired")
    public Map<String, Object> next() {
      LOG.debug("next() - start");

      final DBObject mongoObject = this.bsonDocumentIterator.next();

      final Map<String, Object> result = new HashMap<String, Object>();
      final Set<String> keys = mongoObject.keySet();
      final Iterator<String> iterator = keys.iterator();

      while (iterator.hasNext()) {
        final String key = iterator.next();
        final Object innerObject = mongoObject.get(key);

        result.put(key, innerObject);
      }

      LOG.debug("Returning the resulting Map<String, Object>");
      return result;
    }
  }

  private void resolveVariables(Context ctx, Properties initProps) {
    for (Map.Entry<Object, Object> entry : initProps.entrySet()) {
      if (entry.getValue() != null) {
        entry.setValue(ctx.replaceTokens((String) entry.getValue()));
      }
    }
  }
  
  public static final String DATABASE = "database";
  public static final String HOST = "host";
  public static final String PORT = "port";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String URI = "uri";

}
