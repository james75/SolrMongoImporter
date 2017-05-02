package org.apache.solr.handler.dataimport;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author justin.spies
 *
 */
public class DataSourceTest {
  private static final Logger LOG = LoggerFactory.getLogger(DataSourceTest.class);

  /**
   * 
   * @param args
   */
  public static void main(final String[] args) {
    final MongoDataSource dataSource = new MongoDataSource();
    
    final Properties props = new Properties();
    props.put(MongoDataSource.URI, "mongodb://mongo-01.west.us.dev.bq-s.com:27017,mongo-02.west.us.dev.bq-s.com:27017/bd_bqsn_p2p6");
    props.put(MongoDataSource.DATABASE, "bd_bqsn_p2p6");
    final Context cxt = null;
    dataSource.init(cxt, props);

    // final String query = "{'increment_id': {$ne: null}, 'type_code': 'INV'}";
    String query = "{'instance_id': {$eq: 'bd_bqsn_p2p6'} }";
    outputResults(dataSource.getData(query, "document"));

    query = "";
    outputResults(dataSource.getData(query, "document"));
  }
  
  private static void outputResults(final Iterator<Map<String, Object>> data) {
    int limiter = 0;
    while (data.hasNext() && limiter < 50) {
      final Map<String, Object> value = data.next();
      for (final Entry<String, Object> entry : value.entrySet()) {
        LOG.info("Key is " + entry.getKey() + ", value is " + entry.getValue().toString());
      }
      limiter++;
    }
  }
}
