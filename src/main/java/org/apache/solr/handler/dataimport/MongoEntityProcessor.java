package org.apache.solr.handler.dataimport;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.CommentRequired")
public class MongoEntityProcessor extends EntityProcessorBase {
  private static final Logger LOG = LoggerFactory.getLogger(MongoEntityProcessor.class);
  
  protected MongoDataSource dataSource;
  private String collection;

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.solr.handler.dataimport.EntityProcessorBase#init(org.apache.solr.handler.dataimport.
   * Context)
   */
  @Override
  public void init(final Context context) {
    LOG.debug("init() - start");
    
    super.init(context);
    this.collection = context.getEntityAttribute(COLLECTION);
    LOG.info("Using " + this.collection + " for Mongo queries");
    
    if (this.collection == null) {
      throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
          "Collection name must be supplied");
    }
    
    LOG.debug("Returning new MongoDataSource object");
    this.dataSource = (MongoDataSource) context.getDataSource();
  }

  /**
   * 
   * @param query
   */
  protected void initQuery(final String query) {
    LOG.debug("initQuery(String) - start");
    
    this.query = query;
    LOG.debug("Running query " + query);
    
    DataImporter.QUERY_COUNT.get().incrementAndGet();
    this.rowIterator = this.dataSource.getData(query, this.collection);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.dataimport.EntityProcessorBase#nextRow()
   */
  @Override
  public Map<String, Object> nextRow() {
    LOG.debug("nextRow() - start");
    
    if (rowIterator == null) {
      if (Context.FULL_DUMP.equals(context.currentProcess())) {
        LOG.debug("Getting the next row for a full data import");
        initQuery(context.replaceTokens(context.getEntityAttribute(QUERY)));
      }
      if (Context.DELTA_DUMP.equals(context.currentProcess())) {
        LOG.debug("Getting the next row for a delta data import");
        initQuery(context.replaceTokens(context.getEntityAttribute(DELTA_QUERY)));
      }
    }
    return this.getNext();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.dataimport.EntityProcessorBase#nextDeletedRowKey()
   */
  @Override
  public Map<String, Object> nextDeletedRowKey() {
    if (rowIterator == null) {
      initQuery(context.replaceTokens(context.getEntityAttribute(DELETED_QUERY)));
    }
    return this.getNext();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.dataimport.EntityProcessorBase#nextModifiedParentRowKey()
   */
  @Override
  public Map<String, Object> nextModifiedParentRowKey() {
    if (rowIterator == null) {
      initQuery(context.replaceTokens(context.getEntityAttribute(MODIFIED_PARENT_QUERY)));
    }
    return getNext();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.solr.handler.dataimport.EntityProcessorBase#nextModifiedRowKey()
   */
  @Override
  public Map<String, Object> nextModifiedRowKey() {
    if (rowIterator == null) {
      initQuery(context.replaceTokens(context.getEntityAttribute(MODIFIED_QUERY)));
    }
    return getNext();
  }

  public static final String QUERY = "query";
  public static final String DELTA_QUERY = "deltaQuery";
  public static final String DELETED_QUERY = "deletedQuery";
  public static final String MODIFIED_PARENT_QUERY = "modifiedParentQuery";
  public static final String MODIFIED_QUERY = "modifiedQuery";
  public static final String COLLECTION = "collection";
}