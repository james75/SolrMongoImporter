package org.apache.solr.handler.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

public class MongoEntityProcessor extends EntityProcessorBase {
    private static final Logger LOG = LoggerFactory.getLogger(EntityProcessorBase.class);
    private static final String QUERY = "query";
    private static final String DELTA_QUERY = "deltaQuery";
    private static final String DELTA_IMPORT_QUERY = "deltaImportQuery";
    private static final String DEL_PK_QUERY = "deletedPkQuery";
    private static final String COLLECTION = "collection";
    protected MongoDataSource dataSource;
    private String collection;

    @Override
    public void init(Context context) {
        super.init(context);

        this.collection = context.getEntityAttribute(COLLECTION);

        if (this.collection == null) {
            throw new DataImportHandlerException(SEVERE,
                    "Collection must be supplied");
        }

        this.dataSource = (MongoDataSource) context.getDataSource();
    }

    protected void initQuery(String q) {
        try {
            DataImporter.QUERY_COUNT.get().incrementAndGet();
            rowIterator = dataSource.getData(q, this.collection);
            this.query = q;
        } catch (DataImportHandlerException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("The query failed '" + q + "'", e);
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE, e);
        }
    }

    @Override
    public Map<String, Object> nextRow() {
        if (rowIterator == null) {
            query = this.getQuery();
            initQuery(context.replaceTokens(query));
        }

        return getNext();
    }

    @Override
    public Map<String, Object> nextModifiedRowKey() {
        if (rowIterator == null) {
            String deltaQuery = context.getEntityAttribute(DELTA_QUERY);

            if (deltaQuery == null) {
                return null;
			}

            initQuery(context.replaceTokens(deltaQuery));
        }

        return getNext();
    }

    @Override
    public Map<String, Object> nextDeletedRowKey() {
        if (rowIterator == null) {
            String deletedPkQuery = context.getEntityAttribute(DEL_PK_QUERY);

            if (deletedPkQuery == null) {
                return null;
			}

            initQuery(context.replaceTokens(deletedPkQuery));
        }

        return getNext();
    }

    @Override
    public Map<String, Object> nextModifiedParentRowKey() {
        if (this.rowIterator == null) {
            String parentDeltaQuery = this.context.getEntityAttribute("parentDeltaQuery");

            if (parentDeltaQuery == null) {
                return null;
            }

            LOG.info("Running parentDeltaQuery for Entity: " + this.context.getEntityAttribute("name"));

            this.initQuery(this.context.replaceTokens(parentDeltaQuery));
        }

        return this.getNext();
    }

    public String getQuery() {
        String queryString = this.context.getEntityAttribute(QUERY);

        if ("FULL_DUMP".equals(this.context.currentProcess())) {
            return queryString;
        } else if ("DELTA_DUMP".equals(this.context.currentProcess())) {
            return this.context.getEntityAttribute(DELTA_IMPORT_QUERY);
        }

        return null;
    }
}