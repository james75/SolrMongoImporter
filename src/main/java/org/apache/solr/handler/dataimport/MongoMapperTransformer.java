package org.apache.solr.handler.dataimport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

/**
 * 
 * @author justin.spies
 *
 */
@SuppressWarnings({ "PMD.ModifiedCyclomaticComplexity", "PMD.StdCyclomaticComplexity",
    "PMD.CyclomaticComplexity" })
public class MongoMapperTransformer extends Transformer {
  private static final Logger LOG = LoggerFactory.getLogger(MongoMapperTransformer.class);

  @Override
  @SuppressWarnings({ "PMD.AvoidInstantiatingObjectsInLoops", "PMD.CommentRequired" })
  public Object transformRow(final Map<String, Object> row, final Context context) {
    LOG.debug("Transforming row: " + row);

    BSONObject obj = null;
    Object document = null;
    for (final Map<String, String> field : context.getAllEntityFields()) {
      if (obj == null) {
        obj = new BasicBSONObject(row);
        try {
          LOG.debug("Parsing " + obj.toString() + " as JSON");
          document = Configuration.defaultConfiguration().jsonProvider().parse(obj.toString());

          LOG.debug("Resulting JSON for " + obj.toString() + " is " + document);
        } catch (InvalidJsonException e) {
          LOG.warn("Invalid row found: " + row);
        }
      }

      Object buf;
      final String jsonPath = field.get(JSONPATH);
      if (jsonPath != null && document != null) {
        try {
          buf = JsonPath.read(document, jsonPath);
        } catch (PathNotFoundException e) {
          buf = row.get(field.get(DataImporter.COLUMN));
        }

        if (buf instanceof ObjectId) {
          buf = ((ObjectId) buf).toHexString();
        } else if (buf instanceof List && ((List<?>) buf).getClass().equals(ObjectId.class)) {
          final List<String> list = new ArrayList<String>();
          for (final Object e : (List<?>) buf) {
            list.add(((ObjectId) e).toHexString());
          }
          buf = list;
        }
      } else {
        buf = row.get(field.get(DataImporter.COLUMN));
        if (buf instanceof ObjectId) {
          buf = ((ObjectId) buf).toHexString();
        }
      }
      row.put(field.get(DataImporter.COLUMN), buf);
    }

    LOG.debug("Row to be returned is " + row);
    return row;
  }

  public static final String JSONPATH = "jsonpath";
}