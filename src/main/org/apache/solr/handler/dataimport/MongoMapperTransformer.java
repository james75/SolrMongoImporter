package org.apache.solr.handler.dataimport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoMapperTransformer extends Transformer {
	private static final Logger LOG = LoggerFactory.getLogger(Transformer.class);
    public static final String MONGO_FIELD = "mongoField";

    @Override
    public Object transformRow(Map<String, Object> row, Context context) {

        for (Map<String, String> map : context.getAllEntityFields()) {
			LOG.info("MongoMapperTransformer transformRow called.");
            String mongoFieldName = map.get(MONGO_FIELD);
            if (mongoFieldName == null)
                continue;

            String columnFieldName = map.get(DataImporter.COLUMN);

			LOG.info("mongoFieldName: " + mongoFieldName + ", columnFieldName: " + columnFieldName);

			Object fieldValue = row.get(mongoFieldName);

			// If BasicDBList/array then flatten for Solr. See example input/output below.
			if (fieldValue != null && fieldValue instanceof ArrayList) {
				ArrayList mongoFieldValueList = (ArrayList)fieldValue;

				for (int i = 0; i < mongoFieldValueList.size(); i++) {
					Object mongoFieldValue = mongoFieldValueList.get(i);

					if (mongoFieldValue != null && mongoFieldValue instanceof Document) {
						Document mongoDBObject = (Document)mongoFieldValue;
						String tempKey;

						for (String key : mongoDBObject.keySet()) {
							Object tempObj = mongoDBObject.get(key);
							// Merge the field name and key together while uppercasing the first letter of the key.
							// i.e. image:priority becomes imagePriority
							tempKey = columnFieldName + key.substring(0, 1).toUpperCase() + key.substring(1);

							if (row.containsKey(tempKey)) {
								List<Object> tempArrayList = (ArrayList<Object>)row.get(tempKey);
								tempArrayList.add(tempObj);
								row.put(tempKey, tempArrayList );
							} else {
								LOG.info("Adding key: " + tempKey);
								List<Object> tempArrayList = new ArrayList<Object>();
								tempArrayList.add(tempObj);
								row.put(tempKey, tempArrayList);
							}
						}
					}					
				}
			} else {
				row.put(columnFieldName, row.get(mongoFieldName));
			}

			row.remove(mongoFieldName);
        }

        return row;
    }

/*
	INPUT from Mongo. BasicDBList/JSON array 
	"images" : [ {
		"priority" : 2,
		"caption" : "The Katherine 6-Light Geometric Chandelier has a more rugged look than some of the dainty chandeliers out there.",
		"photo" : "/resources/images/dti/2016/06/HL_katherine061216_17343781.jpg",
		"credit" : "Ballard Designs "
	}, {
		"priority" : 1,
		"caption" : "Modern Forms&#8217; Marimba Pendant Chandelier, in gold leaf and bronze or silver leaf and white, can dim to 10 percent with a dimmer switch ($659, 2modern.com). ",
		"photo" : "/resources/images/dti/2016/06/Hl_marimbasilver061216_17343782.jpg",
		"credit" : "2modern "
	} ]

	OUTPUT from transformRow method above.
	"imagePriority": [
		2,
		1
	],
	"imageCaption": [
		"The Katherine 6-Light Geometric Chandelier has a more rugged look than some of the dainty chandeliers out there.",
		"Modern Forms’ Marimba Pendant Chandelier, in gold leaf and bronze or silver leaf and white, can dim to 10 percent with a dimmer switch ($659, 2modern.com). "
	],
	"imagePhoto": [
		"/resources/images/dti/2016/06/HL_katherine061216_17343781.jpg",
		"/resources/images/dti/2016/06/Hl_marimbasilver061216_17343782.jpg"
	],
	"imagePhoto": [
		"/resources/images/dti/2016/06/HL_katherine061216_17343781.jpg",
		"/resources/images/dti/2016/06/Hl_marimbasilver061216_17343782.jpg"
	]
*/
}