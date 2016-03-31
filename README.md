# Solr Mongo Importer
Welcome to the Solr Mongo Importer project. This project provides MongoDb support for the Solr Data Import Handler.

## Features
* Retrive data from a MongoDb collection
* Authenticate using MongoDb authentication
* Map Mongo fields to Solr fields
* Delta import available

## Classes

* MongoDataSource - Provides a MongoDb datasource
    * database (**required**) - The name of the data base you want to connect to
    * host     (*optional* - default: localhost)
    * port     (*optional* - default: 27017)
    * username (*optional*)
    * password (*optional*)
* MongoEntityProcessor - Use with the MongoDataSource to query a MongoDb collection
    * collection (**required**)
    * query (**required**)
    * deltaQuery (*optional*)
    * deltaImportQuery (*optional*)
* MongoMapperTransformer - Map MongoDb fields to your Solr schema
    * mongoField (**required**)

## Installation
1. Firstly you will need a copy of the Solr Mongo Importer jar.
    ### Getting Solr Mongo Importer
    1. [Download the latest JAR from github](https://github.com/james75/SolrMongoImporter/releases)
    2. Build your own using the ant build script you will need the JDK installed as well as Ant and Ivy
2. You will also need the [Mongo Java driver JAR]   (https://github.com/mongodb/mongo-java-driver/downloads)

3. Place both of these jar's in your Solr libaries folder ( I put mine in 'dist' folder with the other jar's)
4. Add lib directives to your solrconfig.xml

```xml
    <lib path="../../dist/solr-mongo-importer-{version}.jar" />
    <lib path="../../dist/mongo.jar" />
```

##Usage
Here is a sample data-config.xml showing the use of all components
```xml
<?xml version="1.0" encoding="UTF-8" ?>
<dataConfig>
     <dataSource name="MyMongo" type="MongoDataSource" database="Inventory" />
     <document name="Products">
         <entity processor="MongoEntityProcessor"
                 query="{'Active':1}"
                 collection="ProductData"
                 datasource="MyMongo"
                 deltaQuery="{'UpdateDate':{$gt:{$date:'${dih.last_index_time}'}}}"
                 deltaImportQuery="{'_id':'${dih.delta._id}'}"
                 transformer="MongoMapperTransformer" >
             <field column="title"           name="title"       mongoField="Title"/>
             <field column="description"     name="description" mongoField="Long Description"/>
             <field column="brand"           name="brand"  />
         </entity>
     </document>
 </dataConfig>
```
