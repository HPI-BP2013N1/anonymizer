
This is an Anonymizer for Relational Databases
==============================================

What for?
---------
If you want to give other people insights into your database but can't release your sensitive your original data, you can do this with the help of this project.


What should you provide?
------------------------
* 3 databases:
    + original database = database to anonymize
    + translation database = database for translation tables used for pseudonymizing (should be empty unless it is already filled with translations you want to use)
    + anonymized database = database for the anonymized results (will be cleared before filling)
* Configuration File (for details see below)
* Scope File (for details see below)
 

Implemented Functionalities
---------------------------
We provide several mechanisms to anonymize or pseudonymize your data.

First, there is an *Analyser*. This component needs two lists: one with tables and attributes that somehow should be changed (*Config*); one with all tables, that should considered to use (*Scope*). See below for details how the Config needs to be structured. The Analyser then runs though your database to collect dependencies of the listed attributes. Afterwards he delivers you an updated Config. Here, you can now have a look at the found dependencies and decide which ones you want to be considered for the following anonymization. 

The following strategies for anonymizing your data are implemented:
* __SetDefaultStrategy__
    It provides a mechanism to delete atrributes. Alternatively, you can set an attribute on a fixed value if you want to keep the "overall existance", but don't want to release the concrete occurences. Use the additional info of *Config* for defining the fixed value.

* __PseudonomizeStrategy__
    This method allows you to pseudonomize attributes. It gives every distinct value an own new pseudonym.  You can predefine prefixes, e.g. if you want to keep the "smell" of an attribute. For example: If you want to pseudonomize names, you can use "name" as prefix and the strategy will fill the remaining positions with pseudonyms. Use the additional info of *Config* to define prefixes.

* __CharacterStrategy__
    Here, you can pseudonymize your attributes characterwise. You can decide which character position you want to keep and which one to pseudonymize. Use the additional info of *Config* for specify the character pattern. Please notice: "K" stands for keeping these characters, "P" for pseudonymizing those others.
    
* __DeleteRowStrategy__
    If you want to make sure that certain tuples are definitely not (not even anonymized) within the result database you can choose them to be deleted. Write it similar to SQL where-clause, but without "where". Use the additional info of *Config* for this.

* __RetainRowStrategy__
    You can ensure that certain tuples shell be kept. Discribe the row you want to be retained similar to SQL where-clause, but without "where". Use the additional info of *Config* for this.

* __UniformDistributionStrategy__
    This strategy allows you to transform the distribution of an attribute into an equipartition if you want to keep the original distribution of an attribute secret.

Special attention is needed for:
*UniformDistributionStrategy*
You must take other columns into consideration to not reveal the original cardinalities. For example, if there is an entry date column in the same table and you know that all categories are loaded in daily bunches you might deduce that a category with later starting dates was larger than another category with earlier dates.

*RetainRowStrategy*
Be careful with the order of the applied strategies when using also the RetainRowStrategy. You should put a retain rule first because it won't work if the values have already been transformed before the RetainRowStrategy is applied. (The retain comparison is always done against the original row via ResultSetRowReader.)


Supported Environments
----------------------
* JAVA SE 1.7
* DB2 or H2 (Several other databases might be possible but ar not testet yet. As long as your database does not need special SQL support it should work. Have a look at the SQLHelper-Classes if you are in doubt.)
* Maven (You can import the Anonymizer as a Maven Project via m2e into your eclipse.)


Dependencies
------------
* google guava 17.0
* for testing:
    + junit
    + h2
    + mockito-core
    + hamcrest-library
    + dbunit


Config - the Configuration File
-------------------------------
Here, you can see an example for the *Config* file (please stick to the shown formatting:

    # originalDB newDB transformationDB each with username password
    jdbc:h2:mem:original name pw
    jdbc:h2:mem:destination name pw
    jdbc:h2:mem:transformations name pw
    # schema name and batch size
    ORIGINAL 10000
    
    - Default: de.hpi.bp2013n1.anonymizer.SetDefaultStrategy
    - P: de.hpi.bp2013n1.anonymizer.PseudonymizeStrategy
    - S: de.hpi.bp2013n1.anonymizer.CharacterStrategy
    - U: de.hpi.bp2013n1.anonymizer.UniformDistributionStrategy
    - Retain: de.hpi.bp2013n1.anonymizer.RetainRowStrategy
    
    # Table.Field		Type		AdditionalInfo
    VISITOR           Retain   SURNAME = 'Michael'
    VISITOR.SURNAME	P
        VISIT.VISITORSURNAME
    VISITOR.BIRTHDATE	S	KKKKPPPP
        VISIT.VISITORBIRTHDATE
    VISITOR.ZIPCODE	P
    VISITOR.ADDRESS	Default
        #CINEMA.ADDRESS
        #VISIT.CINEMAADDRESS
    CINEMA.COMPANY	P	CinemaCompany
        VISIT.CINEMACOMPANY
        #GREATMOVIES.COMPANY
    CINEMA.ADDRESS	P	Address
        VISIT.CINEMAADDRESS
        #VISITOR.ADDRESS
    VISIT.MOVIE	    P	Movie
        GREATMOVIES.MOVIE
    PRODUCTSALES.PRODUCTID	U

Lines that begin with "#" are comments and ignored during anonymization.

At first, you need to define the connections to the three needed databases.
Then specify the schema of the used tables and a batchsize.
Now, you can define an abbreviation for the wanted strategies. Add the belonging url.

"Table.Field" is used to list the attributes affected through the Anonymizer. Keep in mind: #<Tabel.Field> is treated as comment and ignored during anonymization.
"Type" defines the used strategy for the transformation concerning this attribute.
"AdditionalInfo" allows you to specify pattern (for CharacterStrategy), prefixes (for PseudonymizeStrategy), conditions (for RetainRow- and DeleteRowStrategy) etc. as defined in the used strategy.

The indented lines as dependents from the last unindented line above. The #-marked dependents are possible dependents revealed by the Analyser. You can decide whether you want to take those in consideration or not by deleting or keeping the #.


Architecture of the TransformationStrategy
------------------------------------------
The TransformationStategy is the actual heart of our project. Every strategy extents this abstract class. It provides the methods each concrete strategy should implement. So each new strategy should check if a rule (consiting of an table.attribut, its dependents, a given strategy and the optional additionalInfo) is valid for the tasks the strategy wants to process.

Everything concerning the transformation is encapsulated within the abstract methods prepareTableForTransformation and the setUpTransformation.
Furthermore, the TransformationStrategy provides the possibility to return a shuffeled array (of numbers or characters or strings) and 

You can easily add new anonymizing strategies. Each new stategy needs to be added to the CLASSPATH and must be named in the *Config*.    



**Contributions are Welcome!**
------------------------------

