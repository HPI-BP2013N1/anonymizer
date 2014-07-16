
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
    
    pseudonomize over hole attribut
    Use the additional info of *Config* for defining prefixes.

* __CharacterStrategy__
    Here, you can pseudonymize your attributes characterwise. You can decide which character position you want to keep and which one to pseudonymize. Use the additional info of *Config* for this.
    
* __DeleteRowStrategy__
    If you want to make sure that certain tuples are definitely not (not even anonymized) within the result database you can choose them to be deleted. Write it similar to SQL where-clause, but without "where". Use the additional info of *Config* for this.

* __RetainRowStrategy__
    You can ensure that certain tuples shell be kept. Discribe the row you want to be retained similar to SQL where-clause, but without "where". Use the additional info of *Config* for this.

* __UniformDistributionStrategy__


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
    - bsp. config zeigen und erklären (bsp für "Where"-ohne where in add info)
    - ebenso scope?


Sample Usage
------------------------------
    - wie auf commandline bedienen (CLASSPATH etc)
    - wie neue strategien erzeugen

Special attention is needed for:
*UniformDistributionStrategy*
You must take other columns into consideration to not reveal the original cardinalities. For example, if there is an entry date column in the same table and you know that all categories are loaded in daily bunches you might deduce that a category with later starting dates was larger than another category with earlier dates.

*RetainRowStrategy*
Be careful with the order of the applied strategies when using also the RetainRowStrategy. You should put a retain rule first because it won't work if the values have already been transformed before the RetainRowStrategy is applied. (The retain comparison is always done against the original row via ResultSetRowReader.)


Architecture of the TransformationStrategy
------------------------------------------
The TransformationStategy is the core of our project. It provides the methods each concrete strategy should implement.
    - einzelne methoden der Klasse vorstellen (ggf. bsp)
    - müssen im classpath liegen

You can easily add new anonymizing strategies. Each new stategy must be named in the config.    



**Contributions are Welcome!**
------------------------------
