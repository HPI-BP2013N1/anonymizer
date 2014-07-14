
This is an Anonymizer for Relational Databases
==============================================

What for?
---------
If you want to give other people insights into your database but can't release your sensitive your original data, you can do this with the helf of this project.

Implemented Functionalities
---------------------------
We provide several mechanisms to anonymize your data:

* anonymize = delete attributes
* pseudonomize
    + over hole attribut
    + characterwise
* uniform distributen
* retain certain tuples
* delete certain tuples

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
    
Sample Usage and Configuration
------------------------------
    - wie auf commandline bedienen (CLASSPATH etc)
    - bsp. config zeigen und erklären
    - ebenso scope
    - wie neue strategien erzeugen

Special attention is needed for:
*UniformDistributionStrategy*
You must take other columns into consideration to not reveal the original cardinalities. For example, if there is an entry date column in the same table and you know that all categories are loaded in daily bunches you might deduce that a category with later starting dates was larger than another category with earlier dates.

*RetainStrategy*
You should put a retain rule first because it won't work if the values have already been transformed before the retain strategy is applied. (The retain comparison is always done against the original row via ResultSetRowReader.)

Architecture of the TransformationStrategy
------------------------------------------
The TransformationStategy is the core of our project. It provides the methods each concrete strategy should implement.
    - einzelne methoden der Klasse vorstellen (ggf. bsp)
    - müssen im classpath liegen

You can easily add new anonymizing strategies. Each new stategy must be named in the config.    



**Contributions are Welcome!**
------------------------------
