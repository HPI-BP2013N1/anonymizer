
Anonymisiserer für relationale Datenbanken
==========================================

Wozu?
-----
Stellen Sie sich vor, Sie wollen anderen Personen Einblick in eine Ihrer Datenbanken verschaffen, dürfen aber aus Datenschutzgründen oder um Firmengeheimnisse zu wahren keine sensiblen Originaldaten herausgeben. Dieses Programm hilft dabei, in dem damit bestimmte Daten verfälscht oder gelöscht werden können.


Was wird benötigt?
------------------
* 3 Datenbanken:
    + Originaldatenbank = zu anonymisierende Datenbank
    + Übersetzungsdatenbank = Datenbank zur Ablage von Übersetzungsdaten für eine Pseudonymisierung (sollte leer sein, wenn man nicht bereits Übersetzungen hat, die wiederverwendet werden sollen)
    + Zieldatenbank = wo die anonymisierten, herauszugebenden Daten hinsollen (wird geleert, bevor die Daten hierhin übertragen werden!)
* Konfigurationsdatei (für Details siehe unten)
* Scope-Datei (Scope (engl.) = Wirkungsbereich, Anwendungsbereich, für Details siehe unten)
 

Implementierte Funktionalität
-----------------------------
Es werden verschiedene Mechanismen zur Anonymisierung und Pseudonymisierung bereitgestellt.

Zuerst gibt es einen *Analyzer* (Analysierer).
Diese Komponente benötigt zwei Listen als Eingabe: eine mit den Tabellen und Attributen, die auf irgendeine Art und Weise verändert werden sollen (*Konfiguration*), und eine mit allen Tabellen, die überhaupt in die Zieldatenbank übertragen werden sollen (*Scope*).
Siehe unten für Details zum Aufbau dieser beiden Dateien.
Der Analyzer untersucht dann die Originaldatenbank auf Abhängigkeiten im Datenbankschema zu den aufgeführten Attributen.
Als Ergebnis wird eine ergänzte Konfigurationsdatei ausgegeben.
Dort können die gefundenen Abhängigkeiten betrachtet werden und entschieden werden, ob sie bei der folgenden Anonymisierung beachtet werden sollen.

Die folgenden Strategien zur Anonymisierung von Daten sind derzeit implementiert:

* __SetDefaultStrategy__

	Löscht Attributwerte. Es kann ein Wert vorgegeben werden, der stattdessen bei jedem Tupel für dieses Attribut eingetragen werden soll, sodass z. B. eine gewisse Struktur im Wert beibehalten wird (z. B. Postleitzahl 12345, damit der Wert noch fünfstellig ist).

* __PseudonomizeStrategy__

	Dieses Verfahren erlaubt das Pseudonymisieren von Attributwerten. Es vergibt für jeden einzigartigen Attributwert ein neues Pseudonym.

* __CharacterStrategy__

	Hiermit können Attributwerte zeichenweise pseudonymisiert werden. Es kann angegeben werden, welche Stellen beibehalten und welche pseudonymisiert werden sollen.

* __DeleteRowStrategy__

	Um auszuschließen, dass bestimmte Tupel in der Zieldatenbank erscheinen (nicht einmal anonymisiert), kann dieses Verfahren angewendet werden.

* __RetainRowStrategy__

	Hiermit kann sichergestellt werden, dass bestimmte Tupel auf jeden Fall (ggf. anonymisisert und pseudonymisiert) in der Zieldatenbank erscheinen. Das kann z. B. sinnvoll sein in Verbindung mit der...

* __UniformDistributionStrategy__

	Dieses Verfahren erlaubt es die Werteverteilung für ein Attribut in eine Gleichverteilung zu bringen, um die originale Verteilung der Werte geheimzuhalten.


Unterstützte Umgebungen
-----------------------
* Java SE 1.7
* DB2 oder H2 (andere Datenbanksysteme könnten funktionieren wurden aber nicht getestet). Solange das DBMS keine Spezialbehandlung bei den SQL-Anfragen benötigt, sollte es funktionieren. Schauen Sie sich im Zweifelsfall die SQLHelper-Klassen an.)
* Maven (Sie können das Projekt auch als Maven Project mit m2e in Eclipse importieren.)


Abhängigkeiten
--------------
* google guava 17.0
* zum Testen:
    + junit
    + h2
    + mockito-core
    + hamcrest-library
    + dbunit


Konfiguration - die Konfigurationsdatei
---------------------------------------
Hier können Sie ein Beispiel für die *Konfigurationsdatei* sehen (bitte behalten Sie die gezeigte Formatierung bei):

    # originalDB neueDB übersetzungsDB jeweils mit Benutzername und Passwort
    jdbc:h2:mem:original name pw
    jdbc:h2:mem:destination name pw
    jdbc:h2:mem:transformations name pw
    # Schemaname und Batch-Insert-Größe
    ORIGINAL 10000
    
    - Default: de.hpi.bp2013n1.anonymizer.SetDefaultStrategy
    - P: de.hpi.bp2013n1.anonymizer.PseudonymizeStrategy
    - S: de.hpi.bp2013n1.anonymizer.CharacterStrategy
    - U: de.hpi.bp2013n1.anonymizer.UniformDistributionStrategy
    - Retain: de.hpi.bp2013n1.anonymizer.RetainRowStrategy
    
    # Tabelle.Attribut      Typ     Zusätzliche Informationen
    VISITOR                 Retain  SURNAME = 'Michael'
    VISITOR.SURNAME         P
        VISIT.VISITORSURNAME
    VISITOR.BIRTHDATE       S       KKKKPPPP
        VISIT.VISITORBIRTHDATE
    VISITOR.ZIPCODE         P
    VISITOR.ADDRESS         Default
        #CINEMA.ADDRESS
        #VISIT.CINEMAADDRESS
    CINEMA.COMPANY          P       CinemaCompany
        VISIT.CINEMACOMPANY
        #GREATMOVIES.COMPANY
    CINEMA.ADDRESS          P       Address
        VISIT.CINEMAADDRESS
        #VISITOR.ADDRESS
    VISIT.MOVIE             P       Movie
        GREATMOVIES.MOVIE
    PRODUCTSALES.PRODUCTID  U

Zeilen, die mit "#" beginnen, sind Kommentare und werden bei der Anonymisierung ignoriert.

Zuerst müssen die Verbindungsdaten zu den drei benötigten Datenbanken angegeben werden.
Es folgt der Schemaname der zu bearbeitenden Tabellen und die Anzahl der jeweils gleichzeitig einzufügenden Tupel.
Darunter können Abkürzungen für die gewünschten Verfahren vergeben werden. Dazu wird jeweils der vollqualifizierte Klassenname des Verfahrens angegeben.

Unter "Tabelle.Attribut" werden die Attribute aufgelistet, die beim Anonymisieren verändert werden sollen. Bedenken Sie: die mit # beginnenden Zeilen werden beim Anonymisieren als Kommentar ignoriert.
"Typ" definiert, welches Verfahren auf das Attribut angewendet werden soll.
"Zusätzliche Informationen" können Muster (für die CharacterStrategy), Präfixe (für PseudonymizeStrategy), Bedingungen (für RetainRow- und DeleteRowStrategy) etc. sein, wie vom jeweiligen Verfahren vorgegeben.

Die eingerückten Zeilen listen vom darüberstehenden nicht eingerückten Attribut abhängige Attribute auf.
Die mit # auskommentierten Abhängigen sind mögliche Abhängige, die der Analyzer gefunden hat. Sie können entscheiden, ob diese beachtet werden sollen oder nicht, indem Sie die # entfernen oder stehenlassen.

Ein paar Details zur den Verfahren:

* __SetDefaultStrategy__

	Die zusätzlichen Informationen in der *Konfiguration* enthalten den jeweils einzusetzenden Wert, der bei allen Tupeln eingetragen wird. Ansonsten wird das Attribut leer gesetzt.

* __PseudonomizeStrategy__

	Es können Präfixe als zusätzliche Information in der *Konfiguration* angegeben werden, z. B. um die Art eines Attributs angedeutet zu lassen. Wenn bspw. Namen pseudonymisiert werden sollen, kann "Name" als Präfix angegeben werden, damit die Pseudonyme "Name" gefolgt von zufälligen Buchstaben lauten

* __CharacterStrategy__

	Die zusätzliche Information in der *Konfiguration* gibt hier das Ersetzungsmuster an. "K" (keep) bedeutet, dass die Stelle nicht verändert werden soll, während P (pseudoynmisieren) heißt, dass die Stelle pseudonymisiert werden soll.
     
* __DeleteRowStrategy__

	Als zusätzliche Information in der *Konfiguration* soll hier eine Selektionsbedingung angegeben werden, wie bei einer SQL-Where-Klausel, nur ohne "where".
     
* __RetainRowStrategy__

	Als zusätzliche Information in der *Konfiguration* soll hier eine Selektionsbedingung angegeben werden, wie bei einer SQL-Where-Klausel, nur ohne "where".
	Beachten Sie, dass die Reihenfolge der auf ein Attribut bzw. eine Tabelle angewandten Verfahren eine Rolle spielt, wenn die RetainRowStrategy benutzt wird.
	Eine Retain-Regel sollte zuerst in der *Konfiguration* stehen, da sie nicht funktioniert, wenn durch eine darüberstehende Regel bereits eigentlich zu behaltende Tupel gelöscht werden.
	Der Test, ob ein Tupel auf die Selektionsbedingung passt, wird immer mit dem unveränderten Tupel aus der Originaldatenbank ausgeführt.
     
* __UniformDistributionStrategy__

	Um die ursprüngliche Größe jeder Kategorie nach Ausprägung des Attributs nicht trotzdem ermittelbar zu machen, müssen auch andere Spalten berücksichtigt werden.
	Wenn es bspw. ein weiteres Attribut für den Zeitpunkt des Eintragens des Tupels gibt und man weiß, dass Tupel für jede Ausprägung täglich in Stapeln einlaufen, kann geschlussfolgert werden, dass eine Kategorie mit späteren Eintragezeitpunkten größer war als eine mit früheren Eintragezeitpunkten.

Scope - welche Tabellen übertragen werden sollen
------------------------------------------------
Zusätzlich zur hierüber beschriebenen *Konfigurationsdatei* muss auch eine *Scope-Datei* erstellt werden.
Diese ist eine simple Textdatei, in der pro Zeile ein Tabellenname steht.
Nur Tabellen, welche in dieser *Scope-Datei* aufgelistet werden, werden vom Analyzer und Anonymisiser angesehen, transformiert und in die Zieldatenbank übertragen.

Ein Beispiel-*Scope* für das obige *Konfigurations*-Beispiel könnte folgendes sein:

    VISITOR
    VISIT
    CINEMA
    GREATMOVIES
    PRODUCTSALES

Beachten Sie, dass der implizite Schemaname für diese Tabellen in der *Konfigurationsdatei* steht.


Architektur der TransformationStrategy
--------------------------------------
Die *TransformationStategy* ist das Herz dieses Projekts.
Jedes Verfahren zur Datentransformation erbt von dieser abstrakten Klasse.
Sie deklariert die Methoden, die jedes Verfahren implementieren muss.
Z. B. muss jedes Verfahren in der Lage sein überprüfen zu können, ob eine Konfigurationsregel (bestehend aus Tabelle.Attribut, den abhängigen Feldern, einem Verfahren und den zusätzlichen Informationen) die Voraussetzungen für dieses Verfahren erfüllt.

Alles, was die Transformation der Daten anbelangt, soll sich in den Methoden
*setUpTransformation*,
*prepareTableForTransformation* und
*transform* abspielen.
*setUpTransformation* wird einmal nach dem Lesen der *Konfigurationsdatei* ausgeführt.
*prepareTableForTransformation* wird jedes mal ausgeführt, bevor eine neue Tabelle bearbeitet wird.
Schließlich wird *transform* jedes Mal aufgerufen, wenn ein Attributwert in einem Tupel transformiert werden soll.

Es können einfach weitere Anonymisiserungsverfahren implementiert werden.
Jedes neue Verfahren muss dem CLASSPATH zur Laufzeit hinzugefügt werden und die Klasse des Verfahrens muss wie oben beschrieben in der *Konfigurationsdatei* aufgeführt werden.

Start per Kommandozeile
-----------------------
* Analyzer:

        $ java -cp Analyzer.jar:<jdbc-Treiuber> de.hpi.bp2013n1.anonymizer.analyzer.Main Konfiguration_vorher.txt scope.txt Konfiguration_hinterher.txt

* Anonymizer:

        $ java -cp Anonymizer.jar:<jdbc-Treiber> de.hpi.bp2013n1.anonymizer.Anonymizer Konfiguration_hinterher.txt scope.txt Protokolldatei.log

Ersetzen Sie &lt;jdbc-Treiber&gt; mit Ihrem Treiber, z. B. db2jcc4.jar für DB2.


**Beiträge sind willkommen!**
-----------------------------

