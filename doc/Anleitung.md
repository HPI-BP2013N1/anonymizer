Anonymisierer: Schritt für Schritt Anleitung
============================================

1. [Ermittlung des Bedarfs an vorzunehmenden Verfremdungen](#step1)
2. [Erstellen der ersten Konfigurationsdatei](#step2)
3. [Analyse des Datenbankschemas starten](#analyzer)
4. [Analyeergebnis auswerten](#post-analysis)
5. [Datenbankschema in die Zieldatenbank einspielen](#createschema)
5. [Anonymisierung starten](#anonymize)
6. [Zieldatenbank transferieren](#movedestionationdb)

<a name="step1"></a>
Schritt 1: Ermittlung des Bedarfs an vorzunehmenden Verfremdungen
-----------------------------------------------------------------

Bevor Sie den Anonymisierer benutzen können, müssen Sie zunächst festlegen,
welche Daten auf welche Art und Weise verfremdet (transformiert) werden sollen.
Als erstes sollten dafür die Relationen identifiziert werden, die überhaupt
herausgegeben und dafür anonymisiert werden sollen. Für deren Attribute müssen
dann geeignete Transformationsverfahren bestimmt werden. Die standardmäßig zur Verfügung gestellten Transformationsverfahren umfassen

1. das Leeren von Attributen (in allen Tupeln wird die betroffene Attribut auf
den gleichen Wert gesetzt -- vergleichbar mit Schwärzen von Spalten)
2. das Ersetzen jeder Attributausprägung durch ein eindeutiges Pseudonym
3. das zeichenweise Ersetzen durch Pseudonymzeichen (für zusammengesetzte
Attribute, bei denen jede Stelle z. B. in einem Code eine eigene Bedeutung
trägt)
4. das Löschen von Tupeln, die bestimmte Bedingungen erfüllen
5. das Löschen von Tupeln, sodass anschließend in einem Attribut einer Relation
von jeder Ausprägung gleich viele Exemplare übrig bleiben

Zusätzlich kann festgelegt werden, dass Tupel, die bestimmte Bedingungen erfüllen, auf keinen Fall gelöscht (aber dennoch verfremdet) werden sollen.

Sensible Informationen, wie zum Beispiel personenbezogene Daten, sollen in der
Regel dem Empfänger eines anonymisierten Datensatzes nicht zur Verfügung stehen.
Daher bietet sich für solche Attribute grundsätzlich Variante 1 (Leeren) an.
Allerdings lässt sich dabei anschließend nicht mehr feststellen, welche Tupel
sich in dem originalen Datensatz bei diesem Attribut voneinander unterschieden
haben. Wenn Sie zum Beispiel eine Relation mit von Kunden bestellten Artikeln
(Posten) haben und dort (vereinfacht angenommen) der Name als
Fremdschlüsselattribut in die Relation aller Kunden verwendet wurde, in der
Postenrelation der Name aus Datenschutzgründen aber geleert wird, lässt sich
anschließend nicht mehr ermitteln, ob zwei Posten zum selben Kunden gehörten
oder nicht. Je nachdem, zu welchem Zweck die anonymisierten Daten herausgegeben
werden, kann das erwünscht sein oder nicht. Falls der Verwendungszweck bedingt,
dass die Kundenzuordnung, wenn auch nicht namentlich, noch vorhanden sein muss,
bietet sich als Alternatives Transformationsverfahren die Variante 2
(Pseudonymisierung) an. Dabei wird beispielsweise Herr Müller zu "A" und Frau
Meier zu "C". Ihre Namen bleiben so verborgen, aber es kann noch unterschieden
werden, ob bestimmte Posten von "A" oder "C" gekauft wurden.

Manchmal werden in einem Attribut gleichzeitig mehrere Informationen
gespeichert (zusammengesetzte Attribute). Für den Fall, dass es sich bei dem
Attribut um ein Kodewort handelt, bei dem jedes einzelne Zeichen oder Gruppen
von Zeichen eine eigene Bedeutung trägt (zum Beispiel
"Produktnummer-Jahrgang-Seriennummer"), ist es womöglich von Vorteil, wenn
der Zusammenhang dieser Bedeutungen über alle Tupel hinweg erhalten bleibt (alle
Tupel, die vorher die gleiche Produktnummer im Kode aufwiesen, haben nach der
Anonymisierung immer noch untereinander gleiche Zeichen anstelle der
Produktnummer). Hierfür kann Variante 3 (zeichenweise Pseudonymisierung) benutzt
werden. Dabei wird jedem Zeichen ein neues Zeichen wie ein Pseudonym zugeordnet
(aus A wird beispielsweise C und aus U eine 7). So wird sichergestellt, dass
sich wiederholende Zeichenkombinationen auch im anonymisierten Datensatz als
andere sich wiederholende Zeichenkombinationen wiederzufinden sind (CC7 statt
AAU). Außerdem kann angegeben werden, dass nur ein Teil der Zeichenkette
verändert werden soll (Seriennummer wird transformiert, Produktnummer und
Jahrgang bleiben bestehen).

Bei jeder Pseudonymisierung oder Anonymisierung ist jedoch zu beachten, dass es
unter Umständen nicht ausreicht, nur ein Attribut zu leeren oder zu
pseudonymisieren. Oftmals lassen sich Attribute aus den anderen Attributen
innerhalb eines Tupels rekonstruieren, insbesondere, wenn weitere Datensätze
vorliegen, mit denen kreuzreferenziert werden kann (zum Beispiel hat [L.
Sweeney](#ref1) anhand von Volkszählungsdaten der USA aus dem Jahr 1990
ermittelt, dass 87% der US-Bürger nur anhand von fünfstelliger Postleitzahl,
Geschlecht und Geburtsdatum eindeutig identifiziert werden konnten). Daher
müssen bei jeder Relation zunächst einmal alle Attribute für die Anonymisierung
berücksichtigt werden, um eine wirkungsvolle Anonymisierung zu gewährleisten.

Sind einige Tupel besonders sensibel, weil sie zum Beispiel zu Prototypen
aus der Entwicklungsabteilung gehören, sollten diese Vermutlich mit Variante 4
komplett aus dem Datensatz entfernt werden. Von den gelöschten Tupeln abhängige
Tupel in anderen Relationen (z. B. über Fremdschlüssel oder weil in der
Anonymisiererkonfiguration so angegeben) werden ebenfalls gelöscht.

Manchmal sollen die Werteverteilungen innerhalb einer Relation geheim bleiben
(damit zum Beispiel nicht mehr ermittelt werden kann, welche Produkte sich
besonders gut verkauft haben). In diesem Fall können mit Variante 5 Tupel so
entfernt werden, dass am Ende für ein bestimmtes Attribut (Produktschlüssel) von
jeder Ausprägung gleich viele Tupel übrig bleiben (für jedes Produkt gibt es
dann beispielsweise gleich viele Posten).

<a name="step2"></a>
Schritt 2: Erstellen der ersten Konfigurationsdatei
---------------------------------------------------

Ist man sich im ersten Schritt bewusst geworden, was mit den Daten passieren
soll, ist der nächste Schritt, dies in eine dem Anonymisierungsprogramm
verständliche Form zu bringen. Das Programm benötigt zwei Konfigurationsdateien.
Die erste ist eine Liste der überhaupt zu betrachtenden Relationen. Diese werden
in einer Textdatei aufgezählt, wobei der Name jeder Relation (Tabelle) auf einer
Zeile steht. Zum Beispiel:

    PRODUKTE
	POSTEN
	KUNDEN

Diese Textdatei nennen wir von nun an *Scope-Datei* (Scope (engl.) = Einflussbereich).

Die zweite Konfigurationsdatei enthält wesentlich mehr Informationen, denn sie
beschreibt, wie die einzelnen Attribute transformiert werden sollen, welche
davon miteinander zusammenhängen, woher die Daten überhaupt kommen und wohin sie
gespeichert werden sollen. Diese zweite Datei wird im folgenden schlicht die
*Konfigurationsdatei* genannt, da hierein die meiste Arbeit investiert werden
muss.

Für die ersten Zeilen können zu Beginn folgende Zeilen kopiert werden:

    jdbc:db2://dbserver:port/OriginalDB Benutzername Passwort
    jdbc:db2://dbserver:port/ZielDB Benutzername Passwort
    jdbc:db2://dbserver:port/TransformationsDB Benutzername Passwort
    # Schemaname und Batch-Insert-Größe
    DBSCHEMA 10000
    
    - D: de.hpi.bp2013n1.anonymizer.SetDefaultStrategy
    - P: de.hpi.bp2013n1.anonymizer.PseudonymizeStrategy
    - C: de.hpi.bp2013n1.anonymizer.CharacterStrategy
    - U: de.hpi.bp2013n1.anonymizer.UniformDistributionStrategy
	- DeleteWhere: de.hpi.bp2013n1.anonymizer.DeleteRowStrategy
    - RetainWhere: de.hpi.bp2013n1.anonymizer.RetainRowStrategy

Zeilen, die mit einer Raute (#) beginnen, sind Kommentarzeilen und werden vom
Programm ignoriert. Sie denen lediglich dem besseren Verständnis für den Leser
der Datei.

In den ersten drei Zeilen wird definiert, wie mit den an der Anonymisierung
beteiligten Datenbanken verbunden werden kann. Die erste Zeile enthält die
JDBC-URL für eine Verbindung zur Datenbank, die die originalen Daten enthält,
gefolgt vom für die Verbindung zu benutzenden Benutzernamen und dessen Passwort.
Der Datenbankmanagementsystem-(DBMS)-Benutzer, der hier angegeben wird, muss
Leseberechtigungen für die zu anonymisierenden Daten und die Metadaten
(Fremdschlüssel etc.) für die Relationen haben. Wie die JDBC-URL aufgebaut ist,
hängt vom verwendeten DBMS ab. Die obigen URLs sind an die von IBM DB2
angelehnt. Sie enthalten vor allem den Hostnamen oder die IP-Adresse des
Datenbankservers, auf welchem Port dort mit der Datenbank verbunden werden kann
und den Namen der Datenbank auf dem Server. Bei Dateigestützten DBMS kann die
URL zum Beispiel den Pfad zur Datenbankdatei enthalten. Konsultieren Sie hierfür
die Dokumentation ihres Datenbanksystems und wie man dazu mittels JDBC
Verbindungen aufbauen kann.

Die zweite Zeile enthält die gleichen Informationen, aber für die Datenbank, in
der die anonynmisierten Daten gespeichert werden sollen. Hier muss der
DBMS-Benutzer Schreibrechte haben. Das Anonymisierungsprogramm geht davon aus,
dass bereits alle Tabellen dort angelegt wurden, daher muss dies
[später](#createschema) als vorbereitende Maßnahme geschehen. Die dritte Zeile
gibt eine Datenbank an, die das Anonymisierungsprogramm benutzen kann, um
beispielsweise die erzeugten Pseudonyme zu hinterlegen. Hier muss der
DBMS-Benutzer Schreib- und Leserechte haben und auch neue Relationen anlegen
können.

Es muss nicht für jede der drei Datenbanken das gleiche DMBS verwendet werden.
Im Gegenteil ist es von Vorteil, wenn die Zieldatenbank von den anderen beiden
getrennt ist. Da sich mithilfe der Transformationsdatenbank die Anonymisierung
teilweise rückgängig machen lässt, muss diese genauso wie die Originaldatenbank
unter Verschluss gehalten werden (falls man sie nach der Anonymisierung nicht
gleich löschen möchte).

Unter den drei Zeilen mit den drei Datenbanken folgt eine Zeile, in der das
Schema, in dem die zu anonymisierenden Relationen definiert sind, aufgeführt
wird, sowie die Anzahl der Zeilen, die beim Einfügen in die Zieldatenbank
jeweils gleichzeitig dorthin geschickt werden. Höhere Zahlen führen in der Regel
zu höherer Geschwindigkeit, allerdings unterstützen nicht alle DBMS beliebig
große Werte (bei DB2 kann beispielsweise das Transaktionslog volllaufen, wenn
die Zahl zu groß gewählt wird).

Die mit einem Minus beginnenden Zeilen definieren, welche
Transformationsverfahren der Anonymisierer verwenden soll. Vor dem Doppelpunkt
steht jeweils ein frei wählbares Zeichen oder Wort, mit dem das Verfahren im
Folgenden in der Konfigurationsdatei benannt wird. Hierbei darf es keine
Doppelungen geben. Hinter dem Doppelpunkt steht jeweils der vollqualifizierte
Name der Java-Klasse, in der das Verfahren implementiert ist. Die oben genannten
sind die standardmäßig mitgelieferten Verfahren. Die SetDefaultStrategy leert
Attribute (setzt ein Attribut für jedes Tupel auf den gleichen Wert),
PseudonymizeStrategy ersetzt Werte durch Pseudonyme, CharacterStrategy tut das
zeichenweise, UniformDistributionStrategy löscht Tupel, um eine Gleichverteilung
für ein Attribut zu erreichen, DeleteRowStrategy löscht Tupel, die eine
Bedingung erfüllen, und RetainRowStrategy erzwingt, dass solche behalten werden,
die eine Bedingung erfüllen.

Anschließend werden die Anonymisierungsregeln in folgender Form notiert:

    Relation.Attribut    Verfahren   (zusätzliche Angaben)
	    Relation.AbhängigesAttribut
		AndereRelation.AbhängigesAttribut

*Relation.Attribut* ist dabei das Attribut (oder für manche Verfahren auch nur
die Relation), auf das das durch *Verfahren* benannte Transformationsverfahren
angewendet werden soll. Bei *Verfahren* muss eines der Wörter benutzt werden,
die bei den zuvor erläuterten mit Minus beginnenden Zeilen vor dem Doppelpunkt
standen. Die meisten Verfahren benötigen oder akzeptieren optional zusätzliche
Angaben, um die Transformation zu beeinflussen. Unter der ersten Zeile einer
solchen Regel können nun eingerückt weitere Attribute in derselben oder anderen
Relationen aufgezählt werden, die von *Relation.Attribut* abhängig sind und
daher auf die gleiche Art und Weise transformiert werden müssen (das heißt zum
Beispiel unter Verwendung der gleichen Pseudonyme). Das ist vor allem bei
Fremdschlüsseln wichtig. Gibt es keine abhängigen Attribute, kann einfach die
nächste Transformationsregel uneingerückt darunter notiert werden.

Wenn die Abhängigkeiten im DBMS als Fremdschlüssel definiert sind oder die Namen
der abhängigen Attribute den übergeordneten Attributnamen enthalten, kann das
Program die Abhängigkeiten auch selbst [im nächsten Schritt](#analyzer)
ermitteln.

Um auf das vorangegangene Beispiel mit den Produkten, Kunden und Posten
zurückzukommen, könnte eine mögliche Konfigurationsdatei wie folgt aussehen:

    jdbc:db2://dbserver:port/OriginalDB Benutzername Passwort
    jdbc:db2://dbserver:port/ZielDB Benutzername Passwort
    jdbc:db2://dbserver:port/TransformationsDB Benutzername Passwort
    DBSCHEMA 10000
    
    - D: de.hpi.bp2013n1.anonymizer.SetDefaultStrategy
    - P: de.hpi.bp2013n1.anonymizer.PseudonymizeStrategy
    - U: de.hpi.bp2013n1.anonymizer.UniformDistributionStrategy
	- DeleteWhere: de.hpi.bp2013n1.anonymizer.DeleteRowStrategy
    - RetainWhere: de.hpi.bp2013n1.anonymizer.RetainRowStrategy

    ARTIKEL.NUMMER         P
        POSTEN.ARTIKELNUMMER
	ARTIKEL                RetainWhere  NUMMER < 1000
	ARTIKEL                DeleteWhere  PREIS > 100
	POSTEN.ARTIKELNUMMER   U      
    KUNDEN.NAME            P  Kunde
        POSTEN.KUNDENNAME
	ARTIKEL.EINSTELLDATUM  D  1990-01-01

Diese Konfiguration würde bewirken, dass bei der Anonymisierung...

1. alle Artikel neu durchnummeriert werden
und die in der POSTEN-Relation referenzierenden Artikelnummern dementsprechend
angepasst werden
2. alle Artikel und davon (über die Nummer) abhängige Posten mit einem
Preis von über 100 gelöscht werden, außer wenn ihre Artikelnummer kleiner als
1000 ist
3. Posten so gelöscht werden, dass es am Ende zu jedem Artikel gleich viele
Posten gibt
4. Kundennamen durch Pseudonyme ersetzt werden (sowohl in der KUNDEN-Relation
als auch in der POSTEN-Relation), wobei jedes Pseudonym mit "Kunde" beginnt
(gefolgt von einer zufälligen Zeichenfolge)
5. das Einstelldatum aller Artikel auf den 1.1.1990 gesetzt wird

Dabei gilt natürlich, dass die Originaldatenbank nicht verändert wird, sondern
dass die Veränderungen den Unterschied zwischen Originaldatenbank und
Zieldatenbank beschreiben.

Das Einrücken der Verfahrenskürzel (P, U usw.), sodass diese alle untereinander
stehen, ist nicht notwendig, wird aber der Übersichtlichkeit halber empfohlen.

In diesem Beispiel haben Sie bereits einige zusätzliche Angaben für
Transformationsregeln und, was sie bewirken, gesehen. Hier eine komplette Liste
der Angaben, die für die standardmäßig bereitgestellten Verfahren gemacht werden
können:

**SetDefaultStrategy**: es kann angegeben werden, auf welchen Wert das Attribut
gesetzt wird

**PseudonymizeStrategy**: es kann ein Präfix angegeben werden, mit dem alle
Pseudonyme beginnen (darf nicht zu lang für das Attribut sein)

**CharacterStrategy**: es muss ein Ersetzungsmuster angegeben werden. Die Länge
des Musters muss mit der Länge des Attributs übereinstimmen. Im Muster steht `K`
dafür, eine Stelle nicht zu ersetzen, und `P` dafür, eine Stelle durch
Pseudonymzeichen zu ersetzen. Ein Muster von `PPPKKKP` sorgt bei einem
siebenstelligen Attribut also dafür, dass die ersten drei und die letzte Stelle
ersetzt werden, während die verbliebenen drei Stellen so bleiben, wie sie sind.

**DeleteRowStrategy**: es muss eine Löschbedingung in Form einer
SQL-WHERE-Klausel (ohne WHERE) angegeben werden.

**RetainRowStrategy**: es muss eine Bedingung für zu behaltende Tupel in Form
einer SQL-WHERE-Klausel (ohne WHERE) angegeben werden.

**UniformDistributionStrategy**: es können zwei Sachverhalte kodiert werden:

1. Eine Projektion des Attributwerts, die angewendet wird, bevor die
Ausprägungen gezählt werden. Durch die Angabe von `SUBSTR(..., 1, 1)` (das
DBMS muss dafür eine SUBSTR-Funktion unterstützen) könnte zum Beispiel
erreicht werden, dass nach Anwendung des Verfahrens für jeden ersten
vorkommenden Buchstaben im Attribut gleich viele Tupel vorliegen.
2. Eine Mindestmenge an Tupeln kann spezifiziert werden, die eine Ausprägung
erreichen muss, damit sie nicht komplett gelöscht wird. Wenn eine Ausprägung
beispielsweise nur einmal vorkommt, würde für jede Ausprägung nur noch ein
Tupel übrig bleiben. Durch eine Angabe von `require at least 10` kann
erreicht werden, dass alle Tupel mit Ausprägungen, die weniger oft als zehn
mal vorkommen, gelöscht werden, dafür aber für jede andere Ausprägung
mindestens zehn Tupel erhalten bleiben. Durch `require at least 20%` werden
alle Ausprägungen verworfen, die weniger als 20% Vorkommen im Vergleich zur
*häufigsten* Ausprägung des Attributs haben. Es wird also sichergestellt,
dass mindestens 20% der Tupel mit der häufigsten Ausprägung erhalten bleiben
(und ebenso viele mit anderen Ausprägungen, bis auf jene mit den zu seltenen
Ausprägungen).

Beide Angaben können durch ein Semikolon getrennt kombiniert werden. Zum
Beispiel stellt `SUBSTR(..., 1, 3); require at least 20` sicher, dass für jede
Kombination der ersten drei Zeichen des Attributs mindestens 20 Tupel übrig
bleiben. Tupel mit Kombinationen, die weniger als 20 Mal vorkommen, werden
gelöscht.

<a name="analyzer"></a>
Schritt 3: Analyse des Datenbankschemas starten
-----------------------------------------------

Nachdem mit der ersten Konfigurationsdatei grundsätzlich definiert wurde, welche
Daten wie anonymisiert werden sollen, sollten als nächstes alle Abhängigkeiten
zu den zu transformierenden Attributen und Tabellen aufgedeckt werden. Fehlen
einige davon in der Konfiguration kann es beim Übertragen der Daten in die
Zieldatenbank zu Constraint-Verletzungen bspw. durch nicht beachtete
Fremdschlüsselbeziehungen kommen.

Um weitere Abhängigkeiten automatisch aufzudecken, enthält das Programm einen
Analysierer, der die Metadaten der Originaldatenbank nach Fremschlüsseln und
ähnlich lautenden Attributen durchsucht. Dabei wird eine ergänzte
Konfigurationsdatei erzeugt. Dieser Analysierer wird folgendermaßen
aufgerufen:

    $ java -cp Analyzer.jar:jdbcdriver.jar de.hpi.bp2013n1.anonymizer.analyzer.Main Konfiguration_vorher.txt scope.txt Konfiguration_hinterher.txt

*jdbcdriver.jar* ist dabei die Java-Bibliothek, die ggf. benötigt wird, damit
ihr JDBC-Treiber für die Datenbankverbindungen geladen werden kann. Bei DB2 ist
das in der Regel eine Datei namens db2jcc.jar oder db2jcc4.jar.
*Konfiguration_vorher.txt* ist die Konfigurationsdatei, die sie im [vorigen
Schritt](#step2) erstellt haben, während *scope.txt* die Scope-Datei ist. Bei
*Konfiguration_hinterher.txt* können Sie durch einen beliebigen Dateinamen oder
Pfad angeben, wo die neue, ergänzte Konfigurationsdatei erstellt werden soll.
Existierende Dateien werden dabei überschrieben!

Neben dem Aufdecken zusätzlicher Abhängigkeiten wird die Konfigurationsdatei bei
der Analyse auch auf ihre Gültigkeit überprüft. Dazu gehört auch, dass die
einzelnen Transformationsverfahren überprüfen, ob die zusätzlichen Angaben zu
jeder Regel den Voraussetzungen des jeweiligen Verfahrens entsprechen.

<a name="post-analysis"></a>
Schritt 4: Auswertung des Analyseergebnisses
--------------------------------------------

Durch den Analysierer wird eine neue Konfigurationsdatei ausgegeben, die ggf.
mehr Zeilen enthält, als die, die sie im zweiten Schritt erstellt haben. Für das
oben genannte Beispiel mit Produkten, Kunden und Posten könnte das Ergebnis zum
Beispiel so aussehen:

    jdbc:db2://dbserver:port/OriginalDB Benutzername Passwort
    jdbc:db2://dbserver:port/ZielDB Benutzername Passwort
    jdbc:db2://dbserver:port/TransformationsDB Benutzername Passwort
    DBSCHEMA 10000
    
    - D: de.hpi.bp2013n1.anonymizer.SetDefaultStrategy
    - P: de.hpi.bp2013n1.anonymizer.PseudonymizeStrategy
    - U: de.hpi.bp2013n1.anonymizer.UniformDistributionStrategy
	- DeleteWhere: de.hpi.bp2013n1.anonymizer.DeleteRowStrategy

    ARTIKEL.NUMMER         P
        POSTEN.ARTIKELNUMMER
		LAGER.ARTIKELNUMMER
	ARTIKEL                RetainWhere  NUMMER < 1000
	ARTIKEL                DeleteWhere  PREIS > 100
	POSTEN.ARTIKELNUMMER   U      
    KUNDEN.NAME            P  Kunde
        POSTEN.KUNDENNAME
		#ARTIKEL.NAME
        #KUNDENKARTEN.KUNDENNAME
	ARTIKEL.EINSTELLDATUM  D  1990-01-01
	    #MITARBEITER.EINSTELLDATUM

Hier wurden vier Abhängigkeiten ergänzt:

1. eine im DBMS vorhandene Fremdschlüsselbeziehung von LAGER.ARTIKELNUMMER zu
ARTIKEL.NUMMER
2. ein zu KUNDEN.NAME ähnlich lautendes Attribut ARTIKEL.NAME
3. ein zu KUNDEN.NAME ähnlich lautendes Attribut KUNDENKARTEN.KUNDENNAME
4. ein zu ARTIKEL.EINSTELLDATUM ähnlich bzw. gleichlautendes Attribut
MITARBEITER.EINSTELLDATUM

Bei den Nummern 2 und 4 handelt es sich vermutlich nicht um Abhängigkeiten, da die
Attribute nur gleich heißen, die Artikelbezeichnung aber nicht mit einem
Kundennamen zusammenhängen dürfte, genausowenig wie das Einstelldatum eines
Mitarbeiters von dem eines Artikels abhängen sollte. Wegen solcher
Unsicherheiten sind alle Vorschläge, die das Programm unterbreitet, die es nicht
auf Fakten aus dem DBMS wie Fremdschlüssel zurückführen kann, standardmäßig
auskommentiert (daher die Raute). Der Vorschlag mit der Nummern 3 könnte
jedoch sehr wohl eine gültige Beziehungen sein, obwohl dafür kein Fremdschlüssel
im DBMS angelegt war. Um diese bei der folgenden Anonymisierung zu beachten,
sollte die Raute entfernt werden, damit die Zeile nicht mehr auskommentiert
ist.

<a name="createschema"></a>
Schritt 5: Datenbankschema in die Zieldatenbank einspielen
----------------------------------------------------------

Die Zieldatenbank sollte vor der Anonymisierung vom Aufbau her der
Originaldatenbank entsprechen. Lediglich Tupel sollte sie noch keine enthalten.
Um das Schema zu übertragen nutzen Sie am besten die Werkzeuge, die Ihnen Ihr
DBMS zur Verfügung stellt. Das Anonymisierungsprogramm enthält keine derartige
Funktionalität.

<a name="anonymize"></a>
Schritt 6: Anonymisierung starten
---------------------------------

Nach den vielen vorbereitenden Schritten kann nun die Anonymisierung gestartet
werden. Dazu sollte folgendes auf der Kommandozeile ausgeführt werden:

    $ java -cp Anonymizer.jar:jdbcdriver.jar de.hpi.bp2013n1.anonymizer.Anonymizer Konfiguration.txt scope.txt Protokolldatei.log

*jdbcdriver.jar* ist hier wieder der bereits in der [Analyse](#analyzer)
erwähnte JDBC-Treiber für die verwendeten DBMS. *Konfiguration.txt* ist die
finale Version der Konfigurationsdatei, die in [Schritt 4](#post-analysis)
entstanden ist. *scope.txt* ist wieder die Scope-Datei. Bei *Protokolldatei.log*
muss ein Dateiname oder Pfad angegeben werden, wohin das Programm Fehler, die
bei der Anonymisierung auftreten können, speichern wird. Diese Logdatei kann
unter Umständen Informationen enthalten, die dem Empfänger des anonymisierten
Datensatzes nicht bekannt werden sollen. Daher sollte diese Logdatei genauso wie
die Transformationsdatenbank nicht herausgegeben werden.

Nach diesem Schritt sollten, sofern keine gravierenden Fehler aufgetreten sind,
in der Zieldatenbank die anonymisierten Tupel vorliegen.

<a name="movedestionationdb"></a>
Schritt 7: Zieldatenbank transferieren
--------------------------------------

Falls die in der Konfigurationsdatei angegebene Zieldatenbankinstanz noch nicht
beim Empfänger der anonymisierten Daten liegt, muss diese noch in geeigneter
Form exportiert und dem Empfänger überreicht werden. Bitte benutzen Sie hierzu
wieder die Werkzeuge, die Ihr DBMS Ihnen zur Verfügung stellt, da das
Anonymisierungsprogramm keine Funktionalität für diesen Schritt bereitstellt.

Referenzen
----------
- <a name="ref1"></a>
  L. Sweeney, *Uniqueness of Simple Demographics in the U.S. Population*,
  LIDAPWP4.
  Carnegie Mellon University, Laboratory for International Data Privacy,
  Pittsburgh, PA: 2000.
 
<!-- vim: tw=80
 -->
