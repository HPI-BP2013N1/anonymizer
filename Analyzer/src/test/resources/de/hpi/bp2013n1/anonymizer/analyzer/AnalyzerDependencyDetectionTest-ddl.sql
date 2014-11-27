CREATE TABLE TABLE1 (
	E SMALLINT, 
	F SMALLINT,
	PRIMARY KEY(E, F)
);
CREATE TABLE TABLE2 (
	E1 SMALLINT,
	F1 SMALLINT,
	PRIMARY KEY(E1, F1)
);

CREATE TABLE TABLE3 (
	A SMALLINT,
	PRIMARY KEY(A)
);
CREATE TABLE TABLE4 (
	B SMALLINT,
	PRIMARY KEY(B)
);
CREATE TABLE TABLE5 (
	A1 SMALLINT, -- ref TABLE3.A
	B1 SMALLINT, -- ref TABLE4.B
	A2 SMALLINT, -- ref TABLE3.A
	PRIMARY KEY(A1, B1)
);
CREATE TABLE TABLE6 (
	A1 SMALLINT, -- ref TABLE5.A1 (indirect TABLE3.A)
	B1 SMALLINT, -- ref TABLE5.B1 (indirect TABLE4.B)
	A2 SMALLINT, -- ref TABLE5.A2 (indirect TABLE3.A) but could only be guessed by the name, as TABLE5.A2 is not even part of a key
	X6 SMALLINT,
	PRIMARY KEY(A1, B1, A2)
);
-- If an A x is deleted in TABLE3, 
-- rows in TABLE5 with A1 = x or A2 = x and
-- rows in TABLE6 with A1 = x or A2 = x should be deleted as well.
-- If a B x is deleted in TABLE4,
-- rows in TABLE5 with B1 = x and
-- rows in TABLE6 with B1 = x should be deleted as well.
-- If an (A1,B1) x is deleted in TABLE5,
-- rows in TABLE6 with (A1,B1) = x should be deleted as well.
-- If an (A1,B1,A2) x is deleted in TABLE5,
-- rows in TABLE6 with (A1,B1,A2) = x should be deleted as well 
-- (included in the rule above).

-- Therefore the config must look like (comments not as in desired output):
-- TABLE3.A
--   TABLE5.A1
--   TABLE5.A2
--   TABLE6.A1 # actually optional if rules can be composed, forbidden otherwise
--   TABLE6.A2
-- TABLE4.B
--   TABLE5.B1
--   TABLE6.B1 # actually optional if rules can be composed, forbidden otherwise
-- TABLE5.A1
--   TABLE6.A1
-- TABLE5.B1
--   TABLE6.B1
-- # how should the program know that these are not real:
-- TABLE6.A1
--   TABLE5.A1
-- TABLE6.A2
--   TABLE5.A2
-- TABLE6.B1
--   TABLE5.B1

-- conclusion: the consequences of one or the other notation must be clear to
-- the author of the config file

-- however the rules should be fewer if FK constraints are present

CREATE TABLE TABLE7 (
	C SMALLINT,
	PRIMARY KEY(C)
);
CREATE TABLE TABLE8 (
	D SMALLINT,
	PRIMARY KEY(D)
);
CREATE TABLE TABLE9 (
	C1 SMALLINT, -- ref TABLE7.A
	D1 SMALLINT, -- ref TABLE8.B
	C2 SMALLINT, -- ref TABLE7.A
	PRIMARY KEY(C1, D1)
);
CREATE TABLE TABLE10 (
	C1 SMALLINT, -- ref TABLE9.A1 (indirect TABLE7.A)
	D1 SMALLINT, -- ref TABLE9.B1 (indirect TABLE8.B)
	C2 SMALLINT, -- ref TABLE9.A2 (indirect TABLE7.A)
	X10 SMALLINT,
	PRIMARY KEY(C1, D1, C2),
	FOREIGN KEY(C1, D1, C2) REFERENCES TABLE9(C1, D1, C2)
);
-- #TABLE7.C
--   #TABLE9.C1
--   #TABLE9.C2
--   #TABLE10.C1 # actually optional if rules can be composed, forbidden otherwise
--   #TABLE10.C2 # actually optional if rules can be composed, forbidden otherwise
-- #TABLE8.D
--   #TABLE9.D1
--   #TABLE10.D1 # actually optional if rules can be composed
-- # the following are actually optional because 
-- # they will be derived from the FK at runtime
-- # if they would be transformed that should be visible in the config file
-- TABLE9.C1
--   TABLE10.C1
-- TABLE9.D1
--   TABLE10.D1
-- TABLE9.C2
--   TABLE10.C2   # the last two C-rules will be merged to 9.(C1,C2)<--10.(C1,C2)
-- # these should NOT be generated:
-- -TABLE10.C1-
--   -TABLE9.C1-
-- -TABLE10.C2-
--   -TABLE9.C2-
-- -TABLE10.D1-
--   -TABLE9.D1-
