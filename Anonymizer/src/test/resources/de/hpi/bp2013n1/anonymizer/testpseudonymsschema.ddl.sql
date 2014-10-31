---
-- #%L
-- Anonymizer
-- %%
-- Copyright (C) 2013 - 2014 HPI Bachelor's Project N1 2013
-- %%
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--      http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- #L%
---
-- <ScriptOptions statementTerminator=";" />
CREATE SCHEMA ORIGINAL;
CREATE TABLE ORIGINAL.CINEMA_ADDRESS
(
   OLDVALUE varchar(50) PRIMARY KEY NOT NULL, NEWVALUE varchar(50) NOT NULL
)
;
CREATE TABLE ORIGINAL.CINEMA_COMPANY
(
   OLDVALUE varchar(20) PRIMARY KEY NOT NULL, NEWVALUE varchar(20) NOT NULL
)
;
CREATE TABLE ORIGINAL.VISITOR_BIRTHDATE_CHARACTERS
(
   OLDVALUE char(1) PRIMARY KEY NOT NULL, NEWVALUE char(1) NOT NULL
)
;
CREATE TABLE ORIGINAL.VISITOR_SURNAME
(
   OLDVALUE varchar(20) PRIMARY KEY NOT NULL, NEWVALUE varchar(20) NOT NULL
)
;
CREATE TABLE ORIGINAL.VISITOR_ZIPCODE
(
   OLDVALUE char(5) PRIMARY KEY NOT NULL, NEWVALUE char(5) NOT NULL
)
;
CREATE TABLE ORIGINAL.VISIT_MOVIE
(
   OLDVALUE varchar(30) PRIMARY KEY NOT NULL, NEWVALUE varchar(30) NOT NULL
)
;
