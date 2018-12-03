CREATE TABLE P1 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER NOT NULL,
  NUM2 INTEGER NOT NULL,
  RATIO FLOAT NOT NULL,
  PRIMARY KEY (ID)
);

PARTITION TABLE P1 ON COLUMN ID;

CREATE INDEX P1_IDX_UNARY_MINUS ON P1 (-NUM, NUM2 ASC);

CREATE TABLE R1IX ( 
  ID INTEGER DEFAULT '0' NOT NULL, 
  DESC VARCHAR(300), 
  NUM INTEGER NOT NULL, 
  RATIO FLOAT NOT NULL,
  CONSTRAINT R1IX_PK_TREE PRIMARY KEY (ID) 
); 
CREATE INDEX R1IX_IDX_NUM_TREE ON R1IX (NUM); 
CREATE INDEX R1IX_IDX_RATIO_TREE ON R1IX (RATIO); 
CREATE INDEX R1IX_IDX_DESC_TREE ON R1IX (DESC);

CREATE TABLE R1 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER NOT NULL,
  NUM2 INTEGER NOT NULL,
  RATIO FLOAT NOT NULL,
  PRIMARY KEY (ID)
);

CREATE TABLE P2 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER NOT NULL,
  NUM2 INTEGER NOT NULL,
  RATIO FLOAT NOT NULL,
  CONSTRAINT P2_PK_TREE PRIMARY KEY (ID)
);
CREATE INDEX P2_IDX_NUM_TREE ON P2 (NUM);

PARTITION TABLE P2 ON COLUMN ID;

CREATE TABLE R2 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER NOT NULL,
  NUM2 INTEGER NOT NULL,
  RATIO FLOAT NOT NULL,
  CONSTRAINT R2_PK_TREE PRIMARY KEY (ID)
);
CREATE INDEX R2_IDX_NUM_TREE ON R2 (NUM);

CREATE TABLE P3 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER NOT NULL,
  NUM2 INTEGER NOT NULL,
  RATIO FLOAT NOT NULL,
  PRIMARY KEY (ID)
);
CREATE INDEX P3_IDX_COMBO ON P3 (NUM, NUM2);

PARTITION TABLE P3 ON COLUMN ID;

CREATE TABLE R3 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER NOT NULL,
  NUM2 INTEGER NOT NULL,
  RATIO FLOAT NOT NULL,
  PRIMARY KEY (ID)
);
CREATE INDEX R3_IDX_COMBO ON R3 (NUM, NUM2);

CREATE TABLE BINGO_BOARD (
 T_ID INTEGER NOT NULL,
 B_ID INTEGER NOT NULL,
 LAST_VALUE VARCHAR(128),
 CONSTRAINT B_PK_TREE PRIMARY KEY (T_ID, B_ID)
);

CREATE TABLE MANY_INTS (
  ID1 BIGINT NOT NULL,
  ID2 BIGINT NOT NULL,
  ID3 BIGINT NOT NULL,
  ID4 BIGINT NOT NULL,
  ID5 BIGINT NOT NULL,
  PRIMARY KEY (ID1, ID2, ID3, ID4)
);
CREATE INDEX IDX ON MANY_INTS (ID1, ID2, ID3, ID4, ID5);

-- special compound index schema for ENG-5537
CREATE TABLE tableX
(
    keyA INT NOT NULL,
    keyB INT NOT NULL,
    keyC SMALLINT DEFAULT '0',
    keyD VARCHAR(20),
    sort1 SMALLINT DEFAULT '3',
    keyE INT NOT NULL,
    PRIMARY KEY (keyA,keyB,keyD)
);

PARTITION TABLE tableX ON COLUMN keyA;

CREATE INDEX idx_x ON tableX(keyA,keyC,keyD,keyE);

CREATE TABLE tableY (
    keyA INT NOT NULL,
    keyB INT NOT NULL,
    keyH INT NOT NULL,
    keyI INT NOT NULL,
    PRIMARY KEY (keyA,keyB,keyH,keyI)
);

PARTITION TABLE tableY ON COLUMN keyA;

CREATE INDEX idx_y_keyI ON tableY(keyH,keyI);

-- tree index on varbinary type
CREATE TABLE varbinaryTableTree (
    id INTEGER NOT NULL,
    varb2 VARBINARY(2),
    varb512 VARBINARY(512)
);
CREATE INDEX varbinaryTableTree_INDEX_varb2 ON varbinaryTableTree(varb2);
CREATE INDEX varbinaryTableTree_INDEX_varb512 ON varbinaryTableTree(varb512);

-- ENG-10478
-- Sometimes we mangle boolean expressions of the form "col < 42 and col < 26661"
-- when "col" is the same table in both conjuncts.  See TestIndexesSuite.testBooleanExpressions.
DROP VIEW V_BTEST_R2_ABS IF EXISTS;
DROP TABLE BTEST_R2 IF EXISTS;
CREATE TABLE BTEST_R2 (
    ID          INTEGER NOT NULL, 
    WAGE        SMALLINT, 
    DEPT        SMALLINT, 
    AGE         SMALLINT, 
    RENT        SMALLINT, 
    PRIMARY KEY (ID)
);
CREATE VIEW V_BTEST_R2_ABS 
    (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) 
AS  SELECT ABS(wage), dept, count(*), sum(age), sum(rent)  
        FROM BTEST_R2 
        GROUP BY ABS(wage), dept;


