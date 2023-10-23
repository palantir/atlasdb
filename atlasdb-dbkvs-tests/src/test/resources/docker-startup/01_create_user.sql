CREATE USER palantir IDENTIFIED BY "7_SeeingStones_7";

GRANT
    CREATE SESSION,
    CREATE PROCEDURE,
    CREATE SEQUENCE,
    CREATE TABLE,
    CREATE VIEW,
    CREATE TYPE
TO palantir;

ALTER USER palantir
    QUOTA UNLIMITED ON users;

EXIT;
