CREATE USER palantir IDENTIFIED BY "7_SeeingStones_7";
grant create session, create procedure, create sequence, create table, create view, create type to palantir;
ALTER USER palantir QUOTA UNLIMITED ON USERS;
exit;
