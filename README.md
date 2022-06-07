# 5300-Rook
Team Rook's DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2022
## Sprint Invierno
Authors : Alex Larsen & Carter Martin
Usage (argument is database directory):
<pre>
$ ./sql5300 ~/cpsc5300/data
</pre>
### Tags 
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop.
- <code>Milestone2</code> includes slotted page, heap file, and heap table implementation.
- <code>Milestone3</code> includes CREATE TABLE, DROP TABLE, SHOW TABLE, and SHOW COLUMNS.
- <code>Milestone4</code> includes CREATE INDEX, SHOW INDEX, and DROP INDEX.
- <code>Milestone5</code> includes the INSERT, DELETE, and SELECT.
- <code>Milestone6</code> includes B+ tree INSERT and LOOKUP.
## Unit Tests
There are some tests for SlottedPage and HeapTable. They can be invoked from the <code>SQL</code> prompt:
```sql
SQL> test
```
We also added a test for our Milestone5 changes. It can be invoked using either of the following commands:
```sql
SQL> test2 or SQL> test queries
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -f ~/cpsc5300/data/*
```
## Valgrind (Linux)
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.
