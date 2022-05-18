# 5300-Rook
Instructor's DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2022

Group members: Terence Leung & William Zhong

Usage (argument is database directory):
<pre>
$ ./sql5300 ~/cpsc5300/data
</pre>

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop.
- <code>Milestone2h</code> has the intructor-provided files for Milestone2. (Note that heap_storage.cpp is just a stub.)
- <code>Milestone2</code> is the instructor's attempt to complete the Milestone 2 assignment.
- <code>Milestone3_prep</code> has the instructor-provided files for Milestone 3. The students' work is in <code>SQLExec.cpp</code> labeled with <code>FIXME</code>.
- <code>Milestone4_prep</code> has the instructor-provided files for Milestone 4. The students' work is in <code>SQLExec.cpp</code> labeled with <code>FIXME</code>.
## Unit Tests
There are some tests for SlottedPage and HeapTable. They can be invoked from the <code>SQL</code> prompt:
```sql
SQL> test
```
There are additional tests for milestone 3 and 4 that can be used by using the <code>SQL</code> prompt:
```sql
SQL> test2
SQL> test table
SQL> test3
SQL> test index
```
(test2 and test table are identitcal and test3 and test index are also identitcal... both ways of writing them results in invoking the same tests)

Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -f data/*
```

## Valgrind (Linux)
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.

## Milestone 4 Handoff video
https://seattleu.instructuremedia.com/embed/da9d1744-d4b1-491e-93ef-a27438903037
