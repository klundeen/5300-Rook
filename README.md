# 5300-Rook

## Team Memebers:
#### Zeng, Zhicong
#### Lolakumari Jayachandran, Vindhya Nair
## Second Sprint Members:
### Terence Leung
### William Zhong

# MileStones
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop. Usage (argument is database directory)(command valgrind checks memory):
<pre>
$  make
$  ./sql5300 ~/cpsc5300/data
$  valgrind  ./sql5300 ~/cpsc5300/data --leak-check=full
SQL> test
</pre>
- <code>Milestone2</code> Implement a rudimentary storage engine. Implemented the basic functions needed for HeapTable with two data types: integer and text.

- <code>Milestone3</code>
