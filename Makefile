CC=gcc

CFLAGS=-Wall -g

BDB_INC=/opt/sw/bdb-4.7/include
BDB_LIB=/opt/sw/bdb-4.7/lib

SQLITE_INC=.

all:	dbrace

clean:
	rm -f *.o dbrace
	rm -f sqlite.db
	rm -rf bdb

dbrace:	dbrace.o sqlite3.o
	gcc -o dbrace -L$(BDB_LIB) -Wl,-rpath=$(BDB_LIB) dbrace.o sqlite3.o -ldb-4.7 -ldl

dbrace.o: dbrace.c
	gcc $(CFLAGS) -I$(BDB_INC) -I$(SQLITE_INC) -c -o dbrace.o dbrace.c