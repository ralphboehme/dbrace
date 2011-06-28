CC=gcc

CFLAGS=-Wall -g

BDB_CFLAGS=/usr/local/BerkeleyDB-5-1/include
BDB_LDFLAGS=/usr/local/BerkeleyDB-5-1/lib
BDB_LIB=-ldb-5.1

all:	dbrace

clean:
	rm -f *.o dbrace
	rm -f sqlite.db
	rm -rf bdb

dbrace:	dbrace.o
	gcc -o dbrace -L$(BDB_LDFLAGS) -Wl,-rpath=$(BDB_LDFLAGS) dbrace.o $(BDB_LIB) -lsqlite3

dbrace.o: dbrace.c
	gcc $(CFLAGS) -I$(BDB_CFLAGS) -c -o dbrace.o dbrace.c