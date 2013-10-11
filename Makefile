CC=/opt/solarisstudio12.3/bin/c99

MYSQL_CFLAGS=$(shell /usr/mysql/bin/mysql_config --cflags)
MYSQL_LIBS=$(shell /usr/mysql/bin/mysql_config --libs)

CFLAGS=-D_XOPEN_SOURCE=600 -D__EXTENSIONS__ -D_GNU_SOURCE -I/usr/local/BerkeleyDB-5-1/include $(MYSQL_CFLAGS)
LDFLAGS=-L/usr/local/BerkeleyDB-5-1/lib -R/usr/local/BerkeleyDB-5-1/lib 
LIBS=-ldb-5.1 -lsqlite3 $(MYSQL_LIBS)

all:	dbrace

clean:
	rm -f *.o dbrace
	rm -f sqlite.db
	rm -rf bdb

dbrace:	dbrace.o bdb.o sqlite.o mysql.o
	gcc -o dbrace $^ $(LDFLAGS) $(LIBS) 
