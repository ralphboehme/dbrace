#ifndef BDB_H
#define BDB_H

#include <db.h>

#define BDB_ENV_DIRECTORY "bdb"
#define BDB_DB_FILENAME "bdb.db"

extern void bdb_open(unsigned long cache, int private, int pageSize, unsigned long txnsize);
extern void bdb_close(void);
extern void bdb_dump(void);
extern void bdb_get(unsigned long n);
extern void bdb_populate(unsigned long n, unsigned long txnsize, int random);

#endif
