#ifndef SQLITE_H
#define SQLITE_H

#define SQLITE_FILENAME "sqlite.db"

extern void sqlite_dump(void);
extern void sqlite_get(unsigned long n);
extern void sqlite_populate(unsigned int n, int random, unsigned long txnsize);

#endif
