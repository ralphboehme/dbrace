#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <fcntl.h>

#include <db.h>
#include "dbrace.h"
#include "bdb.h"

#define BDB_OK        0

/* BDB global config */
static int bdb_env_flags = 0;
static DB_ENV *dbenv;
static DB *db;

static void bdb_error(int rc, const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    vfprintf(stderr, format, ap);
    fprintf(stderr, ": %s\n", db_strerror(rc));
    va_end(ap);
    exit(2);
}


void bdb_dump(void)
{
    int rc;
    DBC *cur;
    DBT key = { 0 }, data = { 0 };

    rc = db->cursor(db, NULL, &cur, 0);
    if (rc != BDB_OK)
        bdb_error(rc, "Couldn't create cursor");

    cur->c_get(cur, &key, &data, DB_FIRST);
    while (rc == BDB_OK) {
	if (print)
	    printf("key: %lu, data: %s\n", *(unsigned long *)key.data, (char *) data.data);
        rc = cur->c_get(cur, &key, &data, DB_NEXT);
    }

    if (rc != DB_NOTFOUND) {
        bdb_error(rc, "Error iterating over btree");
    }

    rc = cur->c_close(cur);
    if (rc != BDB_OK)
        bdb_error(rc, "Couldn't close cursor");
}


void bdb_get(unsigned long n)
{
    int rc;
    unsigned long i;
    DBT key = { 0 }, data = { 0 };

    key.data = &i;
    key.size = sizeof(i);

    for (i = 1; i < n; i+=2) {
        rc = db->get(db, NULL, &key, &data, 0);
        if (rc != BDB_OK)
            bdb_error(rc, "Error fetching key %lu", i);
        else
	    if (print)
		printf("key: %lu, data: %s\n", *(unsigned long *)key.data, (char *) data.data);
    }

    for (i = 2; i < n; i+=2) {
        rc = db->get(db, NULL, &key, &data, 0);
        if (rc != BDB_OK)
            bdb_error(rc, "Error fetching key %lu", i);
        else
	    if (print)
		printf("key: %lu, data: %s\n", *(unsigned long *)key.data, (char *) data.data);
    }
}

static int bdb_insert(DB_TXN *tid, unsigned long n, int random)
{
    char databuf[256];
    DBT key = { 0 }, data = { 0 };
    int i, rc;

    key.data = &n;
    key.size = sizeof(n);
    data.data = databuf;
    if (random)
        data.size = rand() % (255 - 1) + 1; /* 1 to 255 byte data */
    else
        data.size = 14;

    for (i = 0; i < data.size - 1; i++)
        databuf[i] = (n + i) % (128 - 32) + 32;
    databuf[i] = 0;
    rc = db->put(db, tid, &key, &data, 0);

    return rc;
}


void bdb_populate(unsigned long n, unsigned long txnsize, int random)
{
    int rc;
    unsigned long i;
    DB_TXN *tid = NULL;

    printf("a\n");

    if (txnsize > 1) {
        rc = dbenv->txn_begin(dbenv, NULL, &tid, 0);
        if (rc != BDB_OK)
            bdb_error(rc, "Couldn't begin transaction");
    }

    for (i = 1; i < n; i++) {
        rc = bdb_insert(tid, i, random);
        if (rc != BDB_OK)
            bdb_error(rc, "Couldn't insert key %d", i);
        if (txnsize > 1 && i % txnsize == 0) {
            rc = tid->commit(tid, 0);
            if (rc != BDB_OK)
                bdb_error(rc, "Couldn't commit btree");
            rc = dbenv->txn_begin(dbenv, NULL, &tid, 0);
            if (rc != BDB_OK)
                bdb_error(rc, "Couldn't begin transaction");
        }

    }
    if (tid)
        rc = tid->commit(tid, 0);
    if (rc != BDB_OK)
        bdb_error(rc, "Couldn't commit btree");
}

void bdb_open(unsigned long cache, int private, int pageSize, unsigned long txnsize)
{
    int rc = 0;
    struct stat sb;

    if (private)
        bdb_env_flags |= DB_PRIVATE;
    
    /*
     * If the directory exists, we're done. We do not further check
     * the type of the file, DB will fail appropriately if it's the
     * wrong type.
     */
    if (stat(BDB_ENV_DIRECTORY, &sb) != 0) {
        /* Create the directory, read/write/access owner only. */
        if (mkdir(BDB_ENV_DIRECTORY, S_IRWXU) != 0) {
            bdb_error(rc, "mkdir %s failed", BDB_ENV_DIRECTORY);
        }
    }

    /* Create the environment handle. */
    if ((rc = db_env_create(&dbenv, 0)) != 0)
        bdb_error(rc, "db_env_create: ");

    /* Set up error handling. */
    dbenv->set_errpfx(dbenv, progname);
    dbenv->set_errfile(dbenv, stderr);

    if (cache < 0) {
        dbenv->set_flags(dbenv, DB_TXN_WRITE_NOSYNC, 1);
        cache = -cache;
    }

    /* Set the cache */
    rc = dbenv->set_cachesize(dbenv, 0, cache, 1);
    if (rc != BDB_OK)
        bdb_error(rc, "Couldn't set cache to %d MB", cache/(1024*1024));

    /*
     * Open a transactional environment:
     * create if it doesn't exist
     * run recovery
     * read/write owner only
     */
    if ((rc = dbenv->open(dbenv,
                          BDB_ENV_DIRECTORY,
                          bdb_env_flags | DB_CREATE | DB_INIT_MPOOL | DB_INIT_TXN | DB_INIT_LOG | DB_INIT_LOCK,
                          S_IRUSR | S_IWUSR)
            ) != 0) {
        (void)dbenv->close(dbenv, 0);
        bdb_error(rc, "dbenv->open: %s", BDB_ENV_DIRECTORY);
    }

    printf("done.\n");

    rc = db_create(&db, dbenv, 0);
    if (rc != BDB_OK)
        bdb_error(rc, "Couldn't create bdb handle");

    rc = db->set_pagesize(db, pageSize);
    if (rc != BDB_OK)
        bdb_error(rc, "Couldn't set pageSize to %d bytes", pageSize);

    printf("txnsize: %lu\n", txnsize);

    rc = db->open(db, NULL, BDB_DB_FILENAME, NULL, DB_BTREE,
                  DB_CREATE | DB_AUTO_COMMIT,
                  0666);
    if (rc != BDB_OK)
        bdb_error(rc, "Couldn't open %s", BDB_DB_FILENAME);

}

void bdb_close(void)
{
    int rc;

    rc = db->close(db, 0);
    if (rc != BDB_OK)
        bdb_error(rc, "Couldn't close Btree file %s", BDB_DB_FILENAME);
}
