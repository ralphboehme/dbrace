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
#include <sqlite3.h>

#define BDB_OK        0
#define BDB_ENV_DIRECTORY "bdb"
#define BDB_DB_FILENAME "bdb.db"

#define SQLITE_FILENAME "sqlite.db"

static int rc;
static const char *progname;

/* Common global config */
static unsigned long cache = 0;
static int print = 0;

/* BDB global config */
static int bdb_env_flags = 0;

/* SQLite global config */
static sqlite3 *sqldb;

static void
bdb_error(const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    vfprintf(stderr, format, ap);
    fprintf(stderr, ": %s\n", db_strerror(rc));
    va_end(ap);
    exit(2);
}

static void
sqlite_dump(void)
{
    static sqlite3_stmt *sql_stmt;
    
    rc = sqlite3_prepare(sqldb, "select key,value from tbl;", -1, &sql_stmt, NULL);
    if( rc!=SQLITE_OK ){
        printf("sqlite3_prepare error: %s\n", sqlite3_errmsg(sqldb));
        exit(1);
    }

    while ( SQLITE_ROW == (rc = sqlite3_step(sql_stmt)) )
	if (print)
            printf("Key: '%s' - Value: '%s'\n",
                   sqlite3_column_text(sql_stmt, 0),
                   sqlite3_column_text(sql_stmt, 1) );
    
    sqlite3_finalize(sql_stmt);
}

static void
bdb_dump(DB *db)
{
    int rc;
    DBC *cur;
    DBT key = { 0 }, data = { 0 };

    rc = db->cursor(db, NULL, &cur, 0);
    if (rc != BDB_OK)
        bdb_error("Couldn't create cursor");

    cur->c_get(cur, &key, &data, DB_FIRST);
    while (rc == BDB_OK) {
	if (print)
	    printf("key: %lu, data: %s\n", *(unsigned long *)key.data, (char *) data.data);
        rc = cur->c_get(cur, &key, &data, DB_NEXT);
    }

    if (rc != DB_NOTFOUND) {
        bdb_error("Error iterating over btree");
    }

    rc = cur->c_close(cur);
    if (rc != BDB_OK)
        bdb_error("Couldn't close cursor");
}

static void
sqlite_get(unsigned long n)
{
    sqlite3_stmt *sql_stmt;
    unsigned long i;

    rc = sqlite3_prepare(sqldb, "select key,value from tbl where key=?;", -1, &sql_stmt, NULL);
    if( rc!=SQLITE_OK ){
        printf("sqlite3_prepare error: %s\n", sqlite3_errmsg(sqldb));
        exit(1);
    }

    for (i=1; i < n; i+=2) {
        rc = sqlite3_bind_int(sql_stmt, 1, i);
        if( rc != SQLITE_OK ){
            printf("sqlite3_bind_int error: %s\n", sqlite3_errmsg(sqldb));
            exit(1);
        }

        rc = sqlite3_step(sql_stmt);
        if ( rc == SQLITE_ROW ){
	    if (print)
		printf("Key: '%s' - Value: '%s'\n",
		       sqlite3_column_text(sql_stmt, 0),
		       sqlite3_column_text(sql_stmt, 1) );
        }
        sqlite3_reset(sql_stmt);
    }

    for (i=2; i < n; i+=2) {
        rc = sqlite3_bind_int(sql_stmt, 1, i);
        if( rc != SQLITE_OK ){
            printf("sqlite3_bind_int error: %s\n", sqlite3_errmsg(sqldb));
            exit(1);
        }

        rc = sqlite3_step(sql_stmt);
        if ( rc == SQLITE_ROW ){
	    if (print)
		printf("Key: '%s' - Value: '%s'\n",
		       sqlite3_column_text(sql_stmt, 0),
		       sqlite3_column_text(sql_stmt, 1) );
        }
        sqlite3_reset(sql_stmt);
    }
    
    sqlite3_finalize(sql_stmt);
}

static void
bdb_get(DB *db, unsigned long n)
{
    unsigned long i;
    DBT key = { 0 }, data = { 0 };

    key.data = &i;
    key.size = sizeof(i);

    for (i = 1; i < n; i+=2) {
        rc = db->get(db, NULL, &key, &data, 0);
        if (rc != BDB_OK)
            bdb_error("Error fetching key %lu", i);
        else
	    if (print)
		printf("key: %lu, data: %s\n", *(unsigned long *)key.data, (char *) data.data);
    }

    for (i = 2; i < n; i+=2) {
        rc = db->get(db, NULL, &key, &data, 0);
        if (rc != BDB_OK)
            bdb_error("Error fetching key %lu", i);
        else
	    if (print)
		printf("key: %lu, data: %s\n", *(unsigned long *)key.data, (char *) data.data);
    }
}

static sqlite3_stmt *sql_stmt;

static int
sqlite_insert(unsigned long key, int random)
{
    static int inited = 0;
    char data[256];
    int dlen = 14, i;

    if (random)
        dlen = rand() % (255 - 1) + 1; /* 1 to 255 byte data */

    for (i = 0; i < dlen - 1; i++)
        data[i] = (key + i) % (128 - 32) + 32;
    data[i] = 0;

    if ( ! inited) {
        rc = sqlite3_prepare(sqldb, "insert into tbl VALUES (?, ?);", -1, &sql_stmt, NULL);
        if( rc!=SQLITE_OK ){
            printf("sqlite3_prepare error.\n");
            exit(1);
        }
        inited = 1;
    }

    rc = sqlite3_bind_int(sql_stmt, 1, key);
    if( rc!=SQLITE_OK ){
        printf("sqlite3_bind_int error: %s\n", sqlite3_errmsg(sqldb));
        exit(1);
    }

    rc = sqlite3_bind_text(sql_stmt, 2, data, dlen, SQLITE_STATIC);
    if( rc!=SQLITE_OK ){
        printf("sqlite3_bind_text error: %s.\n", sqlite3_errmsg(sqldb));
        exit(1);
    }

    rc = sqlite3_step(sql_stmt);
    if( rc!=SQLITE_DONE ){
        printf("sqlite3_step error.\n");
        exit(1);
    }

    sqlite3_reset(sql_stmt);

    return 0;
}

static int
bdb_insert(DB *db, DB_TXN *tid, unsigned long n, int random)
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

static void
sqlite_populate(unsigned int n, int random, unsigned long txnsize)
{
    int rc;
    unsigned long i;
    char sql_str[200];
    char *zErrMsg = NULL;

    rc = sqlite3_exec(sqldb, "create table tbl(key INTEGER PRIMARY KEY, value BLOB);", NULL, NULL, &zErrMsg);
    if( rc!=SQLITE_OK ){
        fprintf(stderr, "sqlite3_exec error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        exit(1);
    }

    sprintf(sql_str,"PRAGMA default_cache_size = %lu;", cache);

    rc = sqlite3_exec(sqldb, sql_str, NULL, NULL, &zErrMsg);
    if( rc!=SQLITE_OK ){
        fprintf(stderr, "sqlite3_exec error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        exit(1);
    }

    if (txnsize > 1) {
        rc = sqlite3_exec(sqldb, "BEGIN TRANSACTION;", NULL, NULL, &zErrMsg);
        if( rc!=SQLITE_OK ){
            fprintf(stderr, "sqlite3_exec error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
            exit(1);
        }
    }
    for (i = 1; i < n; i++) {
        sqlite_insert(i, random);
        if (txnsize > 1 && i % txnsize == 0) {
            rc = sqlite3_exec(sqldb, "END TRANSACTION;", NULL, NULL, &zErrMsg);
            if( rc!=SQLITE_OK ){
                fprintf(stderr, "sqlite3_exec error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
                exit(1);
            }
            rc = sqlite3_exec(sqldb, "BEGIN TRANSACTION;", NULL, NULL, &zErrMsg);
            if( rc!=SQLITE_OK ){
                fprintf(stderr, "sqlite3_exec error: %s\n", zErrMsg);
                sqlite3_free(zErrMsg);
                exit(1);
            }
        }
    }

    if (txnsize > 1) {
        rc = sqlite3_exec(sqldb, "END TRANSACTION;", NULL, NULL, &zErrMsg);
        if( rc!=SQLITE_OK ){
            fprintf(stderr, "sqlite3_exec error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
            exit(1);
        }
    }

    sqlite3_finalize(sql_stmt);

}

static void
bdb_populate(DB_ENV *dbenv, DB *db, unsigned long n, unsigned long txnsize, int random)
{
    int rc;
    unsigned long i;
    DB_TXN *tid = NULL;

    if (txnsize > 1) {
        rc = dbenv->txn_begin(dbenv, NULL, &tid, 0);
        if (rc != BDB_OK)
            bdb_error("Couldn't begin transaction");
    }

    for (i = 1; i < n; i++) {
        rc = bdb_insert(db, tid, i, random);
        if (rc != BDB_OK)
            bdb_error("Couldn't insert key %d", i);
        if (txnsize > 1 && i % txnsize == 0) {
            rc = tid->commit(tid, 0);
            if (rc != BDB_OK)
                bdb_error("Couldn't commit btree");
            rc = dbenv->txn_begin(dbenv, NULL, &tid, 0);
            if (rc != BDB_OK)
                bdb_error("Couldn't begin transaction");
        }

    }
    if (tid)
        rc = tid->commit(tid, 0);
    if (rc != BDB_OK)
        bdb_error("Couldn't commit btree");
}

static void
bdb_env_open(DB_ENV **dbenvp, unsigned long cache)
{
    DB_ENV *dbenv;
    struct stat sb;

    /*
     * If the directory exists, we're done. We do not further check
     * the type of the file, DB will fail appropriately if it's the
     * wrong type.
     */
    if (stat(BDB_ENV_DIRECTORY, &sb) != 0) {
        /* Create the directory, read/write/access owner only. */
        if (mkdir(BDB_ENV_DIRECTORY, S_IRWXU) != 0) {
            bdb_error("mkdir %s failed", BDB_ENV_DIRECTORY);
        }
    }

    /* Create the environment handle. */
    if ((rc = db_env_create(&dbenv, 0)) != 0)
        bdb_error("db_env_create: ");

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
        bdb_error("Couldn't set cache to %d MB", cache/(1024*1024));


    /*
     * Open a transactional environment:
     * create if it doesn't exist
     * run recovery
     * read/write owner only
     */
    if ((rc = dbenv->open(dbenv,
                          BDB_ENV_DIRECTORY,
                          bdb_env_flags | DB_CREATE | DB_INIT_MPOOL | DB_INIT_TXN,
                          S_IRUSR | S_IWUSR)
            ) != 0) {
        (void)dbenv->close(dbenv, 0);
        bdb_error("dbenv->open: %s", BDB_ENV_DIRECTORY);
    }

    *dbenvp = dbenv;
}

static void
usage()
{
    fprintf(stderr, "usage: \n"
            "%s [-b [-x] [-c <cache in MB>] [-p <page_size>] | -s [-c <cache in pages>]] [-o] [-r] [-t <trn_size>] [-n <nentries>] [-w|-d|-g]\n\n"
            "Options:\n"
            "-b chooses BerkeleyDB, -s chooses SQLite implementation.\n"
            "-o write data to screen\n"
            "-r means data size varies from 1-255 bytes (default fixed 14 bytes).\n"
            "-t <trn_size> is the number of writes in a single transaction (default is all entries).\n"
            "-n how many entries to store in the database (default: 100000)\n"
            "-c cache size (default: 4 MB / 10000 pages)\n"
            "-x set DB_PRIVATE for DB_ENV->open\n"
            "-p BerkeleyDB database pagesite in bytes (default: 4096)\n\n"
            "Possible actions:\n"
            "-w populates the database\n"
            "-d dumps db scanning from the first record to last\n"
            "-g dumps db by directly fetching each key\n", progname);
    exit(1);
}


int
main(int argc, char **argv)
{
    DB *db = NULL;
    DB_ENV *dbenv;
    int dump = 0, get = 0, populate = 0, sqlite = 0, bdb = 0, random = 0;
    int c, pageSize = 4096;
    unsigned long n = 1000, txnsize = 0;

    progname = argv[0];

    while ((c = getopt(argc, argv, "bc:dgn:op:rst:wx")) != EOF)
        switch (c) {
        case 'b':
            bdb = 1;
            break;
        case 'c':
            cache = strtoul(optarg, 0, 0);
            break;
        case 'd':
            dump = 1;
            break;
        case 'g':
            get = 1;
            break;
        case 'n':
            n = strtoul(optarg, 0, 0);
            break;
	case 'o':
	    print = 1;
        case 'r':
            random = 1;
            break;
        case 's':
            sqlite = 1;
            break;
        case 't':
            txnsize = strtoul(optarg, 0, 0);
            break;
        case 'p':
            pageSize = strtoul(optarg, 0, 0);
            break;
        case 'w':
            populate = 1;
            break;
        case 'x':
            bdb_env_flags = DB_PRIVATE;
            break;
        case '?':
            usage();
        }

    if (!txnsize)
        txnsize = n;

    if (argc - optind != 0 || (populate + get + dump) != 1)
        usage();

    if (sqlite) {
        if ( !cache )
            cache = 10000;

        printf("Running SQLite benchmark: ");
        if (populate)
            printf("creating database.\n");
        else if (get)
            printf("reading database, fetching records one by one.\n");
        else /* dump */
            printf("dumping database.\n");            
        printf("Number of records: %lu\n", n);
        printf("Transaction size: %lu\n", txnsize);
        printf("Number of cache pages: %lu\n", cache);

        if (populate) {
            unlink(SQLITE_FILENAME);
            rc = sqlite3_open(SQLITE_FILENAME, &sqldb);
            if (rc != SQLITE_OK) {
                printf("sqlite3_open: Couldn't open %s", SQLITE_FILENAME);
                exit(1);
            }
            sqlite_populate(n, random, txnsize);
        } else {
            rc = sqlite3_open(SQLITE_FILENAME, &sqldb);
            if (rc != SQLITE_OK) {
                printf("sqlite3_open: Couldn't open %s", SQLITE_FILENAME);
                exit(1);
            }
        }

        if (dump)
            sqlite_dump();
        else if (get)
            sqlite_get(n);

        rc = sqlite3_close(sqldb);
        if (rc != SQLITE_OK)
            printf("sqlite3_close: %s", sqlite3_errmsg(sqldb));
    }
    
    if (bdb ) {
        if ( !cache )
            cache = 4;
        cache *= 1024 * 1024;

        printf("Running BerkeleyDB benchmark: ");
        if (populate)
            printf("creating database.\n");
        else if (get)
            printf("reading database, fetching records one by one.\n");
        else /* dump */
            printf("dumping database.\n");            
        printf("Number of records: %lu\n", n);
        printf("Transaction size: %lu\n", txnsize);
        printf("Page size: %u\n", pageSize);
        printf("Cache size: %lu MB\n", cache/(1024*1024));
        
        if (populate)
            system("rm -rf " BDB_ENV_DIRECTORY);
        bdb_env_open(&dbenv, cache);

        rc = db_create(&db, dbenv, 0);
        if (rc != BDB_OK)
            bdb_error("Couldn't create bdb handle");

        rc = db->set_pagesize(db, pageSize);
        if (rc != BDB_OK)
            bdb_error("Couldn't set pageSize to %d bytes", pageSize);

        rc = db->open(db, NULL, BDB_DB_FILENAME, NULL, DB_BTREE,
                      DB_CREATE | ((txnsize > 1) ? DB_AUTO_COMMIT : 0),
                      0666);
        if (rc != BDB_OK)
            bdb_error("Couldn't open %s", BDB_DB_FILENAME);

        if (dump)
            bdb_dump(db);
        else if (get)
            bdb_get(db, n);
        else
            bdb_populate(dbenv, db, n, txnsize, random);

        rc = db->close(db, 0);
        if (rc != BDB_OK)
            bdb_error("Couldn't close Btree file %s", BDB_DB_FILENAME);

    }
    return 0;
}

