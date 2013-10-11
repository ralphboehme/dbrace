#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <fcntl.h>

#include <sqlite3.h>
#include "dbrace.h"
#include "sqlite.h"

static sqlite3 *sqldb;

void sqlite_dump(void)
{
    int rc;
    sqlite3_stmt *sql_stmt;

    rc = sqlite3_open(SQLITE_FILENAME, &sqldb);
    if (rc != SQLITE_OK) {
        printf("sqlite3_open: Couldn't open %s", SQLITE_FILENAME);
        exit(1);
    }
    
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

    rc = sqlite3_close(sqldb);
    if (rc != SQLITE_OK)
        printf("sqlite3_close: %s", sqlite3_errmsg(sqldb));
}

void sqlite_get(unsigned long n)
{
    int rc;
    sqlite3_stmt *sql_stmt;
    unsigned long i;

    rc = sqlite3_open(SQLITE_FILENAME, &sqldb);
    if (rc != SQLITE_OK) {
        printf("sqlite3_open: Couldn't open %s", SQLITE_FILENAME);
        exit(1);
    }

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

    rc = sqlite3_close(sqldb);
    if (rc != SQLITE_OK)
        printf("sqlite3_close: %s", sqlite3_errmsg(sqldb));
}

static int sqlite_insert(sqlite3_stmt *sql_stmt, unsigned long key, int random)
{
    int rc;
    char data[256];
    int dlen = 14, i;

    if (random)
        dlen = rand() % (255 - 1) + 1; /* 1 to 255 byte data */

    for (i = 0; i < dlen - 1; i++)
        data[i] = (key + i) % (128 - 32) + 32;
    data[i] = 0;

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

void sqlite_populate(unsigned int n, int random, unsigned long txnsize)
{
    static int inited = 0;
    int rc;
    unsigned long i;
    char sql_str[200];
    char *zErrMsg = NULL;
    sqlite3_stmt *sql_stmt;

    unlink(SQLITE_FILENAME);
    rc = sqlite3_open(SQLITE_FILENAME, &sqldb);
    if (rc != SQLITE_OK) {
        printf("sqlite3_open: Couldn't open %s", SQLITE_FILENAME);
        exit(1);
    }

    if ( ! inited) {
        rc = sqlite3_prepare(sqldb, "insert into tbl VALUES (?, ?);", -1, &sql_stmt, NULL);
        if( rc!=SQLITE_OK ){
            printf("sqlite3_prepare error.\n");
            exit(1);
        }
        inited = 1;
    }

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
        sqlite_insert(sql_stmt, i, random);
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

    rc = sqlite3_close(sqldb);
    if (rc != SQLITE_OK)
        printf("sqlite3_close: %s", sqlite3_errmsg(sqldb));
}
