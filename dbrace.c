#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <fcntl.h>

#include "sqlite.h"
#include "bdb.h"
#include "mysql.h"

/*
 * Global variables
 */
const char *progname;
unsigned long cache = 0;
int print = 0;

static void usage()
{
    fprintf(stderr, "usage: \n"
            "%s {-b [-x] [-c <cache in MB>] [-p <page_size>] | -s [-c <cache in pages>] | -m}"
                "[-o] [-r] [-t <trn_size>] [-n <nentries>] -w|-d|-g\n\n"
            "Options:\n"
            "-b chooses BerkeleyDB, -s chooses SQLite implementation, -m choose MySQL implementation.\n"
            "-o write data to screen\n"
            "-r means data size varies from 1-255 bytes (default fixed 14 bytes).\n"
            "-t <trn_size> is the number of writes in a single transaction (default is auto).\n"
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


int main(int argc, char **argv)
{
    int dump = 0, get = 0, populate = 0, sqlite = 0, bdb = 0, mysql = 0, random = 0;
    int c, pageSize = 4096;
    unsigned long n = 1000, txnsize = 0;
    int bdb_private;
    int rc;
    char *mysql_host = NULL;    /* H */
    char *mysql_user = NULL;    /* U */
    char *mysql_pw = NULL;      /* P */
    char *mysql_db = NULL;      /* D */

    progname = argv[0];

    while ((c = getopt(argc, argv, "bc:dD:gH:mn:op:P:rst:U:wx")) != EOF)
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
        case 'D':
            mysql_db = strdup(optarg);
            break;
        case 'g':
            get = 1;
            break;
        case 'H':
            mysql_host = strdup(optarg);
            break;
        case 'm':
            mysql = 1;
            break;
        case 'n':
            n = strtoul(optarg, 0, 0);
            break;
        case 'o':
            print = 1;
            break;
        case 'P':
            mysql_pw = strdup(optarg);
            break;
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
        case 'U':
            mysql_user = strdup(optarg);
            break;
        case 'w':
            populate = 1;
            break;
        case 'x':
            bdb_private = 1;
            break;
        case '?':
            usage();
        }

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

        if (populate)
            sqlite_populate(n, random, txnsize);
        else if (dump)
            sqlite_dump();
        else if (get)
            sqlite_get(n);
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
 
        bdb_open(cache, bdb_private, pageSize, txnsize);

        if (dump) {
            bdb_dump();
        } else if (get) {
            bdb_get(n);
        } else {
            bdb_populate(n, txnsize, random);
        }

        bdb_close();

    }

    if (mysql) {
        printf("Running MySQL benchmark: ");
        if (populate)
            printf("creating database.\n");
        else if (get)
            printf("reading database, fetching records one by one.\n");
        else /* dump */
            printf("dumping database.\n");            
        printf("Number of records: %lu\n", n);

        if (dump) {
            mysql_dump(mysql_host, mysql_user, mysql_pw, mysql_db);
        } else if (get) {
            mysql_get(mysql_host, mysql_user, mysql_pw, mysql_db, n);
        } else {
            mysql_populate(mysql_host, mysql_user, mysql_pw, mysql_db,
                           n, txnsize, random);
        }
    }

    return 0;
}

