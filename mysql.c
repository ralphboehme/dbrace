#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <fcntl.h>

#include "dbrace.h"
#include "mysql.h"

#include <my_global.h>
#include <mysql.h>

static MYSQL *con;

static void exit_error(void)
{
    fprintf(stderr, "%s\n", con ? mysql_error(con) : "Couldn't initialize MySQL library");
    if (con)
        mysql_close(con);
    exit(1);        
}

static void mysql_open(char *mysql_host, char *mysql_user, char *mysql_pw, char *mysql_db)
{
    if ((con = mysql_init(NULL)) == NULL)
        exit_error();

    if (mysql_real_connect(con, mysql_host, mysql_user, mysql_pw, mysql_db, 0, NULL, 0) == NULL)
        exit_error();

}

static void mysql_insert(unsigned long key, int random)
{
    int rc;
    char data[256];
    int dlen = 14, i;
    char binbuf[1024], sqlbuf[2048];

    if (random)
        dlen = rand() % (255 - 1) + 1; /* 1 to 255 byte data */

    for (i = 0; i < dlen - 1; i++)
        data[i] = (key + i) % (128 - 32) + 32;
    data[i] = 0;
    mysql_real_escape_string(con, binbuf, data, dlen);

    snprintf(sqlbuf, 4095, "INSERT INTO dbrace VALUES(%lu, '%s')", key, binbuf);

    if (mysql_query(con, sqlbuf))
        exit_error();
}

void mysql_populate(char *mysql_host, char *mysql_user, char *mysql_pw, char *mysql_db,
                    unsigned long n, unsigned long txnsize, int random)
{
    mysql_open(mysql_host, mysql_user, mysql_pw, mysql_db);

    if (mysql_query(con, "DROP TABLE IF EXISTS dbrace"))
        exit_error();

    if (mysql_query(con, "CREATE TABLE dbrace(Id INT PRIMARY KEY,Value VARCHAR(255))"))
        exit_error();

    for (unsigned long i = 1; i < n; i++) {
        mysql_insert(i, random);
    }

    mysql_close(con);
}

void mysql_get(char *mysql_host, char *mysql_user, char *mysql_pw, char *mysql_db,
               unsigned long n)
{
    MYSQL_RES *result = NULL;
    MYSQL_ROW row;
    char sqlbuf[1024];

    mysql_open(mysql_host, mysql_user, mysql_pw, mysql_db);

    for (unsigned long i = 1; i <= n; i++) {
        snprintf(sqlbuf, 1023, "SELECT Value FROM dbrace WHERE Id=%lu", i);
        if (mysql_query(con, sqlbuf))
            exit_error();

        if ((result = mysql_store_result(con)) == NULL)
            exit_error();

        if ((row = mysql_fetch_row(result))) {
            if (print)
                printf("%lu: %s\n", i, row[0]);
        }
        mysql_free_result(result);
    }

    mysql_close(con);
}

void mysql_dump(char *mysql_host, char *mysql_user, char *mysql_pw, char *mysql_db)
{
    MYSQL_RES *result = NULL;
    MYSQL_ROW row;

    mysql_open(mysql_host, mysql_user, mysql_pw, mysql_db);

    if (mysql_query(con, "SELECT Id,Value FROM dbrace"))
        exit_error();

    if ((result = mysql_store_result(con)) ==NULL)
        exit_error();

    while ((row = mysql_fetch_row(result))) {
        if (print)
            printf("%s: %s\n", row[0], row[1]);
    }

    if (result)
        mysql_free_result(result);
    mysql_close(con);
}
