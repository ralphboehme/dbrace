#ifndef MYSQL_H
#define MYSQL_H

extern void mysql_populate(char *mysql_host, char *mysql_user, char *mysql_pw, char *mysql_db,
                           unsigned long n, unsigned long txnsize, int random);

extern void mysql_get(char *mysql_host, char *mysql_user, char *mysql_pw, char *mysql_db,
                      unsigned long n);

extern void mysql_dump(char *mysql_host, char *mysql_user, char *mysql_pw, char *mysql_db);

#endif
