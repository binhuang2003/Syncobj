/*
 * comm.h
 *
 *  Created on: 2013-4-22
 *      Author: huangbin
 */

#ifndef COMM_H_
#define COMM_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <time.h>
#include <string.h>
#include <sys/types.h>
#include <oci.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <list>
#include <map>
#include <iostream>
#include <sys/time.h>
#include <signal.h>

#include <oratypes.h>
#include <ocidfn.h>
#include <oci.h>
#ifdef __STDC__
#include <ociapr.h>
#else
#include <ocikpr.h>
#endif
#include <ocidem.h>
using namespace std;

#ifndef u_int
#  define u_int unsigned int
#endif
#ifndef u_short
#  define u_short unsigned short
#endif
#ifndef u_char
#  define u_char unsigned char
#endif
#ifndef Bool
#  define Bool int
#endif

#ifdef _MSC_VER
#  ifndef int64
#    define int64 __int64
#  endif
#  ifndef u_int64
#    define u_int64 unsigned __int64
#  endif
#else
#  ifndef int64
#    define int64 long long
#  endif
#  ifndef u_int64
#    define u_int64 unsigned long long
#  endif
#endif

#define SOURCE_PROCEDURE 7
#define SOURCE_FUNCTION 8
#define SOURCE_PACKAGE 9
#define SOURCE_PACKAGE_BODY 11
#define SOURCE_TRIGGER 12
#define SOURCE_TYPE 13
#define SOURCE_TYPE_BODY 14

typedef struct _DATABASE {
	OCIEnv *envhp;
	OCIError *errhp;
	OCIServer *srvhp;
	OCISvcCtx *svchp;
	OCISession *seshp;
	OCIStmt *stmt;
	sb4 errcode;
	text errmsg[512];
	int prefetch;
	ub2 charset;
	ub2 ncharset;
	char charsets[20];
	char ncharsets[20];
	FILE *fp_log;
	OCIDescribe *dschp;

	dvoid *context;
	void (*msgfunc)(dvoid *p, unsigned char *buf);
} DATABASE;

typedef struct _login
{
	char user_name[128];
	char password[128];
	char tnsname[512];
	char schema[128];
	int  version;
} login;

#define MAX_INSTANCE_COUNT 10
#define MAX_IP_LENGTH 16
#define MAX_DBNAME_LENGTH 128

#define ON_DELETE_NOACT 0
#define ON_DELETE_SETNULL 1
#define ON_DELETE_CASCADE 2

#define STATUS_ENABLE 0
#define STATUS_DISABLE 1

typedef struct _DATABASE_LOGIN_STRUCT{
	int instance_no;
	char dbinstip[MAX_INSTANCE_COUNT][MAX_IP_LENGTH+1];
	char dbname[MAX_DBNAME_LENGTH+1];
	char dbinstport[10][6];
	char dbinstuser[100];
	char dbinstpassword[100];
	char dbservicename[100];
}DATABASE_LOGIN_STRUCT;

typedef struct _COLUMN_DEFINE{
	char col_name[128];
	ub2 col_type;
	char col_type_name[64];
	ub4 type_length;
	ub1 precision;//if precision is 0, define simply number
	sb1 scale;//for numeric
	ub2 charset_id;
}COLUMN_DEFINE;

typedef struct _TABLE_DEFINE{
	ub4 table_id;
	char name[128];
	char owner[64];
	int col_count;
	bool has_object_type;
	unsigned char is_partition;
	char is_cluster;
	list<COLUMN_DEFINE *> *columns;
}TABLE_DEFINE;

typedef struct _SYNONYMS_DEFINE{
	char obj_name[256];
	char syn_schema[256];
	char syn_name[256];
	char syn_link[256];
}SYNONYMS_DEFINE;

typedef struct _SCHEMA_OBJECT{
	int obj_num;
	map<string, TABLE_DEFINE *> tablemap;
	list<string> viewList;
	list<string> seqList;
	list<string> procedureList;
	list<string> functionList;
	list<string> packageList;
	list<string> packageBodyList;
	list<string> triggerList;
	list<string> typeList;
	list<string> typeBodyList;
	list<string> dirList;
	list<SYNONYMS_DEFINE *> synList;
}SCHEMA_OBJECT;

typedef struct _TAB_PARTITION{
	char partition_name[256];
	char high_value[1024];
	int value_length;
	int partition_pos;
	char tablespace_name[256];
	char compression[10];
}TAB_PARTITION;

typedef struct _FOREIGN_KEY{
	char forname[256];
	char refforname[256];
	char reftable[256];
	char fortable[256];
	ub1 delete_rule;
	ub1 status;
	list<string> *col_name;
	list<string> *refcol;
}FOREIGN_KEY;

typedef struct _TABLEINDEX{
	char index_type[10];
	char uniqueness[10];
	char index_name[256];
	char table_name[256];
	list<string> *colname;
}TABLEINDEX;

typedef struct _CMP2SCHEMA{
	char src_schema[128];
	char dst_schema[128];
	int do_sync;
}CMP2SCHEMA;

typedef struct _PROCPARAM{
	list<CMP2SCHEMA *> cmpchemalist;
	char ddl_path[256];
	int do_level;
	int threads;
}PROCPARAM;

typedef struct _UNIQUEKEY{
	char unique_cons[50];
	char status[10];
	list<string> *unique_col;
}UNIQUEKEY;

typedef struct _DEFCOLUMN{
	char col_name[256];
	char col_type[20];
	char col_ispk;
	char col_isuk;
	char col_isck;
	char constraint_name[100];
	char *search_cond;
	char status[10];
	char NULLABLE[3];
	char *data_default;
	char index_type[10];
	char index_name[128];
	char uniqueness[10];
	int col_id;
	int precision;
	int scale;
	int cons_pos;
	int col_length;
	int char_length;
}DEFCOLUMN;

typedef struct _DEFTABLE{
	char table_name[256];
	int col_count;
	int has_index;
	int has_check;
	int has_pk;
	int has_uk;
	char primary_cons[100];
	DEFCOLUMN defcolumn[];
}DEFTABLE;

typedef struct _DEFUNIQUECOL{
	char col_name[256];
	int pos;
}DEFUNIQUECOL;

typedef struct _DEFUNIQUECONS{
	char cons_name[100];
	char status[10];
	int num;
}DEFUNIQUECONS;

typedef struct _DEFJOB{
	int job;
	char next_date[20];
	char next_sec[20];
	char broken[2];
	char interval[200];
	char what[4000];
	char nls_env[2000];
	char misc_env[256];
}DEFJOB;

char *getcurtime();
void xlog(char *fmt, ...);
#define DEBUG 1

#ifdef DEBUG
#define msg(str,args...) xlog("%s:-%s-%5d---" str,getcurtime(), __FILE__,__LINE__,##args) ;\
	fflush(stdout);
#else
#define msg(str,args...)
#endif

void initDB(DATABASE *db);
sb4 oraError8(DATABASE *db);
sword prepareStmt8(DATABASE *db, OCIStmt **stmt, text *sql);
sword executeUpdate8(DATABASE *db, OCIStmt *stmt);
sword releaseStmt8(DATABASE *db, OCIStmt *stmt);
sword prepareExecute8(DATABASE *db, OCIStmt **stmt, text *sql);
sword executeQuery8(DATABASE *db, OCIStmt *stmt);
sword prepareQuery8(DATABASE *db, OCIStmt **stmt, text *sql);
sword disconnectDB(DATABASE *db);
sword connectDB(DATABASE *db, text *username, text *password, text *tnsname,
		ub4 authmode, sword mode);
sword connectSYS(DATABASE *db);
sword LogonDB(DATABASE *db, login *login_info);
void checkerr(OCIError *errhp, sword status);
void printtypename (ub2 type, char *type_name);
void write_ddl(char *file_path, char *ddl);
void write_dif_obj(char *file_path, char *objname);
void init_err_file(char *fname);
void init_msg_file(char *fname);
void finish_err_file();
void write_err_file(char *objtype, char *objname, char *errmessage);
void xlog_out(char *fmt, ...);
void start_status_file(char *fname);
void end_status_file(char *fname);
void alter_schema(char *schema_name, DATABASE *db);

#endif /* COMM_H_ */
