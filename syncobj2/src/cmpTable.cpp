/*
 * cmpTable.cpp
 *
 *  Created on: 2013-4-22
 *      Author: huangbin
 */

#include "comm.h"

#define NPOS 1024
#define MAXLEN 256
#define CREATE_TABLE_DDL 1
#define ALTER_TABLE_ADD_DDL 2

typedef struct _ZERO_COLUMN {
	int col_id;
	char col_name[100];
	struct _ZERO_COLUMN *nxt;
} ZERO_COLUMN;

int zero_col_num = 0;
ZERO_COLUMN *zero_col_list = NULL;

static void describe_column(OCIParam *parmp, ub4 parmcnt, DATABASE *db, TABLE_DEFINE *table_def);
static void describe_type(OCIParam  *type_parmp, DATABASE *db);
static void describe_typeattr(OCIParam *attrlist_parmp, ub4 num_attr, DATABASE *db);
static void describe_typecoll(OCIParam *collelem_parmp, sword coll_typecode, DATABASE *db);
static void describe_typemethodlist(OCIParam *methodlist_parmp, ub4 num_method, text *comment, DATABASE *db);
static void describe_typemethod(OCIParam *method_parmp, text *comment, DATABASE *db);
static void describe_typearg(OCIParam *arglist_parmp, ub1 type, ub4 start, ub4 end, DATABASE *db);
int select_all_table(char *owner, DATABASE *db, int is_src);
void create_table_from_tablelist(DATABASE *db, DATABASE *dbdst, int do_level,
		char *ddl_table_file, char *ddl_file);
int select_table_comments(DATABASE *db, DATABASE *dbdst, int do_level, char *ddl_file);
int select_col_comments(DATABASE *db, DATABASE *dbdst, int do_level, char *ddl_file);
int select_one_table(char *owner, DATABASE *db, char *table_name, DEFTABLE *td);
void create_one_table(DATABASE *db, DATABASE *dbdst, int do_level,
		char *ddl_table_file, char *ddl_file, DEFTABLE *td);
void init_column(DEFCOLUMN *defcolumn);

list<string> addColumnList;
list<string> primary_key;
list<string> all_constraints;
char loadconfig[256];
char redoconfig[256];
char primary_cons[128]={0};
list<UNIQUEKEY*> unique_key;
list<DEFTABLE *> tablelist;
list<DEFTABLE *> tablelist_dst;
list<string> dif_table_list;

extern login login_info1;
extern login login_info2;
extern int do_foreign_key;
extern int do_index;
extern int do_create_dictionary;
extern int do_create_loadconf;
extern int do_create_copyconf;
extern int do_check;
extern int do_table_privilege;
extern int do_default;
extern int do_unique;
extern int init;
extern list<string> view_list;

//select DISTINCT(a.index_name), a.index_type, a.uniqueness, b.column_name from all_indexes a , all_ind_columns b
//where a.index_name= b.index_name and a.table_name= b.table_name and a.owner='TEST' and a.table_name='TAB1';

static void strtoupper(char *str, int str_len) {
	int i;
	for (i = 0; i < str_len; i++) {
		str[i] = toupper(str[i]);
	}
}

int isListExists(list<string> *mylist, char *keystr){
	int ret= 0;
	list<string>::iterator it;

	if(mylist->empty()){
		return ret;
	}
	char tmpstr[128]={0};
	for(it=mylist->begin(); it!=mylist->end(); it++){
		memset(tmpstr, 0, sizeof(tmpstr));
		strcpy(tmpstr, (*it).c_str());
		if(strcasecmp(tmpstr, keystr)==0){
			ret= 1;
			break;
		}
	}
	return ret;
}

TABLEINDEX *hasSameIndexName(list<TABLEINDEX *> *indexlist, char *index_name){
	list<TABLEINDEX *>::iterator it;
	TABLEINDEX *node= NULL;
	TABLEINDEX *ret= NULL;
	for(it=indexlist->begin(); it!=indexlist->end(); it++){
		node=(*it);
		if(node && strcmp(node->index_name, index_name)==0){
			ret= node;
			break;
		}
	}
	return ret;
}

UNIQUEKEY *hasSameUniqueName(list<UNIQUEKEY*> *uniquelist, char *unique_name){
	list<UNIQUEKEY *>::iterator it;
	UNIQUEKEY *node= NULL;
	UNIQUEKEY *ret= NULL;
	for(it=uniquelist->begin(); it!=uniquelist->end(); it++){
		node=(*it);
		if(node && strcmp(node->unique_cons, unique_name)==0){
			ret= node;
			break;
		}
	}
	return ret;
}

DEFUNIQUECONS *hasSameUniqueName2(list<DEFUNIQUECONS*> *uniquelist, char *unique_name){
	list<DEFUNIQUECONS *>::iterator it;
	DEFUNIQUECONS *node= NULL;
	DEFUNIQUECONS *ret= NULL;
	for(it=uniquelist->begin(); it!=uniquelist->end(); it++){
		node=(*it);
		if(node && strcmp(node->cons_name, unique_name)==0){
			ret= node;
			break;
		}
	}
	return ret;
}

int combine_sql(int pos, int *cpos, char *ddl_sql, char *col_type, int col_precision, int col_scale,
		int col_size, char *col_name, char *col_null, char *data_default, int char_length){
	if(*cpos>0){
		pos= pos + sprintf(ddl_sql+pos, ",\n");
	}
	if(strcmp(col_type, "NUMBER")==0 && col_precision>0){
		pos= pos + sprintf(ddl_sql+pos, "\"%s\" %s(%d,%d)",
				col_name, col_type, col_precision, col_scale);
	}else if(strcmp(col_type, "FLOAT")==0 && col_precision>0){
		pos= pos + sprintf(ddl_sql+pos, "\"%s\" %s(%d)", col_name, col_type, col_precision);
	}else if(strcmp(col_type, "CHAR")==0 ||
			strcmp(col_type, "VARCHAR2")==0 || strcmp(col_type, "RAW")==0){
		if(char_length!=0 && col_size!=char_length){
			pos= pos + sprintf(ddl_sql+pos, "\"%s\" %s(%d CHAR)", col_name, col_type, char_length);
		}else{
			pos= pos + sprintf(ddl_sql+pos, "\"%s\" %s(%d)", col_name, col_type, col_size);
		}
	}else if(strcmp(col_type, "NCHAR")==0 || strcmp(col_type, "NVARCHAR2")==0){
		pos= pos + sprintf(ddl_sql+pos, "\"%s\" %s(%d)", col_name, col_type, col_size/2);
	}else{
		pos= pos + sprintf(ddl_sql+pos, "\"%s\" %s", col_name, col_type);
	}
//	if(isListExists(&primary_key, col_name)){
//		pos= pos + sprintf(ddl_sql+pos, " PRIMARY KEY");
//	}
	if(data_default && strlen(data_default)>0 && do_default==1){
		pos= pos + sprintf(ddl_sql+pos, " default %s ", data_default);
	}
	if(col_null[0]=='N' && strcmp(col_type, "CLOB")!=0 && strcmp(col_type, "NCLOB")!=0
			&& strcmp(col_type, "BLOB")!=0)
		pos= pos + sprintf(ddl_sql+pos, " NOT NULL");
	(*cpos)++;
	return pos;
}

int getPartitionInfo(char *tbname, char *owner, DATABASE *db, char *partsql){
	//get partitioning_type, partition_count
	char sqlbuf[1024]={0};
	int ret;
	char partitioning_type[64]={0};
	int partition_count;

	OCIDefine *defcolp[10];
	sword indp[10];

	sprintf(sqlbuf,
			"SELECT PARTITIONING_TYPE,PARTITION_COUNT FROM ALL_PART_TABLES WHERE OWNER='%s' AND TABLE_NAME='%s'", owner, tbname);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2:%s\n", db->errmsg);
		ret = -2;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) partitioning_type,
			(sb4) 64, SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getPartitionInfo:%s\n", db->errmsg);
		ret = -2;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 2,
			(dvoid *) &partition_count,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[1], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getPartitionInfo:%s\n", db->errmsg);
		ret = -2;
	}
	ret = executeQuery8(db, db->stmt);
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute getPartitionInfo:%s\n", db->errmsg);
		ret = -2;
	}
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			msg("partitioning_type=%s, count=%d\n", partitioning_type, partition_count);
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
	//get part_key (column name)
	memset(sqlbuf, 0, sizeof(sqlbuf));
	char column_name[256]={0};
	sprintf(sqlbuf,
			"SELECT COLUMN_NAME FROM ALL_PART_KEY_COLUMNS WHERE OWNER='%s' AND NAME='%s' AND OBJECT_TYPE='TABLE'", owner, tbname);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2:%s\n", db->errmsg);
		ret = -2;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) column_name,
			(sb4) 256, SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getPartitionInfo:%s\n", db->errmsg);
		ret = -2;
	}
	ret = executeQuery8(db, db->stmt);
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute getPartitionInfo:%s\n", db->errmsg);
		ret = -2;
	}
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			msg("COLUMN_NAME=%s\n", column_name);
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
	//get partition_name,high value, high value_length,partition_position
	TAB_PARTITION *tab_node= NULL;
	list<TAB_PARTITION *> partlist;
	char partition_name[256];
	char high_value[1024];
	int value_length;
	int partition_pos;
	char tablespace_name[256];
	char compression[10];

	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf,
			"SELECT partition_name,high_value,high_value_length,partition_position,tablespace_name,compression FROM all_tab_partitions WHERE TABLE_OWNER='%s' AND TABLE_NAME='%s'", owner, tbname);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2:%s\n", db->errmsg);
		ret = -2;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) partition_name,
			(sb4) sizeof(partition_name), SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getPartitionInfo:%s\n", db->errmsg);
		ret = -2;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 2,
			(dvoid *) high_value,
			(sb4) sizeof(high_value), SQLT_STR, (dvoid *) &indp[1], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getPartitionInfo:%s\n", db->errmsg);
		ret = -2;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 3,
			(dvoid *) &value_length,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[2], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getPartitionInfo:%s\n", db->errmsg);
		ret = -2;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 4,
			(dvoid *) &partition_pos,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[3], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getPartitionInfo:%s\n", db->errmsg);
		ret = -2;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 5,
			(dvoid *) tablespace_name,
			(sb4) sizeof(tablespace_name), SQLT_STR, (dvoid *) &indp[4], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getPartitionInfo:%s\n", db->errmsg);
		ret = -2;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 6,
			(dvoid *) compression,
			(sb4) sizeof(compression), SQLT_STR, (dvoid *) &indp[5], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getPartitionInfo:%s\n", db->errmsg);
		ret = -2;
	}
	ret = executeQuery8(db, db->stmt);
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute getPartitionInfo:%s\n", db->errmsg);
		ret = -2;
	}
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			msg("%s,%s,%d,%d,%s,%s\n",partition_name, high_value, value_length, partition_pos, tablespace_name, compression);

			tab_node= (TAB_PARTITION *)malloc(sizeof(TAB_PARTITION));
			sprintf(tab_node->partition_name, "%s", partition_name);
			tab_node->value_length= value_length;
			if(value_length>0){
				sprintf(tab_node->high_value, "%s", high_value);
			}
			tab_node->partition_pos= partition_pos;
			sprintf(tab_node->tablespace_name, "%s", tablespace_name);
			if(strcmp(compression, "DISABLED")==0){
				sprintf(tab_node->compression, "NOCOMPRESS");
			}else{
				sprintf(tab_node->compression, "COMPRESS");
			}
			partlist.push_back(tab_node);
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	int pos=0, cpos= 0;
	pos= pos + sprintf(partsql+pos, "PARTITION BY %s (\"%s\")\n(", partitioning_type, column_name);
	list<TAB_PARTITION *>::iterator it;
	cpos= 0;
	for(it=partlist.begin(); it!=partlist.end(); it++){
		tab_node= (*it);
		if(cpos>0){
			pos= pos + sprintf(partsql+pos, ",\n");
		}
		if(strcmp(partitioning_type, "RANGE")==0){
			pos= pos + sprintf(partsql+pos, "PARTITION \"%s\" VALUES LESS THAN (%s) TABLESPACE \"%s\" %s",
					tab_node->partition_name, tab_node->high_value, tab_node->tablespace_name, tab_node->compression);
		}
		if(strcmp(partitioning_type, "LIST")==0){
			pos= pos + sprintf(partsql+pos, "PARTITION \"%s\" VALUES (%s) TABLESPACE \"%s\" %s",
					tab_node->partition_name, tab_node->high_value, tab_node->tablespace_name, tab_node->compression);
		}
		if(strcmp(partitioning_type, "HASH")==0){
			pos= pos + sprintf(partsql+pos, "PARTITION \"%s\" TABLESPACE \"%s\"",
					tab_node->partition_name, tab_node->tablespace_name);
		}
		cpos++;
		if(tab_node){
			free(tab_node);
		}
	}
	pos= pos + sprintf(partsql+pos, ")");

//	msg("%s\n", partsql);
	return 0;

}

FOREIGN_KEY *get_foreign_node(char *refforname, list<FOREIGN_KEY *> *foreign_key){
	list<FOREIGN_KEY *>::iterator it;
	FOREIGN_KEY *node, *ret= NULL;

	for(it=foreign_key->begin(); it!=foreign_key->end(); it++){
		node= (*it);
		if(node && strcmp(node->refforname, refforname)==0){
			ret= node;
			break;
		}
	}
	return ret;
}

FOREIGN_KEY *get_foreign_node_by_cons(char *froname, list<FOREIGN_KEY *> *foreign_key){
	list<FOREIGN_KEY *>::iterator it;
	FOREIGN_KEY *node, *ret= NULL;

	for(it=foreign_key->begin(); it!=foreign_key->end(); it++){
		node= (*it);
		if(node && strcmp(node->forname, froname)==0){
			ret= node;
			break;
		}
	}
	return ret;
}

void free_foreign_node(list<FOREIGN_KEY *> *foreign_key){
	list<FOREIGN_KEY *>::iterator it;
	FOREIGN_KEY *node;

	for(it=foreign_key->begin(); it!=foreign_key->end(); it++){
		node= (*it);
		if(node){
			free(node);
		}
	}
}

int add_index(char *tbname, DATABASE *db, DATABASE *dbdst, char *ddl_file, int do_level){
	char sqlbuf[1024]={0};
	int ret;
	OCIDefine *defcolp[10];
	sword indp[10];
	list<TABLEINDEX *> indexlist;
	list<TABLEINDEX *>::iterator it;
	list<string>::iterator it2;
	TABLEINDEX *node;
	char ddl_sql[1024]={0};

	//start Foreign key
	//REFERENCES "TEST"."LESSON" ("ID") ENABLE
	char index_type[10];
	char uniqueness[10];
	char index_name[256];
	char table_name[256];
	char colname[256];

	indexlist.clear();
	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf,
			"select distinct t1.index_type,t1.UNIQUENESS,t1.INDEX_NAME,t1.TABLE_NAME,t2.COLUMN_NAME from all_indexes t1,all_ind_columns t2 \
			where t1.INDEX_NAME= t2.INDEX_NAME and t1.TABLE_NAME=t2.TABLE_NAME and t1.TABLE_NAME='%s' and t1.OWNER='%s'",
			tbname, login_info1.schema);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 add_index:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) index_type,
			(sb4) sizeof(index_type), SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 2,
			(dvoid *) uniqueness,
			(sb4) sizeof(uniqueness), SQLT_STR, (dvoid *) &indp[1], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[2], db->errhp,
			(ub4) 3,
			(dvoid *) index_name,
			(sb4) sizeof(index_name), SQLT_STR, (dvoid *) &indp[2], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[3], db->errhp,
			(ub4) 4,
			(dvoid *) table_name,
			(sb4) sizeof(table_name), SQLT_STR, (dvoid *) &indp[3], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[4], db->errhp,
			(ub4) 5,
			(dvoid *) colname,
			(sb4) sizeof(colname), SQLT_STR, (dvoid *) &indp[4], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_index:%s\n", db->errmsg);
		ret = -2;
	}
	ret = executeQuery8(db, db->stmt);
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute add_index:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			msg("index_name=%s, index_type=%s, table_name=%s, unique=%s\n", index_name, index_type, table_name, uniqueness);
			ret= isListExists(&all_constraints, index_name);
			if(ret)
				continue;
//			if(strcmp(primary_cons, index_name)==0){
//				continue;
//			}

			if(indexlist.size()==0){
				node= (TABLEINDEX *)malloc(sizeof(TABLEINDEX));
				memset(node->index_type, 0, sizeof(node->index_type));
				memset(node->index_name, 0, sizeof(node->index_name));
				memset(node->table_name, 0, sizeof(node->table_name));
				memset(node->uniqueness, 0, sizeof(node->uniqueness));
				node->colname= new list<string>;
				node->colname->push_back(colname);
				strcpy(node->index_type, index_type);
				strcpy(node->index_name, index_name);
				strcpy(node->table_name, table_name);
				strcpy(node->uniqueness, uniqueness);
				indexlist.push_back(node);
			}else{
				node= hasSameIndexName(&indexlist, index_name);
				if(node){
					node->colname->push_back(colname);
				}else{
					node= (TABLEINDEX *)malloc(sizeof(TABLEINDEX));
					memset(node->index_type, 0, sizeof(node->index_type));
					memset(node->index_name, 0, sizeof(node->index_name));
					memset(node->table_name, 0, sizeof(node->table_name));
					memset(node->uniqueness, 0, sizeof(node->uniqueness));
					node->colname= new list<string>;
					node->colname->push_back(colname);
					strcpy(node->index_type, index_type);
					strcpy(node->index_name, index_name);
					strcpy(node->table_name, table_name);
					strcpy(node->uniqueness, uniqueness);
					indexlist.push_back(node);
				}
			}

		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	if(indexlist.size()>0){
		for(it=indexlist.begin(); it!=indexlist.end(); it++){
			node= (*it);
			if(!node)
				continue;
			memset(ddl_sql, 0, sizeof(ddl_sql));
			int pos= 0;
			int cpos= 0;
			int unbitmap= strcmp(node->index_type, "BITMAP");
			int nonunique= strcmp(node->uniqueness, "UNIQUE");
			if(!unbitmap){
				pos= pos+sprintf(ddl_sql+pos, "CREATE %s INDEX \"%s\".\"%s\" ON %s.\"%s\" (",
						node->index_type, login_info2.schema, node->index_name, login_info2.schema, tbname);
			}else if(!nonunique){
				pos= pos+sprintf(ddl_sql+pos, "CREATE %s INDEX \"%s\".\"%s\" ON %s.\"%s\" (",
						node->uniqueness, login_info2.schema, node->index_name, login_info2.schema, tbname);
			}else{
				pos= pos+sprintf(ddl_sql+pos, "CREATE INDEX \"%s\".\"%s\" ON %s.\"%s\" (",
						login_info2.schema, node->index_name, login_info2.schema, tbname);
			}
			for(it2=node->colname->begin(); it2!=node->colname->end(); it2++){
				string tmp= (*it2);
				if(cpos>0){
					pos= pos+sprintf(ddl_sql+pos, ",");
				}
				pos= pos+sprintf(ddl_sql+pos, "\"%s\"", tmp.c_str());
				cpos++;
			}
			pos= pos+sprintf(ddl_sql+pos, ")\n");

			if(do_level==1){
				prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
			}
			write_ddl(ddl_file, ddl_sql);
			msg("index_sql=%s\n", ddl_sql);
			delete node->colname;
			free(node);
		}
	}

	return ret;
}

void add_index2(DEFTABLE *td, int do_level, char *ddl_file, DATABASE *dbdst){
	list<TABLEINDEX *> indexlist;
	DEFCOLUMN *cd;
	TABLEINDEX *node;
	list<TABLEINDEX *>::iterator it;
	list<string>::iterator it2;
	int col;
	int ret;
	char ddl_sql[1024];

	indexlist.clear();
	all_constraints.clear();
	for(col=0; col<td->col_count; col++){
		cd= &td->defcolumn[col];
		if(strlen(cd->constraint_name)>0){
			all_constraints.push_back(cd->constraint_name);
		}
	}
	for(col=0; col<td->col_count; col++){
		cd= &td->defcolumn[col];
		if(strlen(cd->index_name)<=0)
			continue;
		ret= isListExists(&all_constraints, cd->index_name);
		if(ret)
			continue;
//		msg("index_namexxx=%s\n", cd->index_name);
		if(indexlist.size()==0){
			node= (TABLEINDEX *)malloc(sizeof(TABLEINDEX));
			memset(node->index_type, 0, sizeof(node->index_type));
			memset(node->index_name, 0, sizeof(node->index_name));
			memset(node->table_name, 0, sizeof(node->table_name));
			memset(node->uniqueness, 0, sizeof(node->uniqueness));
			node->colname= new list<string>;
			node->colname->push_back(cd->col_name);
			strcpy(node->index_type, cd->index_type);
			strcpy(node->index_name, cd->index_name);
			strcpy(node->table_name, td->table_name);
			strcpy(node->uniqueness, cd->uniqueness);
			indexlist.push_back(node);
		}else{
			node= hasSameIndexName(&indexlist, cd->index_name);
			if(node){
				ret= isListExists(node->colname, cd->col_name);
				if(!ret){
					node->colname->push_back(cd->col_name);
				}
			}else{
				node= (TABLEINDEX *)malloc(sizeof(TABLEINDEX));
				memset(node->index_type, 0, sizeof(node->index_type));
				memset(node->index_name, 0, sizeof(node->index_name));
				memset(node->table_name, 0, sizeof(node->table_name));
				memset(node->uniqueness, 0, sizeof(node->uniqueness));
				node->colname= new list<string>;
				node->colname->push_back(cd->col_name);
				strcpy(node->index_type, cd->index_type);
				strcpy(node->index_name, cd->index_name);
				strcpy(node->table_name, td->table_name);
				strcpy(node->uniqueness, cd->uniqueness);
				indexlist.push_back(node);
			}
		}
	}
	if(indexlist.size()>0){
		for(it=indexlist.begin(); it!=indexlist.end(); it++){
			node= (*it);
			if(!node)
				continue;
			memset(ddl_sql, 0, sizeof(ddl_sql));
			int pos= 0;
			int cpos= 0;
			int unbitmap= strcmp(node->index_type, "BITMAP");
			int nonunique= strcmp(node->uniqueness, "UNIQUE");
			if(!unbitmap){
				pos= pos+sprintf(ddl_sql+pos, "CREATE %s INDEX \"%s\".\"%s\" ON %s.\"%s\" (",
						node->index_type, login_info2.schema, node->index_name, login_info2.schema, td->table_name);
			}else if(!nonunique){
				pos= pos+sprintf(ddl_sql+pos, "CREATE %s INDEX \"%s\".\"%s\" ON %s.\"%s\" (",
						node->uniqueness, login_info2.schema, node->index_name, login_info2.schema, td->table_name);
			}else{
				pos= pos+sprintf(ddl_sql+pos, "CREATE INDEX \"%s\".\"%s\" ON %s.\"%s\" (",
						login_info2.schema, node->index_name, login_info2.schema, td->table_name);
			}
			for(it2=node->colname->begin(); it2!=node->colname->end(); it2++){
				string tmp= (*it2);
				if(cpos>0){
					pos= pos+sprintf(ddl_sql+pos, ",");
				}
				pos= pos+sprintf(ddl_sql+pos, "\"%s\"", tmp.c_str());
				cpos++;
			}
			pos= pos+sprintf(ddl_sql+pos, ")\n");

			if(do_level==1){
				prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
			}
			write_ddl(ddl_file, ddl_sql);
			msg("index_sql=%s\n", ddl_sql);
			delete node->colname;
			free(node);
		}
	}
}

int add_foreign_key2(DATABASE *db, DATABASE *dbdst, char *ddl_file, int do_level){
	char sqlbuf[1024]={0};
	int ret;
	OCIDefine *defcolp[11];
	sword indp[11];
	list<FOREIGN_KEY *> foreign_key;
	list<FOREIGN_KEY *>::iterator it;
	list<string>::iterator it1;
	FOREIGN_KEY *node;
	char ddl_sql[1024]={0};

	//start Foreign key
	//REFERENCES "TEST"."LESSON" ("ID") ENABLE
	char forname[256]={0};
	char fortable_name[256]={0};
	char forcol[256]={0};
	int postion= 0;
	char fornameref[256]={0};
	char tablenameref[256]={0};
	char colnameref[256]={0};
	char delete_rule[12]={0};
	char status[10]={0};
	int i= 0;
	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf,
			"select a.CONSTRAINT_NAME,a.TABLE_NAME,a.COLUMN_NAME,a.POSITION,c.delete_rule,c.status,b.CONSTRAINT_NAME,b.TABLE_NAME,b.COLUMN_NAME from ALL_CONS_COLUMNS a, ALL_CONS_COLUMNS b,\
     (select constraint_name,table_name,r_owner,r_constraint_name,delete_rule,status from ALL_CONSTRAINTS where owner='%s' and constraint_type='R') c \
     where a.CONSTRAINT_NAME=c.constraint_name and b.CONSTRAINT_NAME=c.r_constraint_name and a.owner='%s' and a.OWNER=b.OWNER and a.POSITION=b.POSITION \
     order by a.table_name,a.position",
			login_info1.schema, login_info1.schema);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 add_foreign_key:%s\n", db->errmsg);
		return -1;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) forname,
			(sb4) sizeof(forname), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_foreign_key2:%s\n", db->errmsg);
		ret = -2;
		goto add_foreign_key2_exit;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) fortable_name,
			(sb4) sizeof(fortable_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_foreign_key2:%s\n", db->errmsg);
		ret = -2;
		goto add_foreign_key2_exit;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) forcol,
			(sb4) sizeof(forcol), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_foreign_key2:%s\n", db->errmsg);
		ret = -2;
		goto add_foreign_key2_exit;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &postion,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_foreign_key2:%s\n", db->errmsg);
		ret = -2;
		goto add_foreign_key2_exit;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) delete_rule,
			(sb4) sizeof(delete_rule), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_foreign_key2:%s\n", db->errmsg);
		ret = -2;
		goto add_foreign_key2_exit;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) status,
			(sb4) sizeof(status), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_foreign_key2:%s\n", db->errmsg);
		ret = -2;
		goto add_foreign_key2_exit;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) fornameref,
			(sb4) sizeof(fornameref), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_foreign_key2:%s\n", db->errmsg);
		ret = -2;
		goto add_foreign_key2_exit;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) tablenameref,
			(sb4) sizeof(tablenameref), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_foreign_key2:%s\n", db->errmsg);
		ret = -2;
		goto add_foreign_key2_exit;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) colnameref,
			(sb4) sizeof(colnameref), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_foreign_key2:%s\n", db->errmsg);
		ret = -2;
		goto add_foreign_key2_exit;
	}
	i++;

	ret = executeQuery8(db, db->stmt);
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute add_foreign_key:%s\n", db->errmsg);
		ret = -2;
		goto add_foreign_key2_exit;
	}
	msg("execute successful\n");
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			if(dif_table_list.size()>0 && !isListExists(&dif_table_list, fortable_name)){
				msg("filter %s\n", fortable_name);
				continue;
			}
			msg("table=%s,forcol=%s,forname=%s,fornameref=%s,reftable=%s,refcol=%s,delete_rule=%s,status=%s\n",
					fortable_name, forcol, forname, fornameref,tablenameref,colnameref, delete_rule, status);
			if(foreign_key.size()==0){
				node= (FOREIGN_KEY *)malloc(sizeof(FOREIGN_KEY));
				node->col_name = new list<string>;
				node->refcol= new list<string>;
				memset(node->forname, 0, sizeof(node->forname));
				memset(node->refforname, 0, sizeof(node->refforname));
				memset(node->reftable, 0, sizeof(node->reftable));
				if(strstr(delete_rule, "SET NULL")){
					node->delete_rule= ON_DELETE_SETNULL;
				}else if(strstr(delete_rule, "CASCADE")){
					node->delete_rule= ON_DELETE_CASCADE;
				}else{
					node->delete_rule= ON_DELETE_NOACT;
				}
				if(strstr(status, "ENABLED")){
					node->status= STATUS_ENABLE;
				}else{
					node->status= STATUS_DISABLE;
				}
				node->col_name->push_back(forcol);
				node->refcol->push_back(colnameref);
				strcpy(node->forname, forname);
				strcpy(node->refforname, fornameref);
				strcpy(node->reftable, tablenameref);
				strcpy(node->fortable, fortable_name);
				foreign_key.push_back(node);
			}else{
				node= get_foreign_node_by_cons(forname, &foreign_key);
				if(node){
					node->col_name->push_back(forcol);
					node->refcol->push_back(colnameref);
				}else{
					node= (FOREIGN_KEY *)malloc(sizeof(FOREIGN_KEY));
					node->col_name = new list<string>;
					node->refcol= new list<string>;
					memset(node->forname, 0, sizeof(node->forname));
					memset(node->refforname, 0, sizeof(node->refforname));
					memset(node->reftable, 0, sizeof(node->reftable));
					if(strstr(delete_rule, "SET NULL")){
						node->delete_rule= ON_DELETE_SETNULL;
					}else if(strstr(delete_rule, "CASCADE")){
						node->delete_rule= ON_DELETE_CASCADE;
					}else{
						node->delete_rule= ON_DELETE_NOACT;
					}
					if(strstr(status, "ENABLED")){
						node->status= STATUS_ENABLE;
					}else{
						node->status= STATUS_DISABLE;
					}
					node->col_name->push_back(forcol);
					node->refcol->push_back(colnameref);
					strcpy(node->forname, forname);
					strcpy(node->refforname, fornameref);
					strcpy(node->reftable, tablenameref);
					strcpy(node->fortable, fortable_name);
					foreign_key.push_back(node);
				}
			}
		}
	}
	ret= 0;

	add_foreign_key2_exit:

	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	if(foreign_key.size()==0){
		return 0;
	} else {
		int pos = 0;
		int cpos = 0;
		for (it = foreign_key.begin(); it != foreign_key.end(); it++) {
			memset(ddl_sql, 0, sizeof(ddl_sql));
			node = (*it);
			if (node->col_name->size() > 0 && node->refcol->size() > 0) {
				pos = pos + sprintf(ddl_sql + pos, "ALTER TABLE %s.\"%s\" ADD \n", login_info2.schema, node->fortable);
				pos = pos + sprintf(ddl_sql + pos, "CONSTRAINT \"%s\" FOREIGN KEY (", node->forname);
				cpos = 0;
				for (it1 = node->col_name->begin();
						it1 != node->col_name->end(); it1++) {
					if (cpos > 0) {
						pos = pos + sprintf(ddl_sql + pos, ",");
					}
					pos = pos
							+ sprintf(ddl_sql + pos, "\"%s\"", (*it1).c_str());
					cpos++;
				}
				delete node->col_name;
				pos = pos + sprintf(ddl_sql + pos, ") REFERENCES %s.\"%s\" (", login_info2.schema, node->reftable);
				cpos = 0;
				for (it1 = node->refcol->begin(); it1 != node->refcol->end(); it1++) {
					if (cpos > 0) {
						pos = pos + sprintf(ddl_sql + pos, ",");
					}
					pos = pos	+ sprintf(ddl_sql + pos, "\"%s\"", (*it1).c_str());
					cpos++;
				}
				delete node->refcol;
				pos = pos + sprintf(ddl_sql + pos, ")");
				if (node->delete_rule == ON_DELETE_SETNULL) {
					pos = pos + sprintf(ddl_sql + pos, " ON DELETE SET NULL");
				} else if (node->delete_rule == ON_DELETE_CASCADE) {
					pos = pos + sprintf(ddl_sql + pos, " ON DELETE CASCADE");
				}
				if (node->status == STATUS_ENABLE) {
					pos = pos + sprintf(ddl_sql + pos, " ENABLE");
				} else {
					pos = pos + sprintf(ddl_sql + pos, " DISABLE");
				}
				msg("do_foreign_key=%s\n", ddl_sql);
				if (do_level == 1) {
					prepareExecute8(dbdst, &dbdst->stmt, (text *) ddl_sql);
				}
				write_ddl(ddl_file, ddl_sql);
				pos = 0;

			}
		}
		return 1;

		free_foreign_node(&foreign_key);
	}

	return ret;

}

int add_foreign_key(char *tbname, DATABASE *db, DATABASE *dbdst, char *ddl_file, int do_level){
	char sqlbuf[1024]={0};
	int ret;
	OCIDefine *defcolp[10];
	sword indp[10];
	list<FOREIGN_KEY *> foreign_key;
	list<FOREIGN_KEY *>::iterator it;
	list<string>::iterator it1;
	FOREIGN_KEY *node;
	char ddl_sql[1024]={0};

	//start Foreign key
	//REFERENCES "TEST"."LESSON" ("ID") ENABLE
	char forname[256];
	char forcol[256];
	int postion= 0;
	char fornameref[256];
	char delete_rule[12];
	char status[10];
	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf,
			"select distinct(a.COLUMN_NAME),a.CONSTRAINT_NAME,b.R_CONSTRAINT_NAME,b.DELETE_RULE,b.STATUS,a.position from all_cons_columns a,all_constraints b where a.CONSTRAINT_NAME=b.CONSTRAINT_NAME and b.TABLE_NAME='%s' \
			AND B.OWNER='%s' AND B.CONSTRAINT_TYPE='R' order by a.position",
			tbname, login_info1.schema);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 add_foreign_key:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) forcol,
			(sb4) sizeof(forcol), SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 2,
			(dvoid *) forname,
			(sb4) sizeof(forname), SQLT_STR, (dvoid *) &indp[1], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[2], db->errhp,
			(ub4) 3,
			(dvoid *) fornameref,
			(sb4) sizeof(fornameref), SQLT_STR, (dvoid *) &indp[2], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[3], db->errhp,
			(ub4) 4,
			(dvoid *) delete_rule,
			(sb4) sizeof(delete_rule), SQLT_STR, (dvoid *) &indp[3], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[4], db->errhp,
			(ub4) 5,
			(dvoid *) status,
			(sb4) sizeof(status), SQLT_STR, (dvoid *) &indp[4], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[5], db->errhp,
			(ub4) 6,
			(dvoid *) &postion,
			(sb4) sizeof(sb4), SQLT_INT, (dvoid *) &indp[5], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_foreign_key:%s\n", db->errmsg);
		ret = -2;
	}
	ret = executeQuery8(db, db->stmt);
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute add_foreign_key:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			msg("%s,%s,%s,%s,%s\n", forcol, forname, fornameref, delete_rule, status);
			if(foreign_key.size()==0){
				node= (FOREIGN_KEY *)malloc(sizeof(FOREIGN_KEY));
				node->col_name = new list<string>;
				node->refcol= new list<string>;
				memset(node->forname, 0, sizeof(node->forname));
				memset(node->refforname, 0, sizeof(node->refforname));
				memset(node->reftable, 0, sizeof(node->reftable));
				if(strstr(delete_rule, "SET NULL")){
					node->delete_rule= ON_DELETE_SETNULL;
				}else if(strstr(delete_rule, "CASCADE")){
					node->delete_rule= ON_DELETE_CASCADE;
				}else{
					node->delete_rule= ON_DELETE_NOACT;
				}
				if(strstr(status, "ENABLED")){
					node->status= STATUS_ENABLE;
				}else{
					node->status= STATUS_DISABLE;
				}
				node->col_name->push_back(forcol);
				strcpy(node->forname, forname);
				strcpy(node->refforname, fornameref);
				foreign_key.push_back(node);
			}else{
				node= get_foreign_node_by_cons(forname, &foreign_key);
				if(node){
					node->col_name->push_back(forcol);
				}else{
					node= (FOREIGN_KEY *)malloc(sizeof(FOREIGN_KEY));
					node->col_name = new list<string>;
					node->refcol= new list<string>;
					memset(node->forname, 0, sizeof(node->forname));
					memset(node->refforname, 0, sizeof(node->refforname));
					memset(node->reftable, 0, sizeof(node->reftable));
					if(strstr(delete_rule, "SET NULL")){
						node->delete_rule= ON_DELETE_SETNULL;
					}else if(strstr(delete_rule, "CASCADE")){
						node->delete_rule= ON_DELETE_CASCADE;
					}else{
						node->delete_rule= ON_DELETE_NOACT;
					}
					if(strstr(status, "ENABLED")){
						node->status= STATUS_ENABLE;
					}else{
						node->status= STATUS_DISABLE;
					}
					node->col_name->push_back(forcol);
					strcpy(node->forname, forname);
					strcpy(node->refforname, fornameref);
					foreign_key.push_back(node);
				}
			}
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	//end foreign key

	if(foreign_key.size()==0){
		return 0;
	}

	char refcolname[256];
	char refconstr[256];
	char reftable[256];
	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf,
			"SELECT COLUMN_NAME,CONSTRAINT_NAME,TABLE_NAME FROM ALL_CONS_COLUMNS WHERE CONSTRAINT_NAME IN \
			(SELECT R_CONSTRAINT_NAME FROM ALL_CONSTRAINTS WHERE TABLE_NAME='%s' AND OWNER='%s' AND \
			CONSTRAINT_TYPE='R') AND OWNER='%s'",
			tbname, login_info1.schema, login_info1.schema);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 add_foreign_key:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) refcolname,
			(sb4) sizeof(refcolname), SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 2,
			(dvoid *) refconstr,
			(sb4) sizeof(refconstr), SQLT_STR, (dvoid *) &indp[1], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[2], db->errhp,
			(ub4) 3,
			(dvoid *) reftable,
			(sb4) sizeof(reftable), SQLT_STR, (dvoid *) &indp[2], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_foreign_key:%s\n", db->errmsg);
		ret = -2;
	}
	ret = executeQuery8(db, db->stmt);
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute add_foreign_key:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			node= get_foreign_node(refconstr, &foreign_key);
			if(node){
				node->refcol->push_back(refcolname);
				strcpy(node->reftable, reftable);
			}
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	if(foreign_key.size()==0){
		return 0;
	}else{
		int pos=0;
		int cpos= 0;
		for(it=foreign_key.begin(); it!=foreign_key.end(); it++){
			node= (*it);
			if(node->col_name->size()>0 && node->refcol->size()>0){
			pos= pos + sprintf(ddl_sql+pos, "ALTER TABLE %s.\"%s\" ADD \n", login_info2.schema, tbname);
			pos= pos + sprintf(ddl_sql+pos, "CONSTRAINT \"%s\" FOREIGN KEY (",node->forname);
			cpos= 0;
			for(it1=node->col_name->begin(); it1!=node->col_name->end(); it1++){
				if(cpos>0){
					pos= pos + sprintf(ddl_sql+pos, ",");
				}
				pos= pos + sprintf(ddl_sql+pos, "\"%s\"",(*it1).c_str());
				cpos++;
			}
			delete node->col_name;
			pos= pos + sprintf(ddl_sql+pos, ") REFERENCES %s.\"%s\" (", login_info2.schema, node->reftable);
			cpos= 0;
			for(it1=node->refcol->begin(); it1!=node->refcol->end(); it1++){
				if(cpos>0){
					pos= pos + sprintf(ddl_sql+pos, ",");
				}
				pos= pos + sprintf(ddl_sql+pos, "\"%s\"",(*it1).c_str());
				cpos++;
			}
			delete node->refcol;
			pos= pos + sprintf(ddl_sql+pos, ")");
			if(node->delete_rule==ON_DELETE_SETNULL){
				pos= pos + sprintf(ddl_sql+pos, " ON DELETE SET NULL");
			}else if(node->delete_rule==ON_DELETE_CASCADE){
				pos= pos + sprintf(ddl_sql+pos, " ON DELETE CASCADE");
			}
			if(node->status==STATUS_ENABLE){
				pos= pos + sprintf(ddl_sql+pos, " ENABLE");
			}else{
				pos= pos + sprintf(ddl_sql+pos, " DISABLE");
			}
			if(do_level==1){
				prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
			}
			write_ddl(ddl_file, ddl_sql);
			pos= 0;
			memset(ddl_sql, 0, sizeof(ddl_sql));
			}
		}
		return 1;

		free_foreign_node(&foreign_key);
	}
	return 0;
}

int grant_table_privs(char *tbname, DATABASE *db, DATABASE *dbdst, char *ddl_file, int do_level){

	char grantee[128];
	char privilege[20];
	char ddl_sql[1024]={0};
	char sqlbuf[256];
	char grantable[5];
	OCIDefine *defcolp[3];
	sword indp[3];
	int ret=0;
	int pos= 0;

	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf,
			"select GRANTEE,PRIVILEGE,GRANTABLE from dba_tab_privs where table_name='%s' and owner='%s' ORDER BY GRANTABLE",
			tbname, login_info1.schema);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 grant_table_privs:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) grantee,
			(sb4) sizeof(grantee), SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 2,
			(dvoid *) privilege,
			(sb4) sizeof(privilege), SQLT_STR, (dvoid *) &indp[1], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[2], db->errhp,
			(ub4) 3,
			(dvoid *) grantable,
			(sb4) sizeof(grantable), SQLT_STR, (dvoid *) &indp[2], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos grant_table_privs:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute grant_table_privs:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
	for (;;) {
		pos= 0;
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			if(strlen(privilege)>0 && strlen(grantable)>0){
				pos= pos + sprintf(ddl_sql, "GRANT %s ON %s.\"%s\" to %s",
						privilege, login_info2.schema, tbname, grantee);
				if(strstr(grantable, "YES")){
					pos= pos + sprintf(ddl_sql+pos, " WITH GRANT OPTION");
				}
				if(do_level==1){
					prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
				}
				write_ddl(ddl_file, ddl_sql);
				memset(privilege, 0, sizeof(privilege));
				memset(grantable, 0, sizeof(grantable));
				memset(grantee, 0, sizeof(grantee));
			}
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	return ret;

}

int grant_table_privs2(DATABASE *db, DATABASE *dbdst, char *ddl_file, int do_level){

	char grantee[128]={0};
	char privilege[20]={0};
	char ddl_sql[1024]={0};
	char sqlbuf[256];
	char grantable[5]={0};
	char table_name[256]={0};
	OCIDefine *defcolp[6];
	sword indp[6];
	int ret=0;
	int pos= 0;
	int i= 0;

	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf,
			"select TABLE_NAME,GRANTEE,PRIVILEGE,GRANTABLE from dba_tab_privs where owner='%s' and TABLE_NAME NOT LIKE 'BIN$%' ORDER BY TABLE_NAME,GRANTABLE",
			login_info1.schema);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 grant_table_privs:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) table_name,
			(sb4) sizeof(table_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	i++;
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos grant_table_privs2:%s\n", db->errmsg);
		ret = -2;
		goto grant_table_privs2_exit;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) grantee,
			(sb4) sizeof(grantee), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	i++;
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos grant_table_privs2:%s\n", db->errmsg);
		ret = -2;
		goto grant_table_privs2_exit;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) privilege,
			(sb4) sizeof(privilege), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	i++;
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos grant_table_privs2:%s\n", db->errmsg);
		ret = -2;
		goto grant_table_privs2_exit;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) grantable,
			(sb4) sizeof(grantable), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos grant_table_privs2:%s\n", db->errmsg);
		ret = -2;
		goto grant_table_privs2_exit;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute grant_table_privs2:%s\n", db->errmsg);
		ret = -2;
		goto grant_table_privs2_exit;
	}

	for (;;) {
		pos= 0;
		memset(ddl_sql, 0, sizeof(ddl_sql));
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			if(strlen(privilege)>0 && strlen(grantable)>0){
				pos= pos + sprintf(ddl_sql, "GRANT %s ON %s.\"%s\" to %s",
						privilege, login_info2.schema, table_name, grantee);
				if(strstr(grantable, "YES")){
					pos= pos + sprintf(ddl_sql+pos, " WITH GRANT OPTION");
				}
				msg("grant_privilege=%s\n", ddl_sql);
				if(do_level==1){
					prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
				}
				write_ddl(ddl_file, ddl_sql);
				memset(privilege, 0, sizeof(privilege));
				memset(grantable, 0, sizeof(grantable));
				memset(grantee, 0, sizeof(grantee));
				memset(table_name, 0, sizeof(table_name));
			}
		}
	}
	grant_table_privs2_exit:
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	return ret;

}

void add_check_constraint2(DEFTABLE *td, int do_level, char *ddl_file, DATABASE *dbdst){
	DEFCOLUMN *cd;
	int col;
	int ret;
	int pos= 0;
	char ddl_sql[1024];
	int ci= 0;
	char cons_name[256];
	list<string> conslist;

	conslist.clear();
	for(col=0; col<td->col_count; col++){
		cd= &td->defcolumn[col];
		if(cd->col_isck!='1'){
			continue;
		}
		if(strlen(cd->constraint_name)>0 && strlen(cd->search_cond)>0){
			ret= isListExists(&conslist, cd->constraint_name);
			if(ret)
				continue;

			if(strstr(cd->search_cond, "IS NOT NULL")){
				continue;
			}
			conslist.push_back(cd->constraint_name);
			memset(ddl_sql, 0, sizeof(ddl_sql));
			pos= 0;
			pos= pos + sprintf(ddl_sql, "ALTER TABLE %s.\"%s\" ADD CONSTRAINT \"%s\" CHECK (%s)",
					login_info2.schema, td->table_name, cd->constraint_name, cd->search_cond);
			if(strstr(cd->status, "ENABLED")){
				pos= pos + sprintf(ddl_sql+pos, " ENABLE");
			}else{
				pos= pos + sprintf(ddl_sql+pos, " DISABLE");
			}
			msg("do_check=%s\n", ddl_sql);
			if(do_level==1){
				ret= prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
				if(ret!=OCI_SUCCESS && dbdst->errcode==2264){
					pos= 0;
					memset(ddl_sql, 0, sizeof(ddl_sql));
					memset(cons_name, 0, sizeof(cons_name));
					if(strlen(td->table_name)<26){
						sprintf(cons_name, "c_%s_%d", td->table_name, ci++);
					}else{
						char tmp[26]={0};
						strncpy(tmp, td->table_name, 25);
						sprintf(cons_name, "c_%s_%d", tmp, ci++);
					}
					pos= pos + sprintf(ddl_sql, "ALTER TABLE %s.\"%s\" ADD CONSTRAINT \"%s\" CHECK (%s)",
							login_info2.schema, td->table_name, cons_name, cd->search_cond);
					if(strstr(cd->status, "ENABLED")){
						pos= pos + sprintf(ddl_sql+pos, " ENABLE");
					}else{
						pos= pos + sprintf(ddl_sql+pos, " DISABLE");
					}
					prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
				}
			}
			write_ddl(ddl_file, ddl_sql);
		}

	}
}

int add_check_constraint(char *tbname, DATABASE *db, DATABASE *dbdst, char *ddl_file, int do_level){
	char cons_name[256];
	char search_cond[1024];
	char ddl_sql[1024]={0};
	char sqlbuf[256];
	char status[10];
	OCIDefine *defcolp[3];
	sword indp[3];
	int ret=0;
	int pos= 0;
	int ci= 0;
	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf,
			"SELECT CONSTRAINT_NAME,SEARCH_CONDITION,STATUS FROM ALL_CONSTRAINTS WHERE OWNER='%s' AND TABLE_NAME='%s' AND CONSTRAINT_TYPE='C'",
			login_info1.schema, tbname);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 add_check_constraint:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) cons_name,
			(sb4) sizeof(cons_name), SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 2,
			(dvoid *) search_cond,
			(sb4) sizeof(search_cond), SQLT_STR, (dvoid *) &indp[1], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[2], db->errhp,
			(ub4) 3,
			(dvoid *) status,
			(sb4) sizeof(status), SQLT_STR, (dvoid *) &indp[2], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos add_check_constraint:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute add_check_constraint:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
	for (;;) {

//		if(strncmp(cons_name, "SYS_", 4)==0)
//			continue;

		pos= 0;
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			if(strlen(cons_name)>0 && strlen(search_cond)>0){
				memset(ddl_sql, 0, sizeof(ddl_sql));
				pos= pos + sprintf(ddl_sql, "ALTER TABLE %s.\"%s\" ADD CONSTRAINT \"%s\" CHECK (%s)",
						login_info2.schema, tbname, cons_name, search_cond);
				if(strstr(status, "ENABLED")){
					pos= pos + sprintf(ddl_sql+pos, " ENABLE");
				}else{
					pos= pos + sprintf(ddl_sql+pos, " DISABLE");
				}
				if(do_level==1){
					ret= prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
					if(ret!=OCI_SUCCESS && dbdst->errcode==2264){
						pos= 0;
						memset(ddl_sql, 0, sizeof(ddl_sql));
						memset(cons_name, 0, sizeof(cons_name));
						if(strlen(tbname)<26){
							sprintf(cons_name, "c_%s_%d", tbname, ci++);
						}else{
							char tmp[26]={0};
							strncpy(tmp, tbname, 25);
							sprintf(cons_name, "c_%s_%d", tmp, ci++);
						}
						pos= pos + sprintf(ddl_sql, "ALTER TABLE %s.\"%s\" ADD CONSTRAINT \"%s\" CHECK (%s)",
								login_info2.schema, tbname, cons_name, search_cond);
						if(strstr(status, "ENABLED")){
							pos= pos + sprintf(ddl_sql+pos, " ENABLE");
						}else{
							pos= pos + sprintf(ddl_sql+pos, " DISABLE");
						}
						prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
					}
				}
				write_ddl(ddl_file, ddl_sql);
				memset(cons_name, 0, sizeof(cons_name));
				memset(search_cond, 0, sizeof(search_cond));
			}
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	return ret;
}

int add_unique_constraint(char *tbname, DATABASE *db, DATABASE *dbdst, char *ddl_file, int do_level){
	char col_name[256];
	char cons_name[50];
	char ddl_sql[1024]={0};
	char sqlbuf[256];
	char status[10];
	OCIDefine *defcolp[3];
	sword indp[3];
	int ret=0;
	int pos= 0;

	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf,
			"select a.COLUMN_NAME,a.CONSTRAINT_NAME,b.status from all_cons_columns a, all_constraints b where a.CONSTRAINT_NAME=b.CONSTRAINT_NAME and a.OWNER=b.OWNER and\
			b.TABLE_NAME='%s' and b.owner='%s' and b.constraint_type='U'",
			tbname, login_info1.schema);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 add_unique_constraint:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) col_name,
			(sb4) sizeof(col_name), SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 2,
			(dvoid *) cons_name,
			(sb4) sizeof(cons_name), SQLT_STR, (dvoid *) &indp[1], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[2], db->errhp,
			(ub4) 3,
			(dvoid *) status,
			(sb4) sizeof(status), SQLT_STR, (dvoid *) &indp[2], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos unique:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute unique:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
	unique_key.clear();
	UNIQUEKEY *node;
	list<UNIQUEKEY*>::iterator it;
	list<string>::iterator its;
	for (;;) {
		pos= 0;
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {

			if(unique_key.size()==0){
				node= new UNIQUEKEY;
				memset(node->status, 0, sizeof(node->status));
				strcpy(node->status, status);
				memset(node->unique_cons, 0, sizeof(node->unique_cons));
				strcpy(node->unique_cons, cons_name);
				node->unique_col= new list<string>;
				node->unique_col->push_back(col_name);
				unique_key.push_back(node);
//				msg("%s %s %s\n", cons_name, col_name, status);
			}else{
				node= hasSameUniqueName(&unique_key, cons_name);
				if(node){
					node->unique_col->push_back(col_name);
				}else{
					node= new UNIQUEKEY;
					memset(node->status, 0, sizeof(node->status));
					strcpy(node->status, status);
					memset(node->unique_cons, 0, sizeof(node->unique_cons));
					strcpy(node->unique_cons, cons_name);
					node->unique_col= new list<string>;
					node->unique_col->push_back(col_name);
					unique_key.push_back(node);
//					msg("%s %s %s\n", cons_name, col_name, status);
				}
			}
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	for(it=unique_key.begin(); it!=unique_key.end(); it++){
		int pos= 0;
		int cpos= 0;
		node= (*it);
		memset(ddl_sql, 0, sizeof(ddl_sql));
		if(strlen(node->unique_cons)>0 && node->unique_col->size()>0){
			pos= pos + sprintf(ddl_sql+pos, "ALTER TABLE %s.\"%s\" ADD CONSTRAINT \"%s\" unique (",
					login_info2.schema, tbname, node->unique_cons);
			cpos= 0;
			for(its= node->unique_col->begin(); its!=node->unique_col->end(); its++){
				if(cpos>0){
					pos= pos + sprintf(ddl_sql+pos, ", ");
				}
				pos= pos + sprintf(ddl_sql+pos, "%s", (*its).c_str());
				cpos++;
			}
			delete node->unique_col;
			pos= pos + sprintf(ddl_sql+pos, ")");
//			if(strstr(status, "ENABLED")){
//				pos= pos + sprintf(ddl_sql+pos, " ENABLE");
//			}else{
//				pos= pos + sprintf(ddl_sql+pos, " DISABLE");
//			}
			msg("%s\n", ddl_sql);
			if(do_level==1){
				ret= prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
//				if(ret!=OCI_SUCCESS && dbdst->errcode==2264){
//					pos= 0;
//					memset(ddl_sql, 0, sizeof(ddl_sql));
//					memset(cons_name, 0, sizeof(cons_name));
//					if(strlen(tbname)<26){
//						sprintf(cons_name, "c_%s_%d", tbname, ci++);
//					}else{
//						char tmp[26]={0};
//						strncpy(tmp, tbname, 25);
//						sprintf(cons_name, "c_%s_%d", tmp, ci++);
//					}
//					pos= pos + sprintf(ddl_sql, "ALTER TABLE %s.\"%s\" ADD CONSTRAINT \"%s\" CHECK (%s)",
//							login_info2.schema, tbname, cons_name, search_cond);
//					if(strstr(status, "ENABLED")){
//						pos= pos + sprintf(ddl_sql+pos, " ENABLE");
//					}else{
//						pos= pos + sprintf(ddl_sql+pos, " DISABLE");
//					}
//					prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
//				}
			}
			write_ddl(ddl_file, ddl_sql);
		}
		delete node;
	}

	return ret;
}

int getTypeInt(char *type_name){
	int ret=0 ;
	if(strcmp(type_name, "NVARCHAR2")==0 || strcmp(type_name, "VARCHAR2")==0){
		ret= 1;
	}else if(strcmp(type_name, "NUMBER")==0 || strcmp(type_name, "FLOAT")==0){
		ret= 2;
	}else if(strcmp(type_name, "LONG")==0){
		ret= 8;
	}else if(strcmp(type_name, "NCHAR VARYING")==0 || strcmp(type_name, "VARCHAR")==0){
		ret= 9;
	}else if(strcmp(type_name, "DATE")==0){
		ret= 12;
	}else if(strcmp(type_name, "RAW")==0){
		ret= 23;
	}else if(strcmp(type_name, "LONG RAW")==0){
		ret= 24;
	}else if(strcmp(type_name, "ROWID")==0){
		ret= 69;
	}else if(strcmp(type_name, "NCHAR")==0 || strcmp(type_name, "CHAR")==0){
		ret= 96;
	}else if(strcmp(type_name, "BINARY_FLOAT")==0){
		ret= 100;
	}else if(strcmp(type_name, "BINARY_DOUBLE")==0){
		ret= 101;
	}else if(strcmp(type_name, "MLSLABEL")==0){
		ret= 105;
	}else if(strcmp(type_name, "MLSLABEL")==0){
		ret= 106;
	}else if(strcmp(type_name, "NCLOB")==0 || strcmp(type_name, "CLOB")==0){
		ret= 112;
	}else if(strcmp(type_name, "BLOB")==0){
		ret= 113;
	}else if(strcmp(type_name, "BFILE")==0){
		ret= 114;
	}else if(strcmp(type_name, "CFILE")==0){
		ret= 115;
	}else if(strstr(type_name, "TIMESTAMP(")){
		if(strstr(type_name, "WITH TIME ZONE")){
			ret= 181;
		}else if(strstr(type_name, "WITH LOCAL TIME ZONE")){
			ret= 231;
		}else{
			ret= 180;
		}
	}else if(strstr(type_name, "TIME(")){
		if(strstr(type_name, "WITH TIME ZONE")){
			ret= 179;
		}else{
			ret= 178;
		}
	}else if(strstr(type_name, "INTERVAL YEAR(")){
		ret= 182;
	}else if(strstr(type_name, "INTERVAL DAY(")){
		ret= 183;
	}else if(strcmp(type_name, "UROWID")==0){
		ret= 208;
	}
	//58, 111, 121, 122, 123
	return ret;
}

int create_table_byselect(ub4 table_id, char *tbname, char *owner, char *ddl_sql,
		DATABASE *db, int type, ub1 is_part){
	char sqlbuf[1024]={0};

	int ret;
	int col_id= 0;
	int col_size= 0;
	int char_length= 0;
	char col_name[128] = { 0 };
	char col_type[128] = { 0 };
	char data_default[256]={0};
	char cons_name[50];
	char status[10];
	char cons_type[2];

	int col_precision= 0;
	int col_scale= 0;
	char col_null[3]={0};
	int pos=0,cpos= 0;
	OCIDefine *defcolp[10];
	sword indp[10];
	FILE *fp;
	list<string> foreign_key;
	int is_pk= 0;

	sprintf(sqlbuf,
			"select a.COLUMN_NAME,a.CONSTRAINT_NAME,b.status,b.constraint_type from all_cons_columns a, all_constraints b where a.CONSTRAINT_NAME=b.CONSTRAINT_NAME and a.OWNER=b.OWNER and\
			b.TABLE_NAME='%s' and b.owner='%s' and b.constraint_type IN('U','P')",
			tbname, login_info1.schema);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 create_table_byselect:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) col_name,
			(sb4) sizeof(col_name), SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 2,
			(dvoid *) cons_name,
			(sb4) sizeof(cons_name), SQLT_STR, (dvoid *) &indp[1], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[2], db->errhp,
			(ub4) 3,
			(dvoid *) status,
			(sb4) sizeof(status), SQLT_STR, (dvoid *) &indp[2], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	ret = OCIDefineByPos(db->stmt, &defcolp[3], db->errhp,
			(ub4) 4,
			(dvoid *) cons_type,
			(sb4) sizeof(cons_type), SQLT_STR, (dvoid *) &indp[3], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);

	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos unique:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute unique:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
	unique_key.clear();
	primary_key.clear();
	all_constraints.clear();
	UNIQUEKEY *node;

	for (;;) {
		pos= 0;
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			all_constraints.push_back(cons_name);
			if(cons_type[0]=='P'){
				memset(primary_cons, 0, sizeof(primary_cons));
				strcpy(primary_cons, cons_name);
				primary_key.push_back(col_name);
			}else{
				if(unique_key.size()==0){
					node= new UNIQUEKEY;
					memset(node->status, 0, sizeof(node->status));
					strcpy(node->status, status);
					memset(node->unique_cons, 0, sizeof(node->unique_cons));
					strcpy(node->unique_cons, cons_name);
					node->unique_col= new list<string>;
					node->unique_col->push_back(col_name);
					unique_key.push_back(node);
	//				msg("%s %s %s\n", cons_name, col_name, status);
				}else{
					node= hasSameUniqueName(&unique_key, cons_name);
					if(node){
						node->unique_col->push_back(col_name);
					}else{
						node= new UNIQUEKEY;
						memset(node->status, 0, sizeof(node->status));
						strcpy(node->status, status);
						memset(node->unique_cons, 0, sizeof(node->unique_cons));
						strcpy(node->unique_cons, cons_name);
						node->unique_col= new list<string>;
						node->unique_col->push_back(col_name);
						unique_key.push_back(node);
	//					msg("%s %s %s\n", cons_name, col_name, status);
					}
				}
			}
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	/*select a.col#,a.name,b.DATA_TYPE,a.segcollength from sys.col$ a,dba_tab_cols b
	where a.obj#=89086 and b.table_name='TABLE1' and b.OWNER='HB' and a.name=b.COLUMN_NAME order by a.col#*/
	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf,
			"select a.col#,a.name,b.DATA_TYPE,b.data_length,b.data_precision,b.data_scale,b.nullable,b.data_default,b.char_length \
			 from sys.col$ a,all_tab_cols b \
			where a.obj#=%d and b.table_name='%s' and b.OWNER='%s' and a.name=b.COLUMN_NAME order by a.col#",
			table_id, tbname, owner);
	msg("sqlbuf=%s\n", sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 33:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 1, (dvoid *) &col_id, (sb4) sizeof(int),
			SQLT_INT, (dvoid *) &indp[0], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[2], db->errhp,
			(ub4) 2, (dvoid *) col_name, (sb4) sizeof(col_name), SQLT_STR,
			(dvoid *) &indp[1], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[3], db->errhp,
			(ub4) 3, (dvoid *) col_type, (sb4) sizeof(col_type), SQLT_STR,
			(dvoid *) &indp[2], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[4], db->errhp,
			(ub4) 4, (dvoid *) &col_size, (sb4) sizeof(int),
			SQLT_INT, (dvoid *) &indp[3], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[5], db->errhp,
			(ub4) 5, (dvoid *) &col_precision, (sb4) sizeof(int),
			SQLT_INT, (dvoid *) &indp[4], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[6], db->errhp,
			(ub4) 6, (dvoid *) &col_scale, (sb4) sizeof(int),
			SQLT_INT, (dvoid *) &indp[5], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[7], db->errhp,
			(ub4) 7, (dvoid *) col_null, (sb4) 2,
			SQLT_STR, (dvoid *) &indp[6], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[8], db->errhp,
			(ub4) 8, (dvoid *) data_default, (sb4) sizeof(data_default),
			SQLT_STR, (dvoid *) &indp[7], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[9], db->errhp,
			(ub4) 9, (dvoid *) &char_length, (sb4) sizeof(int),
			SQLT_INT, (dvoid *) &indp[8], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);

	ret = executeQuery8(db, db->stmt);
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute 333:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
	pos= 0;
	cpos= 0;

	if(type==CREATE_TABLE_DDL){
		pos= pos + sprintf(ddl_sql+pos, "CREATE TABLE %s.\"%s\" (\n", login_info2.schema, tbname);
	}else if(type==ALTER_TABLE_ADD_DDL){
		pos= pos + sprintf(ddl_sql+pos, "ALTER TABLE %s.\"%s\" ADD(\n", login_info2.schema, tbname);
	}

	if(do_create_loadconf){
		fp= fopen(loadconfig, "a+");
		fprintf(fp, "table %s.%s %s.%s\n", login_info1.schema, tbname, login_info2.schema, tbname);
	}

	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			if(strstr(col_name, "SYS_NC")){
				continue;
			}
			msg("name=%s, type=%s, size=%d, precision=%d, scale=%d, null=%c\n", col_name, col_type, col_size, col_precision, col_scale, col_null[0]);
			if(type==CREATE_TABLE_DDL){
				pos= combine_sql(pos, &cpos, ddl_sql, col_type, col_precision, col_scale,
						col_size, col_name, col_null, data_default, char_length);
			}else if(type==ALTER_TABLE_ADD_DDL){
				if(isListExists(&addColumnList, col_name)){
					pos= combine_sql(pos, &cpos, ddl_sql, col_type, col_precision, col_scale,
							col_size, col_name, col_null, data_default, char_length);
				}
			}
			is_pk=isListExists(&primary_key, col_name);
			if(do_create_loadconf){
				fprintf(fp, "column %s %s %s %s %d %d %d %d %d\n",
						col_name, col_name, col_type, col_type,
						col_size, is_pk, 0,
						0, col_id);
			}
		}
		col_size= 0;
		col_precision= 0;
		col_scale= 0;
		memset(col_type, 0, sizeof(col_type));
		memset(col_null, 0, sizeof(col_null));
		memset(data_default, 0, sizeof(data_default));
	}
	if(do_create_loadconf){
		fclose(fp);
	}
	if(type==CREATE_TABLE_DDL && primary_key.size()>0){
//		if(strlen(tbname)<27){
			pos= pos + sprintf(ddl_sql+pos, ",\n CONSTRAINT \"%s\" PRIMARY KEY(", primary_cons);
//		}
//		else{
//			char tmptb[30]={0};
//			strncpy(tmptb, tbname, 26);
//			pos= pos + sprintf(ddl_sql+pos, ",\n CONSTRAINT \"%s_PK\" PRIMARY KEY(", tmptb);
//		}
		list<string>::iterator it;
		cpos= 0;
		for(it=primary_key.begin(); it!=primary_key.end(); it++){
			if(cpos>0){
				pos= pos + sprintf(ddl_sql+pos, ",");
			}
			pos= pos + sprintf(ddl_sql+pos, "\"%s\"", (*it).c_str());
			cpos++;
		}
		pos= pos + sprintf(ddl_sql+pos, ") ENABLE");
	}
	if(type==CREATE_TABLE_DDL && unique_key.size()>0){
		list<UNIQUEKEY*>::iterator it;
		list<string>::iterator its;
		for(it=unique_key.begin(); it!=unique_key.end(); it++){
			node= (*it);
			if(strlen(node->unique_cons)>0 && node->unique_col->size()>0){
				pos= pos + sprintf(ddl_sql+pos, ",\n CONSTRAINT \"%s\" unique (", node->unique_cons);
				cpos= 0;
				for(its= node->unique_col->begin(); its!=node->unique_col->end(); its++){
					if(cpos>0){
						pos= pos + sprintf(ddl_sql+pos, ", ");
					}
					pos= pos + sprintf(ddl_sql+pos, "%s", (*its).c_str());
					cpos++;
				}
				delete node->unique_col;
				pos= pos + sprintf(ddl_sql+pos, ")");
				if(strstr(node->status, "ENABLED")){
					pos= pos + sprintf(ddl_sql+pos, " ENABLE");
				}else{
					pos= pos + sprintf(ddl_sql+pos, " DISABLE");
				}
			}
			delete node;
		}
	}

	pos= pos + sprintf(ddl_sql+pos, ")");
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
	if(type==CREATE_TABLE_DDL && is_part==1){
		msg("is partition table\n");

		char partsql[4096]={0};
		getPartitionInfo(tbname, owner, db, partsql);
		pos= pos + sprintf(ddl_sql+pos, "\n%s", partsql);
	}
	return ret;
}

void free_zero_col_list() {
	if (zero_col_list) {
		ZERO_COLUMN *tmp, *tmp_nxt;
		for (tmp = zero_col_list; tmp; tmp = tmp_nxt) {
			tmp_nxt = tmp->nxt;
			free(tmp);
		}
	}
	zero_col_list= NULL;
}

void add_zero_element(int col_id, char *col_name) {
	ZERO_COLUMN *node, *tmp;
	node = (ZERO_COLUMN *) malloc(sizeof(ZERO_COLUMN));
	memset(node->col_name, 0, sizeof(node->col_name));
	sprintf(node->col_name, "%s", col_name);
	node->col_id = col_id;
	node->nxt = NULL;
	if (!zero_col_list) {
		zero_col_list = node;
	} else {
		for (tmp = zero_col_list; tmp->nxt; tmp = tmp->nxt)
			;
		tmp->nxt = node;
	}
}

int find_zero_col_name(int col_id, char *ret_name) {
	ZERO_COLUMN *tmp;
	int ret = 0;
	for (tmp = zero_col_list; tmp; tmp = tmp->nxt) {
		if (col_id == tmp->col_id) {
			ret = 1;
			sprintf(ret_name, "%s", tmp->col_name);
			break;
		}
	}
	return ret;
}

void create_redoconfig(int object_id, char *tbname, DATABASE *db){
	char sqlcmd[1024];
	sword retcode;
	OCIDefine *defcolp[10];
	sword indp[10];
	int col_id;
	int seg_col_id;
	char col_name[100];
	int col_type;
	int col_charsetid;
	int col_len;
	char col_zero_name[100];
	FILE *fp;

	memset(sqlcmd, 0, sizeof(sqlcmd));
	sprintf(sqlcmd,
			"select col#, segcol#, name, type#, charsetid,length from sys.col$ \
			where obj# = %d  and col#!=0 order by segcol#",
			object_id);
//		        printf("%s\n", sqlcmd);

	retcode = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlcmd, strlen((char *) sqlcmd), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (retcode != OCI_SUCCESS) {
		printf("error OCIStmtPrepare2 \n");
		oraError8(db);
		printf("%s\n", db->errmsg);
		return;
	}

//	retcode = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			(ub4) 0, (CONST OCISnapshot *) NULL, (OCISnapshot *) NULL,
//			OCI_DEFAULT);
	retcode = executeQuery8(db, db->stmt);
	if (retcode != OCI_SUCCESS) {
//		oraError8(db);
		printf("OCIStmtExecute:%s\n", db->errmsg);
		goto CHECKS_EXIT;
	}

	retcode= OCIDefineByPos(db->stmt, &defcolp[0], db->errhp, (ub4) 1,
					(dvoid *) &col_id, (sb4) sizeof(int), SQLT_INT,
					(dvoid *) &indp[0], (ub2 *) 0, (ub2 *) 0,
					(ub4) OCI_DEFAULT);
	if (retcode != OCI_SUCCESS) {
		printf("error OCIDefineByPos\n");
		oraError8(db);
		printf("%s\n", db->errmsg);
		goto CHECKS_EXIT;
	}

	retcode= OCIDefineByPos(db->stmt, &defcolp[1], db->errhp, (ub4) 2,
					(dvoid *) &seg_col_id, (sb4) sizeof(int), SQLT_INT,
					(dvoid *) &indp[1], (ub2 *) 0, (ub2 *) 0,
					(ub4) OCI_DEFAULT);
	if (retcode != OCI_SUCCESS) {
		printf("error OCIDefineByPos\n");
		oraError8(db);
		printf("%s\n", db->errmsg);
		goto CHECKS_EXIT;
	}

	retcode = OCIDefineByPos(db->stmt, &defcolp[2], db->errhp, (ub4) 3,
			(dvoid *) col_name, (sb4) sizeof(col_name), SQLT_STR, (dvoid *) &indp[2],
			(ub2 *) 0, (ub2 *) 0, (ub4) OCI_DEFAULT);

	if (retcode != OCI_SUCCESS) {
		printf("error OCIDefineByPos\n");
		oraError8(db);
		printf("%s\n", db->errmsg);
		goto CHECKS_EXIT;
	}

	retcode= OCIDefineByPos(db->stmt, &defcolp[3], db->errhp, (ub4) 4,
					(dvoid *) &col_type, (sb4) sizeof(int), SQLT_INT,
					(dvoid *) &indp[3], (ub2 *) 0, (ub2 *) 0,
					(ub4) OCI_DEFAULT);
	if (retcode != OCI_SUCCESS) {
		printf("error OCIDefineByPos\n");
		oraError8(db);
		printf("%s\n", db->errmsg);
		goto CHECKS_EXIT;
	}

	retcode= OCIDefineByPos(db->stmt, &defcolp[4], db->errhp, (ub4) 5,
					(dvoid *) &col_charsetid, (sb4) sizeof(int),
					SQLT_INT, (dvoid *) &indp[4], (ub2 *) 0, (ub2 *) 0,
					(ub4) OCI_DEFAULT);
	if (retcode != OCI_SUCCESS) {
		printf("error OCIDefineByPos\n");
		oraError8(db);
		printf("%s\n", db->errmsg);
		goto CHECKS_EXIT;
	}

	retcode= OCIDefineByPos(db->stmt, &defcolp[5], db->errhp, (ub4) 6,
					(dvoid *) &col_len, (sb4) sizeof(int), SQLT_INT,
					(dvoid *) &indp[5], (ub2 *) 0, (ub2 *) 0,
					(ub4) OCI_DEFAULT);
	if (retcode != OCI_SUCCESS) {
		printf("error OCIDefineByPos\n");
		oraError8(db);
		printf("%s\n", db->errmsg);
		goto CHECKS_EXIT;
	}

	fp= fopen(redoconfig, "a+");
	fprintf(fp, "table %s %d %s.%s\n", login_info1.schema, object_id, login_info1.schema, tbname);

	for (;;) {
		retcode = OCIStmtFetch(db->stmt, db->errhp, 1, OCI_FETCH_NEXT,
				OCI_DEFAULT);
		if (retcode == OCI_NO_DATA) {
			break;
		} else {
			char tmp[256];
			memset(tmp, 0, sizeof(tmp));
			if (seg_col_id != 0) {
				if (zero_col_num == 0) {
					sprintf(tmp, "column %d %d %d %d %s\n", seg_col_id,
							col_type, col_charsetid, col_len, col_name);
				} else {
					memset(col_zero_name, 0, sizeof(col_zero_name));
					int is_xml = find_zero_col_name(col_id,
							col_zero_name);
					if (is_xml) {
						sprintf(tmp, "column %d %d %d %d %s\n",
								seg_col_id, col_type, col_charsetid,
								col_len, col_zero_name);
					} else {
						sprintf(tmp, "column %d %d %d %d %s\n",
								seg_col_id, col_type, col_charsetid,
								col_len, col_name);
					}
				}
				fprintf(fp, tmp);
			} else {
				zero_col_num++;
				add_zero_element(col_id, col_name);
			}

		}
	}
	fclose(fp);

	CHECKS_EXIT:

	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

void drop_table(char *tbname, DATABASE *db) {
	char sqlcmd[1024] = { 0 };
	sprintf(sqlcmd, "drop table %s.\"%s\" CASCADE CONSTRAINTS PURGE", login_info2.schema, tbname);
	msg("%s\n", sqlcmd);
	prepareExecute8(db, &db->stmt, (text *)sqlcmd);
}

/************************************************************************************************
 * disbale or enable target database foreign key
 *
 * @parameter dst_table_def
 * @parameter do_level						300-disable, 301-enable
 * @parameter ddl_path
 * @parameter dbdst
 *
 * @return 	null
 *
 * **********************************************************************************************
 */

void disable_dst_fk(map<string, TABLE_DEFINE*> *dst_table_def, int do_level,
		char *ddl_path, DATABASE *dbdst){
	map<string, TABLE_DEFINE*>::iterator it;
	TABLE_DEFINE *dst_td;
	char src_tbname[256]={0};
	char *sqlbuf= NULL;
	int len = 0;
	OCIDefine *defcolp[5];
	sword indp[5];
	int rtncode;
	char tab_owner[50] = {0};
	char tab_name[256] = {0};
	char cons_name[128] = {0};
	list<string> strfklist;
	list<string>::iterator fkit;

	if(dst_table_def->size()==0){
		return;
	}
	msg("table num=%d\n", dst_table_def->size());
	sqlbuf= (char *)malloc(dst_table_def->size()*100+100);

	len += sprintf(sqlbuf,"select owner,table_name,constraint_name from all_constraints where (constraint_type='R') and (");
	for (it = dst_table_def->begin(); it != dst_table_def->end(); it++) {
		dst_td= it->second;
		sprintf(src_tbname, "%s", (it->first).c_str());
		len += sprintf(sqlbuf+len," (owner='%s' and table_name='%s') or",login_info2.schema,src_tbname);
	}
	sqlbuf[len-1] = '\0';//去掉'r'
	sprintf(sqlbuf+len-2,")");//'o'置为)

//	printf("%s\n", sqlbuf);
	rtncode = OCIStmtPrepare2(dbdst->svchp, &dbdst->stmt, dbdst->errhp, (text *) sqlbuf,
			strlen(sqlbuf), NULL, 0, OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (rtncode != OCI_SUCCESS) {
		oraError8(dbdst);
		free(sqlbuf);
		return;
	}

	rtncode = OCIDefineByPos(dbdst->stmt, &defcolp[0], dbdst->errhp, (ub4) 1,
			(dvoid *) &tab_owner, (sb4) sizeof(tab_owner), SQLT_STR,
			(dvoid *) &indp[0], (ub2 *) 0, (ub2 *) 0, (ub4) OCI_DEFAULT);

	rtncode = OCIDefineByPos(dbdst->stmt, &defcolp[1], dbdst->errhp, (ub4) 2,
			(dvoid *) &tab_name, (sb4) sizeof(tab_name), SQLT_STR,
			(dvoid *) &indp[1], (ub2 *) 0, (ub2 *) 0, (ub4) OCI_DEFAULT);

	rtncode = OCIDefineByPos(dbdst->stmt, &defcolp[2], dbdst->errhp, (ub4) 3,
			(dvoid *) &cons_name, (sb4) sizeof(cons_name), SQLT_STR,
			(dvoid *) &indp[2], (ub2 *) 0, (ub2 *) 0, (ub4) OCI_DEFAULT);

//	rtncode = OCIStmtExecute(dbdst->svchp, dbdst->stmt, dbdst->errhp, (ub4) 0, 0, 0, 0,
//			OCI_DEFAULT);
	rtncode = executeQuery8(dbdst, dbdst->stmt);
	if (rtncode != OCI_SUCCESS) {
//		oraError8(dbdst);
		free(sqlbuf);
		return;
	}
	for (;;) {
		memset(tab_owner,0,sizeof(tab_owner));
		memset(tab_name,0,sizeof(tab_name));
		memset(cons_name,0,sizeof(cons_name));
		rtncode = OCIStmtFetch(dbdst->stmt, dbdst->errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT);
		if (rtncode == OCI_NO_DATA) {
			break;
		} else {
			//ALTER TABLE %s.%s DISABLE CONSTRAINT %s CASCADE
			char dis_consint[256]={0};
			if(do_level==300){
				sprintf(dis_consint,"ALTER TABLE %s.%s DISABLE CONSTRAINT %s CASCADE",tab_owner,tab_name,cons_name);
			}else{
				sprintf(dis_consint,"ALTER TABLE %s.%s ENABLE CONSTRAINT %s",tab_owner,tab_name,cons_name);
			}
			strfklist.push_back(dis_consint);
		}
	}
	free(sqlbuf);
	OCIStmtRelease(dbdst->stmt, dbdst->errhp, NULL, 0, OCI_DEFAULT);

	for(fkit= strfklist.begin(); fkit!=strfklist.end(); fkit++){
		msg("%s\n", (*fkit).c_str());
		prepareExecute8(dbdst, &(dbdst->stmt), (text *)(*fkit).c_str());
	}
}

int function_compare(DEFTABLE *src_td, DEFTABLE *dst_td){
	int ret= 0;

	int col= 0;
	DEFCOLUMN *cd_src;
	DEFCOLUMN *cd_dst;

	msg("compare1 %s\n", src_td->table_name);
	msg("compare1 %d %d\n", src_td->col_count, dst_td->col_count);
	if(src_td->col_count!=dst_td->col_count){
		msg("%d\n", dst_td->col_count);
		return 1;
	}

	for(col=0; col<src_td->col_count; col++){
		cd_src= &src_td->defcolumn[col];
		cd_dst= &dst_td->defcolumn[col];
		msg("compare2 %s, %s\n", cd_src->col_name, cd_dst->col_name);
		if(strcmp(cd_src->col_name, cd_dst->col_name)!=0){
			ret= 1;
			break;
		}
		msg("compare2 %s, %s\n", cd_src->col_type, cd_dst->col_type);
		if(strcmp(cd_src->col_type, cd_dst->col_type)!=0){
			ret= 1;
			break;
		}
		msg("compare2 %d, %d\n", cd_src->col_ispk, cd_dst->col_ispk);
		if(cd_src->col_ispk!=cd_dst->col_ispk){
			ret= 1;
			break;
		}
		msg("compare2 %d, %d\n", cd_src->col_isuk, cd_dst->col_isuk);
		if(cd_src->col_isuk!=cd_dst->col_isuk){
			ret= 1;
			break;
		}
		msg("compare2 %d, %d\n", cd_src->col_isck, cd_dst->col_isck);
//		if(cd_src->col_isck!=cd_dst->col_isck){
//			ret= 1;
//			break;
//		}
//		if(strcmp(cd_src->status, cd_dst->status)!=0){
//			ret= 1;
//			break;
//		}
//		if(cd_src->NULLABLE[0]!=cd_dst->NULLABLE[0]){
//			ret= 1;
//			break;
//		}
//		if(cd_src->data_default!=NULL && cd_dst->data_default!=NULL){
//			if(strcmp(cd_src->data_default, cd_dst->data_default)!=0){
//				ret= 1;
//				break;
//			}
//		}
//		if((cd_src->search_cond!=NULL && cd_dst->search_cond==NULL) ||
//				(cd_src->search_cond==NULL && cd_dst->search_cond!=NULL)){
//			ret= 1;
//			break;
//		}
		msg("compare2 %d, %d\n", cd_src->precision, cd_dst->precision);
		if(cd_src->precision != cd_dst->precision){
			ret= 1;
			break;
		}
		msg("compare2 %d, %d\n", cd_src->scale, cd_dst->scale);
		if(cd_src->scale != cd_dst->scale){
			ret= 1;
			break;
		}
		msg("compare2 %d, %d\n", cd_src->col_length, cd_dst->col_length);
		if(cd_src->col_length != cd_dst->col_length){
			ret= 1;
			break;
		}
		msg("compare2 %d, %d\n", cd_src->char_length, cd_dst->char_length);
		if(cd_src->char_length != cd_dst->char_length){
			ret= 1;
			break;
		}
	}

	return ret;
}

void simple_deftable(DEFTABLE *td, DEFTABLE **ptd_smp){
	DEFTABLE *td_smp;
	td_smp= (DEFTABLE *)malloc(sizeof(DEFTABLE));
	memset(td_smp->table_name, 0, sizeof(td_smp->table_name));
	strcpy(td_smp->table_name, td->table_name);
	td_smp->col_count= 0;

	int col= 0;
	DEFCOLUMN *cd, *cd_smp;
	char last_colname[256]={0};

	for(col= 0; col<td->col_count; col++){
		cd= &td->defcolumn[col];
//		msg("cd->colname=%s\n", cd->col_name);
		if(col==0 || strcmp(last_colname, cd->col_name)!=0){
			td_smp = (DEFTABLE *)realloc(td_smp, sizeof(*td_smp) + sizeof(DEFCOLUMN)
					* (td_smp->col_count + 1));
			if (!td_smp) {
				fprintf(stderr, "Realloc column for table error\n");
				exit(1);
			}
			cd_smp = &td_smp->defcolumn[td_smp->col_count];
			td_smp->col_count++;
			init_column(cd_smp);
			strcpy(cd_smp->col_name, cd->col_name);
			strcpy(cd_smp->col_type, cd->col_type);
			cd_smp->col_length= cd->col_length;
			cd_smp->char_length= cd->char_length;
			cd_smp->col_isck= cd->col_isck;
			cd_smp->col_isuk= cd->col_isuk;
			cd_smp->col_ispk= cd->col_ispk;
			cd_smp->cons_pos= cd->cons_pos;
			cd_smp->col_id= cd->col_id;
			cd_smp->precision= cd->precision;
			cd_smp->scale= cd->scale;
			if(cd->search_cond){
				int search_len= strlen(cd->search_cond);
				if(search_len!=0){
					cd_smp->search_cond= (char *)malloc(search_len+1);
					memset(cd_smp->search_cond, 0, search_len+1);
					strcpy(cd_smp->search_cond, cd->search_cond);
				}
			}
			if(strlen(cd->status)!=0){
				strcpy(cd_smp->status, cd->status);
			}
			strcpy(cd_smp->NULLABLE, cd->NULLABLE);
			if(cd->data_default){
				int default_len= strlen(cd->data_default);
				if(default_len!=0){
					cd_smp->data_default= (char *)malloc(default_len+1);
					memset(cd_smp->data_default, 0, default_len+1);
					strcpy(cd_smp->data_default, cd->data_default);
				}
			}

			strcpy(last_colname, cd->col_name);
		}else{
			if(cd->col_isck=='1'){
				cd_smp->col_isck= cd->col_isck;
			}
			if(cd->col_isuk=='1'){
				cd_smp->col_isuk= cd->col_isuk;
			}
			if(cd->col_ispk=='1'){
				cd_smp->col_ispk= cd->col_ispk;
			}
			if(cd->search_cond){
				int search_len= strlen(cd->search_cond);
				if(cd_smp->search_cond==NULL && search_len!=0){
					cd_smp->search_cond= (char *)malloc(search_len+1);
					memset(cd_smp->search_cond, 0, search_len+1);
					strcpy(cd_smp->search_cond, cd->search_cond);
				}
			}
			if(strlen(cd->status)!=0){
				strcpy(cd_smp->status, cd->status);
			}
			if(cd->data_default){
				int default_len= strlen(cd->data_default);
				if(cd_smp->data_default==NULL && default_len!=0){
					cd_smp->data_default= (char *)malloc(default_len+1);
					memset(cd_smp->data_default, 0, default_len+1);
					strcpy(cd_smp->data_default, cd->data_default);
				}
			}
		}
	}
//	msg("tbname=%s, col=%d\n", td_smp->table_name, td_smp->col_count);
	*ptd_smp= td_smp;
}

void free_deftable(DEFTABLE *td){
	if(!td)
		return;
	int col;
	DEFCOLUMN *cd;
	for(col=0; col<td->col_count; col++){
		cd= &td->defcolumn[col];
		if(cd->search_cond)
			free(cd->search_cond);
		if(cd->data_default)
			free(cd->data_default);
	}
	free(td);
}

int two_table_compare(list<string> *dif_table_listt){

	list<DEFTABLE *>::iterator it;
	list<DEFTABLE *>::iterator it_dst;

	DEFTABLE *td, *td_smp;
	DEFTABLE *td_dst, *td_dst_smp;
	int find= 0;
	int ret= 0;

	for(it=tablelist.begin(); it!=tablelist.end(); it++){
		td= (*it);
		find= 0;
		for(it_dst= tablelist_dst.begin(); it_dst!=tablelist_dst.end(); it_dst++){
			td_dst= (*it_dst);
			if(strcmp(td->table_name, td_dst->table_name)==0){
				find= 1;
				break;
			}
		}
		if(find){

			simple_deftable(td, &td_smp);
			simple_deftable(td_dst, &td_dst_smp);
			ret= function_compare(td_smp, td_dst_smp);
			free_deftable(td_smp);
			free_deftable(td_dst_smp);
			if(ret){
				msg("%s is different\n", td->table_name);
				dif_table_listt->push_back(td->table_name);
			}
		}else{
			msg("%s is different\n", td->table_name);
			dif_table_listt->push_back(td->table_name);
		}
	}


	return ret;
}

void compare_table(map<string, TABLE_DEFINE*> *src_table_def,
		map<string, TABLE_DEFINE*> *dst_table_def, int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst){
	map<string, TABLE_DEFINE*>::iterator it;
	TABLE_DEFINE *src_td;
	TABLE_DEFINE *dst_td;
	char src_tbname[256]={0};
//	char dst_tbname[256]={0};

	list<string> suc_table_list;
	list<string>::iterator itstr;
	dif_table_list.clear();
	suc_table_list.clear();
	char ddl_table_file[256]={0};
	char ddl_file[256]={0};
	list<COLUMN_DEFINE*>::iterator it_src_def;
	list<COLUMN_DEFINE*>::iterator it_dst_def;
	COLUMN_DEFINE *src_col, *dst_col;
	bool find= false;
	char ddl_sql[20480];

	sprintf(ddl_table_file, "%s/dif_table", ddl_path);
	sprintf(ddl_file, "%s/ddl_table.sql", ddl_path);
	sprintf(loadconfig, "%s/load.config", ddl_path);
	sprintf(redoconfig, "%s/dictionary.config", ddl_path);

	if(access(ddl_table_file, F_OK)==0){
		remove(ddl_table_file);
	}
	if(access(ddl_file, F_OK)==0){
		remove(ddl_file);
	}
	if(do_create_loadconf && access(loadconfig, F_OK)==0){
		remove(loadconfig);
	}
	if(do_create_dictionary && access(redoconfig, F_OK)==0){
		remove(redoconfig);
	}

	if(do_level==9999){
		//drop table
		for (it = dst_table_def->begin(); it != dst_table_def->end(); it++) {
			dst_td= it->second;
			sprintf(src_tbname, "%s", (it->first).c_str());
			drop_table(src_tbname, dbdst);
		}
		return;
	}

	msg("------------------compare table----------------------\n");
	if (dst_table_def->size() > 0) {
		select_all_table(login_info1.schema, db, 1);
		select_all_table(login_info2.schema, dbdst, 0);

		//比对
		msg("two_table_compare\n");
		two_table_compare(&dif_table_list);
		if(dif_table_list.size()>0){
		//删除目标端不同的表
			for(itstr= dif_table_list.begin(); itstr!=dif_table_list.end(); itstr++){
				sprintf(src_tbname, "%s", (*itstr).c_str());
				if(do_level==1)
					drop_table(src_tbname, dbdst);
			}
			msg("create_table_from_tablelist\n");
			create_table_from_tablelist(db, dbdst, do_level, ddl_table_file, ddl_file);
			if(do_foreign_key)
				add_foreign_key2(db, dbdst, ddl_file, do_level);
			grant_table_privs2(db, dbdst, ddl_file, do_level);
			select_table_comments(db, dbdst, do_level, ddl_file);
			select_col_comments(db, dbdst, do_level, ddl_file);
		}else{
			msg("src and dst tables are same\n");
		}
	}else{
		select_all_table(login_info1.schema, db, 1);
		msg("create_table_from_tablelist\n");
		create_table_from_tablelist(db, dbdst, do_level, ddl_table_file, ddl_file);
		if(do_foreign_key)
			add_foreign_key2(db, dbdst, ddl_file, do_level);
		grant_table_privs2(db, dbdst, ddl_file, do_level);
		select_table_comments(db, dbdst, do_level, ddl_file);
		select_col_comments(db, dbdst, do_level, ddl_file);
	}
}

void getTableCount(DATABASE *db, char *tbname, char *schema_name, ub4 *retcount){
	char sqlbuf[1024]={0};
	int ret;
	ub4 count=0;

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,
			"SELECT count(*) from %s.\"%s\"", schema_name, tbname);
//	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2:%s\n", db->errmsg);
		ret = -2;
		return;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) &count,
			(sb4) sizeof(ub4), SQLT_INT, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
//	msg("%s count=%d\n", tbname, count);
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
//			msg("count=%u\n", count);
			*retcount= count;
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

int getMaxLobSize(DATABASE *db, char *tbname, char *colname, int *size){
	char sqlbuf[1024]={0};
	int ret;
	ub4 count=0;
	*size= count;

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,
			"SELECT max(dbms_lob.getlength(%s)) from %s.\"%s\"", colname, login_info1.schema, tbname);
//	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) &count,
			(sb4) sizeof(ub4), SQLT_INT, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
//	msg("%s count=%d\n", tbname, count);
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
//			msg("count=%u\n", count);
			*size= count;
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
	return 0;
}

void getParameter(DATABASE *db, char *param, char *retvalue){
	char sqlbuf[1024]={0};
	int ret;
	char value[128] = { 0 };
	int id= 0;

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,
			"select value from nls_database_parameters where parameter='%s'", param);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2:%s\n", db->errmsg);
		ret = -2;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) value,//OCIDefineByPos primary_key,30长度，若不够30则填充空格(十进制32)
			(sb4) 128, SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute:%s\n", db->errmsg);
		ret = -2;
	}
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			msg("value=%s\n", value);
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	memset(sqlbuf, 0, sizeof(sqlbuf));

	sprintf(sqlbuf,
			"select NLS_CHARSET_ID('%s') from dual", value);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2:%s\n", db->errmsg);
		ret = -2;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) &id,//OCIDefineByPos primary_key,30长度，若不够30则填充空格(十进制32)
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute:%s\n", db->errmsg);
		ret = -2;
	}
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			msg("id=%d\n", id);
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	sprintf(retvalue, "%s#%d", value, id);
}

void getMaxLongSize(char *tbname, int *longsize, DATABASE *db){
	FILE *fp= NULL;
	char buf[256];
	int maxlongsize= 0;
	int size= 0;
	fp= fopen(loadconfig, "r");
	if(!fp){
		msg("open %s failed\n", loadconfig);
		return;
	}
	char key[256]={0};
	sprintf(key, "table %s.%s ", login_info1.schema, tbname);
	int find= 0;
	char src_col_name[256];
	char dst_col_name[256];
	char src_col_type[32];
	char dst_col_type[32];
	int col_size= 0;
	int is_pk= 0;
	int is_given_pk= 0;
	int is_unique= 0;
	int col_id= 0;
	int len= 0;
	while(fgets(buf, sizeof(buf), fp)){
		if(strncmp(buf, "table ", 6)==0){
			if(find)
				break;
			if(strstr(buf, key)){
				find= 1;
			}else{
				find= 0;
			}
		}else{
			if(find){
				//column T3 T3 BLOB BLOB 4000 0 0 0 3
				if(sscanf(buf, "column %s %s %s %s %d %d %d %d %d\n%n", src_col_name, dst_col_name, src_col_type,
						dst_col_type, &col_size, &is_pk, &is_given_pk, &is_unique, &col_id, &len)
				&& len == strlen(buf)){
					if(strcmp(src_col_type, "CLOB")==0 || strcmp(src_col_type, "NCLOB")==0
							|| strcmp(src_col_type, "BLOB")==0){
						getMaxLobSize(db, tbname, src_col_name, &size);
						if(size>maxlongsize){
							maxlongsize= size;
						}
					}
				}
			}
		}
	}
	fclose(fp);
	*longsize= maxlongsize;
}

void create_rdbcopy_config(map<string, TABLE_DEFINE*> *src_table_def,
		map<string, TABLE_DEFINE*> *dst_table_def, int do_level, char *config_file,
		DATABASE *db, DATABASE *dbdst, char *ddl_path){
	map<string, TABLE_DEFINE*>::iterator it;
	TABLE_DEFINE *src_td;
	TABLE_DEFINE *dst_td;
	char src_tbname[256]={0};
//	char dst_tbname[256]={0};
	list<string> table_list;
	table_list.clear();
	ub4 count1=0;
	ub4 count2=0;
	list<COLUMN_DEFINE*>::iterator it_src_def;
	list<COLUMN_DEFINE*>::iterator it_dst_def;
	COLUMN_DEFINE *src_col, *dst_col;
	bool find= false;
	char rdbconfig[256]={0};
	char result[256]={0};
	char tmp[256]={0};

	sprintf(rdbconfig, "%s/rdbcopy.conf", ddl_path);
	sprintf(result, "%s/result", ddl_path);
	msg("rdbconfig=%s\n", rdbconfig);
	if(access(rdbconfig, F_OK)==0){
		remove(rdbconfig);
	}
	if(access(result, F_OK)==0){
		remove(result);
	}

	sprintf(loadconfig, "%s/load.config", ddl_path);

	FILE *fp3= fopen(result, "a+");
	for (it = src_table_def->begin(); it != src_table_def->end(); it++) {
		src_td = it->second;
		sprintf(src_tbname, "%s", (it->first).c_str());
		dst_td= NULL;
		dst_td= (*dst_table_def)[src_tbname];
		if(!dst_td){
			msg("table=%s distination not exist, please sync table\n", src_tbname);
		}else{
			if(src_td->col_count!=dst_td->col_count){
				msg("table=%s distination column count different with source, please sync table\n", src_tbname);
			}else{
				for(it_src_def=src_td->columns->begin(); it_src_def!=src_td->columns->end(); it_src_def++){
					src_col= (*it_src_def);
//					msg("col_name=%s\n", src_col->col_name);
					find= false;
					for(it_dst_def=dst_td->columns->begin(); it_dst_def!=dst_td->columns->end(); it_dst_def++){
						dst_col= (*it_dst_def);
						if(strcmp(src_col->col_name, dst_col->col_name)==0 && src_col->col_type==dst_col->col_type){
							find= true;
							break;
						}
					}
					if(!find){
						msg("table=%s distination column type or name different with source, please sync table\n", src_tbname);
						break;
					}
				}
				if(find){
					if(do_create_copyconf==0){
						getTableCount(db, src_tbname, login_info1.schema, &count1);
						getTableCount(dbdst, src_tbname, login_info2.schema, &count2);
						msg("table=%s, count1=%u, count2=%u\n", src_tbname, count1, count2);
						if(count1!=count2){
							table_list.push_back(src_tbname);
							fprintf(fp3, "%s----%d:%d\n", src_tbname, count1, count2);
						}
					}else{
						table_list.push_back(src_tbname);
					}
				}
			}
		}
	}
	fclose(fp3);

	if(table_list.size()==0){
		msg("all tables count are same...\n");
	}else{
		char buf[256];
		FILE *fp1= fopen(config_file, "r");

		if(!fp1){
			msg("Open file %s error\n", config_file);
			return;
		}

		FILE *fp2= fopen(rdbconfig, "a+");

		while (fgets(buf, sizeof(buf), fp1)) {
			if(strstr(buf, "compare_object=")){
				break;
			}else{
				if(!strstr(buf, "src_schema=") && !strstr(buf, "dst_schema="))
				fputs(buf, fp2);
			}
		}
		fclose(fp1);
		fprintf(fp2, "copy_mode=directcopy\n");
		list<string>::iterator it;
		int longsize= 0;
		for(it=table_list.begin(); it!=table_list.end(); it++){
			memset(src_tbname, 0, sizeof(src_tbname));
			strcpy(src_tbname, (*it).c_str());
			if(do_level==201){
				getMaxLongSize(src_tbname, &longsize, db);
				if(longsize>1024*1024){
					longsize= longsize/(1024*1024);
				}else{
					longsize= 0;
				}
			}
			fprintf(fp2, "table,,%s.%s,,%s.%s,,%d,,0\n", login_info1.schema, (*it).c_str(),
					login_info2.schema, (*it).c_str(), longsize);
		}
		fprintf(fp2, "truncate=1\n");
		fprintf(fp2, "skip_index=0\n");
		fprintf(fp2, "thread=3\n");
		fprintf(fp2, "long=1M\n");
		memset(buf, 0, sizeof(buf));
		getParameter(db, "NLS_CHARACTERSET", buf);
		fprintf(fp2, "charsetid=%s\n", buf);
		memset(buf, 0, sizeof(buf));
		getParameter(db, "NLS_NCHAR_CHARACTERSET", buf);
		fprintf(fp2, "ncharsetid=%s\n", buf);
		fprintf(fp2, "degree=1\n");
		fprintf(fp2, "direct=ON\n");
		fprintf(fp2, "finishpath=%s/\n", tmp);
		fclose(fp2);

	}
}

void init_column(DEFCOLUMN *defcolumn){
	if(defcolumn){
		defcolumn->char_length= 0;
		defcolumn->col_id= 0;
		defcolumn->col_length= 0;
		defcolumn->precision= 0;
		defcolumn->scale= 0;
		memset(defcolumn->col_name, 0, sizeof(defcolumn->col_name));
		memset(defcolumn->col_type, 0, sizeof(defcolumn->col_type));
		defcolumn->col_ispk= 0;
		defcolumn->col_isuk= 0;
		defcolumn->col_isck= 0;
		memset(defcolumn->status, 0, sizeof(defcolumn->status));
		memset(defcolumn->NULLABLE, 0, sizeof(defcolumn->NULLABLE));
		memset(defcolumn->index_name, 0, sizeof(defcolumn->index_name));
		memset(defcolumn->uniqueness, 0, sizeof(defcolumn->uniqueness));
		memset(defcolumn->constraint_name, 0, sizeof(defcolumn->constraint_name));
		memset(defcolumn->index_type, 0, sizeof(defcolumn->index_type));
		defcolumn->search_cond= NULL;
		defcolumn->data_default= NULL;
		defcolumn->cons_pos= 0;
	}
}

int select_one_table(char *owner, DATABASE *db, char *table_name, DEFTABLE *td){

	char sqlbuf[1024]={0};
	int pos= 0;
	sword ret= 0;
	OCIDefine *defcolp[30];
	sword indp[30];

	char tab_name[128]={0};
	char col_name[128]={0};
	char col_type[30]={0};
	int col_length= 0;
	int char_length= 0;
	char is_pk[3]={0};
	char is_uk[3]={0};
	char is_ck[3]={0};
	char constraint_name[100]={0};
	char search_cond[1024]={0};
	char status[10]={0};
	int col_id=0;
	int precision= 0;
	int scale= 0;
	char nullable[2]={0};
	char data_default[1024]={0};
	char index_type[10]={0};
	char index_name[50]={0};
	char uniqueness[12]={0};
	DEFCOLUMN *cd=NULL;
	int end= 1;
	char save_table[256]={0};
	int i= 0;
//	char cons_pos[3]= {0};
	int cons_pos= 0;

	pos= pos + sprintf(sqlbuf+pos, "SELECT\n");

	pos= pos + sprintf(sqlbuf+pos, "S.TABLE_NAME tab_name,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.COLUMN_NAME col_name,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.DATA_TYPE col_type,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.DATA_LENGTH col_length,");
	pos= pos + sprintf(sqlbuf+pos, "S.CHAR_LENGTH char_length,");
	pos= pos + sprintf(sqlbuf+pos, "(DECODE(PP.CONSTRAINT_TYPE, 'P', '1', '0'))col_ispk,\n");
	pos= pos + sprintf(sqlbuf+pos, "(DECODE(PP.CONSTRAINT_TYPE, 'U', '1', '0'))col_isuk,\n");
	pos= pos + sprintf(sqlbuf+pos, "(DECODE(PP.CONSTRAINT_TYPE, 'C', '1', '0'))col_isck,\n");
	pos= pos + sprintf(sqlbuf+pos, "PP.CONSTRAINT_NAME constraint_name,\n");
	pos= pos + sprintf(sqlbuf+pos, "PP.POSITION position,\n");
	pos= pos + sprintf(sqlbuf+pos, "PP.SEARCH_CONDITION search_condition,\n");
	pos= pos + sprintf(sqlbuf+pos, "PP.STATUS status,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.COLUMN_ID col_id,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.DATA_PRECISION precision,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.DATA_SCALE scale,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.NULLABLE nullable,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.DATA_DEFAULT data_default,\n");
	pos= pos + sprintf(sqlbuf+pos, "TT.INDEX_TYPE index_type,\n");
	pos= pos + sprintf(sqlbuf+pos, "TT.INDEX_NAME index_name,\n");
	pos= pos + sprintf(sqlbuf+pos, "TT.UNIQUENESS uniqueness\n");

	pos= pos + sprintf(sqlbuf+pos, "FROM all_tab_columns S,\n");
	pos= pos + sprintf(sqlbuf+pos, "(SELECT CC.COLUMN_NAME, CC.POSITION, C.OWNER, C.TABLE_NAME, C.CONSTRAINT_TYPE, C.status, C.CONSTRAINT_NAME, C.SEARCH_CONDITION\n");
	pos= pos + sprintf(sqlbuf+pos, "FROM ALL_CONSTRAINTS C, ALL_CONS_COLUMNS CC\n");
	pos= pos + sprintf(sqlbuf+pos, "WHERE C.OWNER= CC.OWNER\n");
	pos= pos + sprintf(sqlbuf+pos, "AND C.CONSTRAINT_TYPE IN ('P','U','C')\n");
	pos= pos + sprintf(sqlbuf+pos, "AND C.CONSTRAINT_NAME=CC.CONSTRAINT_NAME\n");
	pos= pos + sprintf(sqlbuf+pos, "AND C.TABLE_NAME=CC.TABLE_NAME) PP,\n");
//	pos= pos + sprintf(sqlbuf+pos, "AND C.VALIDATED='VALIDATED') PP,\n");
	pos= pos + sprintf(sqlbuf+pos, "(SELECT t1.index_type,t1.UNIQUENESS,t1.INDEX_NAME,t1.TABLE_NAME,t2.COLUMN_NAME\n");
	pos= pos + sprintf(sqlbuf+pos, "from all_indexes t1, all_ind_columns t2\n");
	pos= pos + sprintf(sqlbuf+pos, "WHERE t1.INDEX_NAME=t2.INDEX_NAME\n");
	pos= pos + sprintf(sqlbuf+pos, "AND t1.TABLE_OWNER=t2.INDEX_OWNER\n");
	pos= pos + sprintf(sqlbuf+pos, "AND t1.TABLE_NAME=t2.TABLE_NAME) TT\n");
	pos= pos + sprintf(sqlbuf+pos, "WHERE S.TABLE_NAME=PP.TABLE_NAME(+)\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.COLUMN_NAME=PP.COLUMN_NAME(+)\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.OWNER=PP.OWNER(+)\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.TABLE_NAME=TT.TABLE_NAME(+)\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.COLUMN_NAME=TT.COLUMN_NAME(+)\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.TABLE_NAME='%s'\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.OWNER='%s'\n", table_name, owner);
	pos= pos + sprintf(sqlbuf+pos, "ORDER BY S.COLUMN_ID");

	msg("\n%s\n", sqlbuf);

	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 primary_key:%s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) tab_name,
			(sb4) sizeof(tab_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos:%s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) col_name,
			(sb4) sizeof(col_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) col_type,
			(sb4) sizeof(col_type), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &col_length,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &char_length,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) is_pk,
			(sb4) sizeof(is_pk), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) is_uk,
			(sb4) sizeof(is_uk), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) is_ck,
			(sb4) sizeof(is_ck), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) constraint_name,
			(sb4) sizeof(constraint_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &cons_pos,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) search_cond,
			(sb4) sizeof(search_cond), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) status,
			(sb4) sizeof(status), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &col_id,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &precision,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &scale,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) nullable,
			(sb4) sizeof(nullable), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) data_default,
			(sb4) sizeof(data_default), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) index_type,
			(sb4) sizeof(index_type), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) index_name,
			(sb4) sizeof(index_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) uniqueness,
			(sb4) sizeof(uniqueness), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}

//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute unique:%s\n", db->errmsg);
		ret = -2;
		goto SELECT_ONE_TABLE_EXIT;
	}

	for (;;) {
		pos= 0;
		cons_pos= 0;
		precision= 0;
		scale= 0;
		char_length= 0;
		col_id=0;
		col_length= 0;
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			msg("%s,%s,%s,%d,c_len=%d,pk=%c,uk=%c,ck=%c,constraint=%s,cons_pos=%d,search_cond=%s,status=%s,col_id=%d,precision=%d, scale=%d, nullable=%s, data_default=%s, index_type=%s, index_name=%s, uniqueness=%s\n",
					tab_name, col_name, col_type, col_length,char_length, is_pk[0], is_uk[0], is_ck[0], constraint_name,cons_pos, search_cond,
					status, col_id, precision, scale, nullable, data_default, index_type, index_name, uniqueness);

//				td->col_count= 0;
//				td->has_check= 0;
//				td->has_index= 0;
//				td->has_pk= 0;
//				td->has_uk= 0;
//				memset(td->table_name, 0, sizeof(td->table_name));
//				memset(td->primary_cons, 0, sizeof(td->primary_cons));
			strcpy(td->table_name, tab_name);
			td = (DEFTABLE *)realloc(td, sizeof(*td) + sizeof(DEFCOLUMN)
					* (td->col_count + 1));
			if (!td) {
				fprintf(stderr, "Realloc column for table error\n");
				exit(1);
			}
			cd = &td->defcolumn[td->col_count];
			td->col_count++;
			init_column(cd);
			strcpy(cd->col_name, col_name);
			strcpy(cd->col_type, col_type);
			cd->col_length= col_length;
			cd->char_length= char_length;
			cd->col_isck= is_ck[0];
			cd->col_isuk= is_uk[0];
			cd->col_ispk= is_pk[0];
			if(cd->col_ispk=='1'){
				td->has_pk= 1;
				strcpy(td->primary_cons, constraint_name);
			}
			if(cd->col_isuk=='1'){
				td->has_uk= 1;
			}
			if(strlen(constraint_name)!=0)
				strcpy(cd->constraint_name, constraint_name);
			cd->cons_pos= cons_pos;
			int search_len= strlen(search_cond);
			if(search_len!=0){
				cd->search_cond= (char *)malloc(search_len+1);
				memset(cd->search_cond, 0, search_len+1);
				strcpy(cd->search_cond, search_cond);
				td->has_check= 1;
			}
			if(strlen(status)!=0){
				strcpy(cd->status, status);
			}
			cd->col_id= col_id;
			cd->precision= precision;
			cd->scale= scale;
			strcpy(cd->NULLABLE, nullable);
			int default_len= strlen(data_default);
			if(default_len!=0){
				cd->data_default= (char *)malloc(default_len+1);
				memset(cd->data_default, 0, default_len+1);
				strcpy(cd->data_default, data_default);
			}
			if(strlen(index_name)>0){
				td->has_index= 1;
				strcpy(cd->index_name, index_name);
				strcpy(cd->index_type, index_type);
			}
			if(strlen(uniqueness)>0){
				strcpy(cd->uniqueness, uniqueness);
			}

		}
	}

	SELECT_ONE_TABLE_EXIT:
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
	return ret;

}

int select_all_table(char *owner, DATABASE *db, int is_src){
	char sqlbuf[1024]={0};
	int pos= 0;
	sword ret= 0;
	OCIDefine *defcolp[30];
	sword indp[30];

	char tab_name[128]={0};
	char col_name[128]={0};
	char col_type[30]={0};
	int col_length= 0;
	int char_length= 0;
	char is_pk[3]={0};
	char is_uk[3]={0};
	char is_ck[3]={0};
	char constraint_name[100]={0};
	char search_cond[1024]={0};
	char status[10]={0};
	int col_id=0;
	int precision= 0;
	int scale= 0;
	char nullable[2]={0};
	char data_default[1024]={0};
	char index_type[10]={0};
	char index_name[50]={0};
	char uniqueness[12]={0};
	DEFTABLE *td=NULL;
	DEFCOLUMN *cd=NULL;
	int end= 1;
	char save_table[256]={0};
	int i= 0;
//	char cons_pos[3]= {0};
	int cons_pos= 0;

	pos= pos + sprintf(sqlbuf+pos, "SELECT\n");

	pos= pos + sprintf(sqlbuf+pos, "S.TABLE_NAME tab_name,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.COLUMN_NAME col_name,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.DATA_TYPE col_type,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.DATA_LENGTH col_length,");
	pos= pos + sprintf(sqlbuf+pos, "S.CHAR_LENGTH char_length,");
	pos= pos + sprintf(sqlbuf+pos, "(DECODE(PP.CONSTRAINT_TYPE, 'P', '1', '0'))col_ispk,\n");
	pos= pos + sprintf(sqlbuf+pos, "(DECODE(PP.CONSTRAINT_TYPE, 'U', '1', '0'))col_isuk,\n");
	pos= pos + sprintf(sqlbuf+pos, "(DECODE(PP.CONSTRAINT_TYPE, 'C', '1', '0'))col_isck,\n");
	pos= pos + sprintf(sqlbuf+pos, "PP.CONSTRAINT_NAME constraint_name,\n");
	pos= pos + sprintf(sqlbuf+pos, "PP.POSITION position,\n");
	pos= pos + sprintf(sqlbuf+pos, "PP.SEARCH_CONDITION search_condition,\n");
	pos= pos + sprintf(sqlbuf+pos, "PP.STATUS status,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.COLUMN_ID col_id,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.DATA_PRECISION precision,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.DATA_SCALE scale,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.NULLABLE nullable,\n");
	pos= pos + sprintf(sqlbuf+pos, "S.DATA_DEFAULT data_default,\n");
	pos= pos + sprintf(sqlbuf+pos, "TT.INDEX_TYPE index_type,\n");
	pos= pos + sprintf(sqlbuf+pos, "TT.INDEX_NAME index_name,\n");
	pos= pos + sprintf(sqlbuf+pos, "TT.UNIQUENESS uniqueness\n");

	pos= pos + sprintf(sqlbuf+pos, "FROM all_tab_columns S,\n");
	pos= pos + sprintf(sqlbuf+pos, "(SELECT CC.COLUMN_NAME, CC.POSITION, C.OWNER, C.TABLE_NAME, C.CONSTRAINT_TYPE, C.status, C.CONSTRAINT_NAME, C.SEARCH_CONDITION\n");
	pos= pos + sprintf(sqlbuf+pos, "FROM ALL_CONSTRAINTS C, ALL_CONS_COLUMNS CC\n");
	pos= pos + sprintf(sqlbuf+pos, "WHERE C.OWNER= CC.OWNER\n");
	pos= pos + sprintf(sqlbuf+pos, "AND C.CONSTRAINT_TYPE IN ('P','U','C')\n");
	pos= pos + sprintf(sqlbuf+pos, "AND C.CONSTRAINT_NAME=CC.CONSTRAINT_NAME\n");
	pos= pos + sprintf(sqlbuf+pos, "AND C.TABLE_NAME=CC.TABLE_NAME) PP,\n");
//	pos= pos + sprintf(sqlbuf+pos, "AND C.VALIDATED='VALIDATED') PP,\n");
	pos= pos + sprintf(sqlbuf+pos, "(SELECT t1.index_type,t1.UNIQUENESS,t1.INDEX_NAME,t1.TABLE_NAME,t1.TABLE_OWNER,t2.COLUMN_NAME\n");
	pos= pos + sprintf(sqlbuf+pos, "from all_indexes t1, all_ind_columns t2\n");
	pos= pos + sprintf(sqlbuf+pos, "WHERE t1.INDEX_NAME=t2.INDEX_NAME\n");
	pos= pos + sprintf(sqlbuf+pos, "AND t1.TABLE_OWNER=t2.INDEX_OWNER\n");
	pos= pos + sprintf(sqlbuf+pos, "AND t1.TABLE_NAME=t2.TABLE_NAME) TT\n");
	pos= pos + sprintf(sqlbuf+pos, "WHERE S.TABLE_NAME=PP.TABLE_NAME(+)\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.COLUMN_NAME=PP.COLUMN_NAME(+)\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.OWNER=PP.OWNER(+)\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.TABLE_NAME=TT.TABLE_NAME(+)\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.COLUMN_NAME=TT.COLUMN_NAME(+)\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.OWNER=TT.TABLE_OWNER(+)\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.TABLE_NAME NOT LIKE 'BIN$%'\n");
	pos= pos + sprintf(sqlbuf+pos, "AND S.OWNER='%s'\n", owner);
	pos= pos + sprintf(sqlbuf+pos, "ORDER BY S.TABLE_NAME, S.COLUMN_ID");

	msg("\n%s\n", sqlbuf);

	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 primary_key:%s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) tab_name,
			(sb4) sizeof(tab_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos:%s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) col_name,
			(sb4) sizeof(col_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) col_type,
			(sb4) sizeof(col_type), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &col_length,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &char_length,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) is_pk,
			(sb4) sizeof(is_pk), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) is_uk,
			(sb4) sizeof(is_uk), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) is_ck,
			(sb4) sizeof(is_ck), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) constraint_name,
			(sb4) sizeof(constraint_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &cons_pos,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) search_cond,
			(sb4) sizeof(search_cond), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) status,
			(sb4) sizeof(status), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &col_id,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &precision,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) &scale,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) nullable,
			(sb4) sizeof(nullable), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) data_default,
			(sb4) sizeof(data_default), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) index_type,
			(sb4) sizeof(index_type), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) index_name,
			(sb4) sizeof(index_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}
	i++;

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) uniqueness,
			(sb4) sizeof(uniqueness), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}

//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);

	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute unique:%s\n", db->errmsg);
		ret = -2;
		goto SELECT_ALL_TABLE_EXIT;
	}

	for (;;) {
		pos= 0;
		cons_pos= 0;
		precision= 0;
		scale= 0;
		char_length= 0;
		col_id=0;
		col_length= 0;
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			if(isListExists(&view_list, tab_name))
				continue;
			msg("%s,%s,%s,%d,c_len=%d,pk=%c,uk=%c,ck=%c,constraint=%s,cons_pos=%d,search_cond=%s,status=%s,col_id=%d,precision=%d, scale=%d, nullable=%s, data_default=%s, index_type=%s, index_name=%s, uniqueness=%s\n",
					tab_name, col_name, col_type, col_length,char_length, is_pk[0], is_uk[0], is_ck[0], constraint_name,cons_pos, search_cond,
					status, col_id, precision, scale, nullable, data_default, index_type, index_name, uniqueness);
			if(strcmp(save_table, tab_name)==0){
				end= 0;
			}else{
				end= 1;
			}
			if(strlen(save_table)==0 || end==1){
				strcpy(save_table, tab_name);
				if(td){
					if(is_src){
						tablelist.push_back(td);
					}else{
						tablelist_dst.push_back(td);
					}
				}
				td= (DEFTABLE *)malloc(sizeof(DEFTABLE));
				td->col_count= 0;
				td->has_check= 0;
				td->has_index= 0;
				td->has_pk= 0;
				td->has_uk= 0;
				memset(td->table_name, 0, sizeof(td->table_name));
				memset(td->primary_cons, 0, sizeof(td->primary_cons));
				strcpy(td->table_name, tab_name);
				td = (DEFTABLE *)realloc(td, sizeof(*td) + sizeof(DEFCOLUMN)
						* (td->col_count + 1));
				if (!td) {
					fprintf(stderr, "Realloc column for table error\n");
					exit(1);
				}
				cd = &td->defcolumn[td->col_count];
				td->col_count++;
				init_column(cd);
				strcpy(cd->col_name, col_name);
				strcpy(cd->col_type, col_type);
				cd->col_length= col_length;
				cd->char_length= char_length;
				cd->col_isck= is_ck[0];
				cd->col_isuk= is_uk[0];
				cd->col_ispk= is_pk[0];
				if(cd->col_ispk=='1'){
					td->has_pk= 1;
					strcpy(td->primary_cons, constraint_name);
				}
				if(cd->col_isuk=='1'){
					td->has_uk= 1;
				}
				if(strlen(constraint_name)!=0)
					strcpy(cd->constraint_name, constraint_name);
				cd->cons_pos= cons_pos;
				int search_len= strlen(search_cond);
				if(search_len!=0){
					cd->search_cond= (char *)malloc(search_len+1);
					memset(cd->search_cond, 0, search_len+1);
					strcpy(cd->search_cond, search_cond);
					td->has_check= 1;
				}
				if(strlen(status)!=0){
					strcpy(cd->status, status);
				}
				cd->col_id= col_id;
				cd->precision= precision;
				cd->scale= scale;
				strcpy(cd->NULLABLE, nullable);
				int default_len= strlen(data_default);
				if(default_len!=0){
					cd->data_default= (char *)malloc(default_len+1);
					memset(cd->data_default, 0, default_len+1);
					strcpy(cd->data_default, data_default);
				}
				if(strlen(index_name)>0){
					td->has_index= 1;
					strcpy(cd->index_name, index_name);
					strcpy(cd->index_type, index_type);
				}
				if(strlen(uniqueness)>0){
					strcpy(cd->uniqueness, uniqueness);
				}


			}else{
				td = (DEFTABLE *)realloc(td, sizeof(*td) + sizeof(DEFCOLUMN)
						* (td->col_count + 1));
				if (!td) {
					fprintf(stderr, "Realloc column for table error\n");
					exit(1);
				}
				cd = &td->defcolumn[td->col_count];
				td->col_count++;
				init_column(cd);
				strcpy(cd->col_name, col_name);
				strcpy(cd->col_type, col_type);
				cd->col_length= col_length;
				cd->char_length= char_length;
				cd->col_isck= is_ck[0];
				cd->col_isuk= is_uk[0];
				cd->col_ispk= is_pk[0];
				if(cd->col_ispk=='1'){
					td->has_pk= 1;
					strcpy(td->primary_cons, constraint_name);
				}
				if(cd->col_isuk=='1'){
					td->has_uk= 1;
				}
				if(strlen(constraint_name)!=0)
					strcpy(cd->constraint_name, constraint_name);
				cd->cons_pos= cons_pos;
				int search_len= strlen(search_cond);
				if(search_len!=0){
					cd->search_cond= (char *)malloc(search_len+1);
					memset(cd->search_cond, 0, search_len+1);
					strcpy(cd->search_cond, search_cond);
					td->has_check= 1;
				}
				if(strlen(status)!=0){
					strcpy(cd->status, status);
				}
				cd->col_id= col_id;
				cd->precision= precision;
				cd->scale= scale;
				strcpy(cd->NULLABLE, nullable);
				int default_len= strlen(data_default);
				if(default_len!=0){
					cd->data_default= (char *)malloc(default_len+1);
					memset(cd->data_default, 0, default_len+1);
					strcpy(cd->data_default, data_default);
				}
				if(strlen(index_name)>0){
					td->has_index= 1;
					strcpy(cd->index_name, index_name);
					strcpy(cd->index_type, index_type);
				}
				if(strlen(uniqueness)>0){
					strcpy(cd->uniqueness, uniqueness);
				}
			}

		}
	}
	if(td){
		if(is_src){
			tablelist.push_back(td);
		}else{
			tablelist_dst.push_back(td);
		}
	}

	SELECT_ALL_TABLE_EXIT:
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
	return ret;
}

int select_col_comments(DATABASE *db, DATABASE *dbdst, int do_level, char *ddl_file){
	char ddl_sql[1024]={0};
	char sqlbuf[256];
	char table_name[256]={0};
	char comments[4000]={0};
	char col_name[100]={0};
	OCIDefine *defcolp[3];
	sword indp[3];
	int ret=0;
	int pos= 0;
	int i= 0;

	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf,
			"select TABLE_NAME,COLUMN_NAME,COMMENTS from ALL_COL_COMMENTS where owner='%s' and comments!=' ' and TABLE_NAME NOT LIKE 'BIN$%' ORDER BY TABLE_NAME",
			login_info1.schema);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 select_col_comments:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) table_name,
			(sb4) sizeof(table_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	i++;
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos select_col_comments:%s\n", db->errmsg);
		ret = -2;
		goto select_col_comments_exit;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) col_name,
			(sb4) sizeof(col_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	i++;
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos select_col_comments:%s\n", db->errmsg);
		ret = -2;
		goto select_col_comments_exit;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) comments,
			(sb4) sizeof(comments), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	i++;
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos select_col_comments:%s\n", db->errmsg);
		ret = -2;
		goto select_col_comments_exit;
	}

//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtExecute select_col_comments:%s\n", db->errmsg);
		ret = -2;
		goto select_col_comments_exit;
	}

	for (;;) {
		pos= 0;
		memset(ddl_sql, 0, sizeof(ddl_sql));
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
				pos= pos + sprintf(ddl_sql, "comment on column %s.\"%s\".%s is '%s'",
						login_info2.schema, table_name, col_name, comments);
				msg("col_comments=%s\n", ddl_sql);
				if(do_level==1){
					prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
				}
				write_ddl(ddl_file, ddl_sql);
				memset(comments, 0, sizeof(comments));
				memset(col_name, 0, sizeof(col_name));
				memset(table_name, 0, sizeof(table_name));
		}
	}
	select_col_comments_exit:
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	return ret;

}

int select_table_comments(DATABASE *db, DATABASE *dbdst, int do_level, char *ddl_file){

	char ddl_sql[1024]={0};
	char sqlbuf[256];
	char table_name[256]={0};
	char comments[4000]={0};
	OCIDefine *defcolp[3];
	sword indp[3];
	int ret=0;
	int pos= 0;
	int i= 0;

	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf,
			"select TABLE_NAME,COMMENTS from ALL_TAB_COMMENTS where owner='%s' and table_type='TABLE' and comments!=' ' and TABLE_NAME NOT LIKE 'BIN$%' ORDER BY TABLE_NAME",
			login_info1.schema);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 select_table_comments:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) table_name,
			(sb4) sizeof(table_name), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	i++;
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos select_table_comments:%s\n", db->errmsg);
		ret = -2;
		goto select_table_comments_exit;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[i], db->errhp,
			(ub4) i+1,
			(dvoid *) comments,
			(sb4) sizeof(comments), SQLT_STR, (dvoid *) &indp[i], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	i++;
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos select_table_comments:%s\n", db->errmsg);
		ret = -2;
		goto select_table_comments_exit;
	}

//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute select_table_comments:%s\n", db->errmsg);
		ret = -2;
		goto select_table_comments_exit;
	}

	for (;;) {
		pos= 0;
		memset(ddl_sql, 0, sizeof(ddl_sql));
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
				pos= pos + sprintf(ddl_sql, "comment on table %s.\"%s\" is '%s'",
						login_info2.schema, table_name, comments);
				msg("col_comments=%s\n", ddl_sql);
				if(do_level==1){
					prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
				}
				write_ddl(ddl_file, ddl_sql);
				memset(comments, 0, sizeof(comments));
				memset(table_name, 0, sizeof(table_name));
		}
	}
	select_table_comments_exit:
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	return ret;

}

void create_one_table(DATABASE *db, DATABASE *dbdst, int do_level,
		char *ddl_table_file, char *ddl_file, DEFTABLE *td){

	list<string> colnamelist;
	list<string> pknamelist;
	DEFCOLUMN *cd;
	int pos= 0;
	int cpos= 0;
	int col;
	int ret;
	char ddl_sql[81920];

	colnamelist.clear();
	pknamelist.clear();
	pos= 0;
	cpos= 0;
	memset(ddl_sql, 0, sizeof(ddl_sql));
	pos= pos + sprintf(ddl_sql+pos, "CREATE TABLE %s.\"%s\" (\n", login_info2.schema, td->table_name);
	for(col=0; col<td->col_count; col++){
		cd= &td->defcolumn[col];
		ret= isListExists(&colnamelist, cd->col_name);
		if(ret==1)
			continue;
		colnamelist.push_back(cd->col_name);
		pos= combine_sql(pos, &cpos, ddl_sql, cd->col_type, cd->precision, cd->scale, cd->col_length, cd->col_name, cd->NULLABLE, cd->data_default, cd->char_length);
	}
	if(td->has_pk){
		cpos= 0;
		int pk_status= 1;
		pos= pos + sprintf(ddl_sql+pos, ",\n CONSTRAINT \"%s\" PRIMARY KEY(", td->primary_cons);
		for(col=0; col<td->col_count; col++){
			cd= &td->defcolumn[col];
			if(cd->col_ispk=='1'){
				ret= isListExists(&pknamelist, cd->col_name);
				if(ret==1)
					continue;
				pknamelist.push_back(cd->col_name);
				if(strcmp(cd->status, "ENABLED")==0){
					pk_status= 1;
				}else{
					pk_status= 0;
				}
				if(cpos>0){
					pos= pos + sprintf(ddl_sql+pos, ",");
				}
				pos= pos + sprintf(ddl_sql+pos, "\"%s\"", cd->col_name);
				cpos++;
			}
		}
		if(pk_status==1){
			pos= pos + sprintf(ddl_sql+pos, ") ENABLE");
		}else{
			pos= pos + sprintf(ddl_sql+pos, ") DISABLE");
		}
	}

	if(td->has_uk){
		list<DEFUNIQUECONS *> unique_list;
		list<DEFUNIQUECONS *>::iterator uit;
		DEFUNIQUECONS *node;
		unique_list.clear();
		for(col=0; col<td->col_count; col++){
			cd= &td->defcolumn[col];
			if(cd->col_isuk=='1'){
				node= hasSameUniqueName2(&unique_list, cd->constraint_name);
				if(!node){
					node= (DEFUNIQUECONS *)malloc(sizeof(DEFUNIQUECONS));
					sprintf(node->status, "%s", cd->status);
					sprintf(node->cons_name, "%s", cd->constraint_name);
					node->num= 1;
					unique_list.push_back(node);
				}else{
					node->num++;
				}
			}
		}

		for(uit=unique_list.begin(); uit!=unique_list.end(); uit++){
			node=(*uit);
			int k;
			pos= pos + sprintf(ddl_sql+pos, ",\n CONSTRAINT \"%s\" unique (", node->cons_name);
			cpos= 0;
			for(k=0; k<node->num; k++){
				for(col=0; col<td->col_count; col++){
					cd= &td->defcolumn[col];
					if(cd->cons_pos==(k+1) && cd->col_isuk=='1'){
						if(cpos>0){
							pos= pos + sprintf(ddl_sql+pos, ", ");
						}
						pos= pos + sprintf(ddl_sql+pos, "%s", cd->col_name);
						cpos++;
						break;
					}
				}
			}

			pos= pos + sprintf(ddl_sql+pos, ")");
			if(strstr(node->status, "ENABLED")){
				pos= pos + sprintf(ddl_sql+pos, " ENABLE");
			}else{
				pos= pos + sprintf(ddl_sql+pos, " DISABLE");
			}
			free(node);
		}

	}
	pos= pos + sprintf(ddl_sql+pos, ")");
	msg("%s\n", ddl_sql);
	write_ddl(ddl_file, ddl_sql);
	if(do_level==1){
		msg("execute create table %s ddl\n", td->table_name);
		ret= prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
		if(ret==0 && do_index){
			if(td->has_index)
				add_index2(td, do_level, ddl_file, dbdst);
		}
		if(ret==0 && do_check){
			if(td->has_check){
				add_check_constraint2(td, do_level, ddl_file, dbdst);
			}
		}
	}else{
		if(do_index && td->has_index)
			add_index2(td, do_level, ddl_file, dbdst);
		if(do_check && td->has_check)
			add_check_constraint2(td, do_level, ddl_file, dbdst);
	}
}

void create_table_from_tablelist(DATABASE *db, DATABASE *dbdst, int do_level,
		char *ddl_table_file, char *ddl_file){
	if(tablelist.size()==0){
		msg("tablelist is null\n");
		return;
	}
	list<DEFTABLE *>::iterator it;
	list<string> colnamelist;
	list<string> pknamelist;

	DEFTABLE *td;
	DEFCOLUMN *cd;
	int pos= 0;
	int cpos= 0;
	int col;
	int ret;
	char ddl_sql[81920];
	for(it=tablelist.begin(); it!=tablelist.end(); it++){
		td= (*it);

		if(dif_table_list.size()>0 && !isListExists(&dif_table_list, td->table_name)){
			msg("filter %s\n", td->table_name);
			continue;
		}
		colnamelist.clear();
		pknamelist.clear();
		pos= 0;
		cpos= 0;
		memset(ddl_sql, 0, sizeof(ddl_sql));
		msg("create %s, col=%d\n", td->table_name, td->col_count);
		pos= pos + sprintf(ddl_sql+pos, "CREATE TABLE %s.\"%s\" (\n", login_info2.schema, td->table_name);
		for(col=0; col<td->col_count; col++){
			cd= &td->defcolumn[col];
//			msg("col_name=%s\n", cd->col_name);
			ret= isListExists(&colnamelist, cd->col_name);
			if(ret==1)
				continue;
			colnamelist.push_back(cd->col_name);
			pos= combine_sql(pos, &cpos, ddl_sql, cd->col_type, cd->precision, cd->scale, cd->col_length, cd->col_name, cd->NULLABLE, cd->data_default, cd->char_length);
		}
		if(td->has_pk){
			cpos= 0;
			int pk_status= 1;
			pos= pos + sprintf(ddl_sql+pos, ",\n CONSTRAINT \"%s\" PRIMARY KEY(", td->primary_cons);

			int pk_num= 0;
			int pk_pos= 0;
			for(col=0; col<td->col_count; col++){
				cd= &td->defcolumn[col];
				if(cd->col_ispk=='1' && cd->cons_pos!=pk_pos){
					pk_num++;
					pk_pos= cd->cons_pos;
				}
			}
			msg("pk_num=%d\n", pk_num);
			int pk_i= 1;
			while(pk_i<=pk_num){
				for(col=0; col<td->col_count; col++){
					cd= &td->defcolumn[col];
					if(cd->col_ispk=='1' && cd->cons_pos==pk_i){
						ret= isListExists(&pknamelist, cd->col_name);
						if(ret==1)
							continue;
						pknamelist.push_back(cd->col_name);
						if(strcmp(cd->status, "ENABLED")==0){
							pk_status= 1;
						}else{
							pk_status= 0;
						}
						if(cpos>0){
							pos= pos + sprintf(ddl_sql+pos, ",");
						}
						pos= pos + sprintf(ddl_sql+pos, "\"%s\"", cd->col_name);
						cpos++;

						pk_i++;
						break;
					}
				}
			}
			if(pk_status==1){
				pos= pos + sprintf(ddl_sql+pos, ") ENABLE");
			}else{
				pos= pos + sprintf(ddl_sql+pos, ") DISABLE");
			}
		}

		if(td->has_uk){
			list<DEFUNIQUECONS *> unique_list;
			list<DEFUNIQUECONS *>::iterator uit;
			DEFUNIQUECONS *node;
			unique_list.clear();
			for(col=0; col<td->col_count; col++){
				cd= &td->defcolumn[col];
				if(cd->col_isuk=='1'){
					node= hasSameUniqueName2(&unique_list, cd->constraint_name);
					if(!node){
						node= (DEFUNIQUECONS *)malloc(sizeof(DEFUNIQUECONS));
						sprintf(node->status, "%s", cd->status);
						sprintf(node->cons_name, "%s", cd->constraint_name);
						node->num= 1;
						unique_list.push_back(node);
					}else{
						node->num++;
					}
				}
			}

			for(uit=unique_list.begin(); uit!=unique_list.end(); uit++){
				node=(*uit);
				int k;
				pos= pos + sprintf(ddl_sql+pos, ",\n CONSTRAINT \"%s\" unique (", node->cons_name);
				cpos= 0;
				for(k=0; k<node->num; k++){
					for(col=0; col<td->col_count; col++){
						cd= &td->defcolumn[col];
						if(cd->cons_pos==(k+1) && cd->col_isuk=='1' && strcmp(cd->constraint_name, node->cons_name)==0){
							if(cpos>0){
								pos= pos + sprintf(ddl_sql+pos, ", ");
							}
							pos= pos + sprintf(ddl_sql+pos, "%s", cd->col_name);
							cpos++;
							break;
						}
					}
				}

				pos= pos + sprintf(ddl_sql+pos, ")");
				if(strstr(node->status, "ENABLED")){
					pos= pos + sprintf(ddl_sql+pos, " ENABLE");
				}else{
					pos= pos + sprintf(ddl_sql+pos, " DISABLE");
				}
				free(node);
			}

		}
		pos= pos + sprintf(ddl_sql+pos, ")");
		msg("%s\n", ddl_sql);
		write_ddl(ddl_file, ddl_sql);
		if(do_level==1){
			msg("execute create table %s ddl\n", td->table_name);
			ret= prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
			if(ret==0 && do_index){
				if(td->has_index)
					add_index2(td, do_level, ddl_file, dbdst);
			}
			if(ret==0 && do_check){
				if(td->has_check){
					add_check_constraint2(td, do_level, ddl_file, dbdst);
				}
			}
		}else{
			if(do_index && td->has_index)
				add_index2(td, do_level, ddl_file, dbdst);
			if(do_check && td->has_check)
				add_check_constraint2(td, do_level, ddl_file, dbdst);
		}
	}
}

void describe_table (text *objname, DATABASE *db, TABLE_DEFINE *table_def)
{
  sword     retval;
  OCIParam *parmp, *collst;
  ub2       numcols;
  ub4       objid = 0;
  OCIDescribe *dschp;
  ub1 is_part=0;
  ub1 is_cluster=0;
  char table_name[256]={0};

  checkerr (db->errhp, OCIHandleAlloc((dvoid *) db->envhp, (dvoid **) &dschp,
	                           (ub4) OCI_HTYPE_DESCRIBE,
	                           (size_t) 0, (dvoid **) 0));
  sprintf(table_name, "\"%s\"", objname);
  alarm(30);
  if ((retval = OCIDescribeAny(db->svchp, db->errhp, (dvoid *)table_name,
                               (ub4) strlen((char *) table_name),
                               OCI_OTYPE_NAME, (ub1)OCI_DEFAULT,
                               OCI_PTYPE_TABLE, dschp)) != OCI_SUCCESS)
  {
	  alarm(0);
    if (retval == OCI_NO_DATA)
    {
      printf("NO DATA: OCIDescribeAny on %s\n", objname);
    }
    else                                                      /* OCI_ERROR */
    {
      printf( "ERROR: OCIDescribeAny on %s\n", objname);
      checkerr(db->errhp, retval);
      return;
    }
  }
  else
  {
	  alarm(0);
                                           /* get the parameter descriptor */
    checkerr (db->errhp, OCIAttrGet((dvoid *)dschp, (ub4)OCI_HTYPE_DESCRIBE,
                         (dvoid *)&parmp, (ub4 *)0, (ub4)OCI_ATTR_PARAM,
                         (OCIError *)db->errhp));

                                        /* Get the attributes of the table */
    checkerr (db->errhp, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &objid, (ub4 *) 0,
                         (ub4) OCI_ATTR_OBJID, (OCIError *)db->errhp));

    checkerr (db->errhp, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &is_part, (ub4 *) 0,
                         (ub4) OCI_ATTR_PARTITIONED, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &is_cluster, (ub4 *) 0,
                         (ub4) OCI_ATTR_CLUSTERED, (OCIError *)db->errhp));
    table_def->table_id= objid;
    table_def->is_partition= is_part;
    table_def->is_cluster= is_cluster;
//    msg("table_id=%d, is_part=%d\n", objid, is_part);
                                               /* column list of the table */
    checkerr (db->errhp, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &collst, (ub4 *) 0,
                         (ub4) OCI_ATTR_LIST_COLUMNS, (OCIError *)db->errhp));
                                                      /* number of columns */
    checkerr (db->errhp, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &numcols, (ub4 *) 0,
                         (ub4) OCI_ATTR_NUM_COLS, (OCIError *)db->errhp));
//    table_def->col_count= numcols;
                                               /* now describe each column */
    describe_column(collst, numcols, db, table_def);
  }
                                               /* free the describe handle */
  OCIHandleFree((dvoid *) dschp, (ub4) OCI_HTYPE_DESCRIBE);
}

static void describe_column(OCIParam *parmp, ub4 parmcnt, DATABASE *db, TABLE_DEFINE *table_def)
{
  text      colname1[NPOS][128], colname2[NPOS][128], colname3[NPOS][128];
  text     *namep;
  ub4       sizep;
  ub2       collen[NPOS];
  ub2       coltyp[NPOS];
  OCIParam *parmdp;
  ub4       i, pos;
  sword     retval;
  ub1       precision[NPOS];
  sb1       scale[NPOS];
  COLUMN_DEFINE *column_def;
//  OCITypeCode tpc;

  for (pos = 1; pos <= parmcnt; pos++)
  {
                            /* get the parameter descriptor for each column */
    checkerr (db->errhp, OCIParamGet((dvoid *)parmp, (ub4)OCI_DTYPE_PARAM, db->errhp,
                       (dvoid **)(&parmdp), (ub4) pos));
                                                           /* column length */
    checkerr (db->errhp, OCIAttrGet((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
                      (dvoid*) &collen[pos-1], (ub4 *) 0,
                      (ub4) OCI_ATTR_DATA_SIZE, (OCIError *)db->errhp));
                                                             /* column name */
    checkerr (db->errhp, OCIAttrGet((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
                    (dvoid*) &namep, (ub4 *) &sizep,
                    (ub4) OCI_ATTR_NAME, (OCIError *)db->errhp));

    strncpy((char *)colname1[pos-1], (char *)namep, (size_t) sizep);
    colname1[pos-1][sizep] = '\0';
                                                            /* schema name */
    checkerr (db->errhp, OCIAttrGet((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &namep, (ub4 *) &sizep,
                         (ub4) OCI_ATTR_SCHEMA_NAME, (OCIError *)db->errhp));

    strncpy((char *)colname2[pos-1], (char *)namep, (size_t) sizep);
    colname2[pos-1][sizep] = '\0';
                                                              /* type name */
    checkerr (db->errhp, OCIAttrGet((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &namep, (ub4 *) &sizep,
                         (ub4) OCI_ATTR_TYPE_NAME, (OCIError *)db->errhp));

    strncpy((char *)colname3[pos-1], (char *)namep, (size_t) sizep);
    colname3[pos-1][sizep] = '\0';

                                                              /* data type */
    checkerr (db->errhp, OCIAttrGet((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &coltyp[pos-1], (ub4 *) 0,
                         (ub4) OCI_ATTR_DATA_TYPE, (OCIError *)db->errhp));

                                                              /* precision */
    checkerr (db->errhp, OCIAttrGet ((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
                          (dvoid*) &precision[pos-1], (ub4 *) 0,
                          (ub4) OCI_ATTR_PRECISION, (OCIError *)db->errhp));

                                                                  /* scale */
    checkerr (db->errhp, OCIAttrGet ((dvoid*) parmdp, (ub4) OCI_DTYPE_PARAM,
                        (dvoid*) &scale[pos-1], (ub4 *) 0,
                        (ub4) OCI_ATTR_SCALE, (OCIError *)db->errhp));

//    table_def->columns->push_back(column_def);
//    printf("%s, type=%d\n", colname1[pos-1], coltyp[pos-1]);
    /* if column or attribute is type OBJECT/COLLECTION, describe it by ref */
    if (coltyp[pos-1] == OCI_TYPECODE_OBJECT ||
        coltyp[pos-1] == OCI_TYPECODE_NAMEDCOLLECTION)
    {
    	table_def->has_object_type= true;
//		OCIDescribe *deshp;
//		OCIParam    *parmhp;
//		OCIRef      *typeref= NULL;
//		OCIAttrGet ((dvoid *)parmdp, (ub4)OCI_DTYPE_PARAM, &typeref, (ub4 *)0,
//							(ub4)OCI_ATTR_REF_TDO, (OCIError *)db->errhp);
    }
//    	continue;
//		OCIDescribe *deshp;
//		OCIParam    *parmhp;
//		OCIRef      *typeref;
//		msg("is type object\n");
//										/* get ref to attribute/column type */
//		checkerr (db->errhp, OCIAttrGet ((dvoid *)parmdp, (ub4)OCI_DTYPE_PARAM,
//							(dvoid *)&typeref, (ub4 *)0,
//							(ub4)OCI_ATTR_REF_TDO, (OCIError *)db->errhp));
//															 /* describe it */
//		checkerr (db->errhp, OCIHandleAlloc((dvoid *)db->envhp, (dvoid **)&deshp,
//							(ub4)OCI_HTYPE_DESCRIBE, (size_t)0, (dvoid **)0));
//
//		checkerr (db->errhp, OCIDescribeAny(db->svchp, db->errhp, (dvoid *)typeref, (ub4)0,
//							   OCI_OTYPE_REF, (ub1)1, OCI_PTYPE_TYPE, deshp));
//											/* get the parameter descriptor */
//		checkerr (db->errhp, OCIAttrGet((dvoid *)deshp, (ub4)OCI_HTYPE_DESCRIBE,
//						   (dvoid *)&parmhp, (ub4 *)0, (ub4)OCI_ATTR_PARAM,
//						   (OCIError *)db->errhp));
//													   /* describe the type */
//		describe_type (parmhp, db);
//
//													/* free describe handle */
//		OCIHandleFree((dvoid *) deshp, (ub4) OCI_HTYPE_DESCRIBE);
//    }else{
    	column_def= (COLUMN_DEFINE *)malloc(sizeof(COLUMN_DEFINE));
    	column_def->type_length= collen[pos-1];
        memset(column_def->col_name, 0, sizeof(column_def->col_name));
        strcpy(column_def->col_name, (char *)colname1[pos-1]);
        memset(column_def->col_type_name, 0, sizeof(column_def->col_type_name));
        strcpy(column_def->col_type_name, (char *)colname3[pos-1]);
        column_def->col_type= coltyp[pos-1];
        column_def->precision= precision[pos-1];
        column_def->scale= scale[pos-1];
    	table_def->columns->push_back(column_def);
    	table_def->col_count++;
//    }
  }

//  printf ("\n------------------\n");
//  printf ("TABLE : %s \n", tablename);
#if 0
  printf (
    "\nColumn Name    Schema  Length   Type    Datatype  Precision   Scale\n");
  printf (
    "_____________________________________________________________________\n");
  for (i = 1; i <= parmcnt; i++)
    printf( "%10s%10s%6d%10s%10d%10d%10d\n", colname1[i-1], colname2[i-1],
          collen[i-1], colname3[i-1], coltyp[i-1], precision[i-1], scale[i-1]);
#endif
}


static void describe_type(OCIParam  *type_parmp, DATABASE *db)
{
  sword         retval;
  OCITypeCode   typecode,
                collection_typecode;
  text          schema_name[MAXLEN],
                version_name[MAXLEN],
                type_name[MAXLEN];
  text         *namep;
  ub4           size;                                           /* not used */
  OCIRef       *type_ref;                                       /* not used */
  ub2           num_attr,
                num_method;
  ub1           is_incomplete,
                has_table;
  OCIParam     *list_attr,
               *list_method,
               *map_method,
               *order_method,
               *collection_elem;

  printf ("\n\n-----------------\n");
  printf ("USED-DEFINED TYPE\n-----------------\n");

  checkerr (db->errhp, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
                    (dvoid*) &namep, (ub4 *) &size,
                    (ub4) OCI_ATTR_SCHEMA_NAME, (OCIError *) db->errhp));
  strncpy((char *)schema_name, (char *)namep, (size_t) size);
  schema_name[size] = '\0';
  printf ( "Schema:            %s\n", schema_name);

  checkerr (db->errhp, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
                    (dvoid*) &namep, (ub4 *) &size,
                    (ub4) OCI_ATTR_NAME, (OCIError *) db->errhp));
  strncpy ((char *)type_name, (char *)namep, (size_t) size);
  type_name[size] = '\0';
  printf ( "Name:              %s\n", type_name);

                      /* get ref of type, although we are not using it here */
  checkerr (db->errhp, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&type_ref, (ub4 *)0,
                    (ub4)OCI_ATTR_REF_TDO, (OCIError *)db->errhp));

  checkerr (db->errhp, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
                    (dvoid*) &typecode, (ub4 *) 0,
                    (ub4) OCI_ATTR_TYPECODE, (OCIError *) db->errhp));
  printf ( "Oracle Typecode:   %d\n", typecode);

  checkerr (db->errhp, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
                    (dvoid*) &namep, (ub4 *) &size,
                    (ub4) OCI_ATTR_VERSION, (OCIError *) db->errhp));
  strncpy ((char *)version_name, (char *)namep, (size_t) size);
  version_name[size] = '\0';
  printf ( "Version:           %s\n", version_name);

  checkerr (db->errhp, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
                    (dvoid*) &is_incomplete, (ub4 *) 0,
                    (ub4) OCI_ATTR_IS_INCOMPLETE_TYPE, (OCIError *)db->errhp));
  printf ( "Is incomplete:     %d\n", is_incomplete);

  checkerr (db->errhp, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
                    (dvoid*) &has_table, (ub4 *) 0,
                    (ub4) OCI_ATTR_HAS_NESTED_TABLE, (OCIError *)db->errhp));
  printf ( "Has nested table:  %d\n", has_table);

                                         /* describe type attributes if any */
  checkerr (db->errhp, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
                    (dvoid*) &num_attr, (ub4 *) 0,
                    (ub4) OCI_ATTR_NUM_TYPE_ATTRS, (OCIError *) db->errhp));
  printf ( "Number of attrs:   %d\n", num_attr);
  if (num_attr > 0)
  {
                               /* get the list of attributes and pass it on */
    checkerr (db->errhp, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&list_attr, (ub4 *)0,
                    (ub4)OCI_ATTR_LIST_TYPE_ATTRS, (OCIError *)db->errhp));
    describe_typeattr(list_attr, num_attr, db);
  }

            /* describe the collection element if this is a collection type */
  if (typecode == OCI_TYPECODE_NAMEDCOLLECTION)
  {
    checkerr (db->errhp, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&collection_typecode, (ub4 *)0,
                      (ub4)OCI_ATTR_COLLECTION_TYPECODE, (OCIError *)db->errhp));
    printf ( "Collection typecode: %d\n", collection_typecode);

    checkerr (db->errhp, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&collection_elem, (ub4 *)0,
                      (ub4)OCI_ATTR_COLLECTION_ELEMENT, (OCIError *)db->errhp));

    describe_typecoll(collection_elem, collection_typecode, db);
  }

                                          /* describe the MAP method if any */
  checkerr (db->errhp, OCIAttrGet((dvoid*) type_parmp, (ub4) OCI_DTYPE_PARAM,
                    (dvoid*) &map_method, (ub4 *) 0,
                    (ub4) OCI_ATTR_MAP_METHOD, (OCIError *)db->errhp));
  if (map_method != (dvoid *)0)
    describe_typemethod(map_method,(text *)"TYPE MAP METHOD\n---------------", db);

  /* describe the ORDER method if any; note that this is mutually exclusive */
  /* with MAP                                                               */
  checkerr (db->errhp, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&order_method, (ub4 *)0,
                    (ub4)OCI_ATTR_ORDER_METHOD, (OCIError *)db->errhp));
  if (order_method != (dvoid *)0)
    describe_typemethod(order_method,
                        (text *)"TYPE ORDER METHOD\n-----------------", db);

                              /* describe all methods (including MAP/ORDER) */
  checkerr (db->errhp, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&num_method, (ub4 *)0,
                    (ub4)OCI_ATTR_NUM_TYPE_METHODS, (OCIError *)db->errhp));
  printf("Number of methods: %d\n", num_method);
  if (num_method > 0)
  {
    checkerr (db->errhp, OCIAttrGet((dvoid *)type_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&list_method, (ub4 *)0,
                      (ub4)OCI_ATTR_LIST_TYPE_METHODS, (OCIError *)db->errhp));

    describe_typemethodlist(list_method, num_method,
                           (text *)"TYPE METHOD\n-----------", db);
  }
}

static void   describe_typeattr(OCIParam *attrlist_parmp, ub4 num_attr, DATABASE *db)
{
  OCIParam     *attr_parmp;
  sword         retval;
  text         *attr_name,
               *schema_name,
               *type_name;
  ub4           namesize, snamesize, tnamesize;
  ub4           size;
  ub2           datasize;
  OCITypeCode   typecode;
  ub2           datatype;
  ub1           precision;
  sb1           scale;
  ub4           i,
                pos;

  printf(
     "\nAttr Name      Schema      Type        Length Typ Datatyp Pre Scal\n");
  printf(
     "____________________________________________________________________\n");

  for (pos = 1; pos <= num_attr; pos++)
  {
                  /* get the attribute's describe handle from the parameter */
    checkerr (db->errhp, OCIParamGet((dvoid *)attrlist_parmp, (ub4)OCI_DTYPE_PARAM,
    		db->errhp, (dvoid **)&attr_parmp, (ub4)pos));

                       /* obtain attribute values for the type's attributes */
    checkerr (db->errhp, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&attr_name, (ub4 *)&namesize,
                      (ub4)OCI_ATTR_NAME, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&schema_name, (ub4 *)&snamesize,
                      (ub4)OCI_ATTR_SCHEMA_NAME, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&type_name, (ub4 *)&tnamesize,
                      (ub4)OCI_ATTR_TYPE_NAME, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&datasize, (ub4 *)0,
                      (ub4)OCI_ATTR_DATA_SIZE, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&typecode, (ub4 *)0,
                      (ub4)OCI_ATTR_TYPECODE, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&datatype, (ub4 *)0,
                      (ub4)OCI_ATTR_DATA_TYPE, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&precision, (ub4 *)0,
                      (ub4)OCI_ATTR_PRECISION, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)attr_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&scale, (ub4 *)0,
                      (ub4)OCI_ATTR_SCALE, (OCIError *)db->errhp));

    /* if typecode == OCI_TYPECODE_OBJECT, you can proceed to describe it
       recursively by calling describe_type() with its name; or you can
       obtain its OCIRef by using OCI_ATTR_REF_TDO, and then describing the
       type by REF                                                          */

                                          /* print values for the attribute */
    printf("%10.*s%10.*s%16.*s%8d%4d%8d%4d%5d\n", namesize, attr_name,
                snamesize, schema_name, tnamesize, type_name, datasize,
                typecode, datatype, precision, scale);
  }
  printf("\n");
}


static void  describe_typecoll(OCIParam *collelem_parmp, sword coll_typecode, DATABASE *db)
{
  text         *attr_name,
               *schema_name,
               *type_name;
  ub4           size;
  ub2           datasize;
  ub4           num_elems;
  OCITypeCode   typecode;
  ub2           datatype;
  sword         retval;

  checkerr (db->errhp, OCIAttrGet((dvoid *)collelem_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&schema_name, (ub4 *)&size,
                    (ub4)OCI_ATTR_SCHEMA_NAME, (OCIError *)db->errhp));
  checkerr (db->errhp, OCIAttrGet((dvoid *)collelem_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&type_name, (ub4 *)&size,
                    (ub4)OCI_ATTR_TYPE_NAME, (OCIError *)db->errhp));
  checkerr (db->errhp, OCIAttrGet((dvoid *)collelem_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&datasize, (ub4 *)0,
                    (ub4)OCI_ATTR_DATA_SIZE, (OCIError *)db->errhp));
  checkerr (db->errhp, OCIAttrGet((dvoid *)collelem_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&typecode, (ub4 *)0,
                    (ub4)OCI_ATTR_TYPECODE, (OCIError *)db->errhp));
  checkerr (db->errhp, OCIAttrGet((dvoid *)collelem_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&datatype, (ub4 *)0,
                    (ub4)OCI_ATTR_DATA_TYPE, (OCIError *)db->errhp));
  if (coll_typecode == OCI_TYPECODE_VARRAY)
    checkerr (db->errhp, OCIAttrGet((dvoid *)collelem_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&num_elems, (ub4 *)0,
                      (ub4)OCI_ATTR_NUM_ELEMS, (OCIError *)db->errhp));
  else num_elems = 0;

  printf("Schema    Type            Length   Type Datatype Elements\n");
  printf("_________________________________________________________\n");

  printf("%10s%16s%9d%5d%9d%8d\n", schema_name, type_name,
         datasize, typecode, datatype, num_elems);
}

static void   describe_typemethodlist(OCIParam *methodlist_parmp, ub4 num_method, text *comment, DATABASE *db)
{
  sword      retval;
  OCIParam  *method_parmp;
  ub4        i, pos;
                                                /* traverse the method list */
  for (pos = 1; pos <= num_method; pos++)
  {
    checkerr (db->errhp, OCIParamGet((dvoid *)methodlist_parmp,
                                 (ub4)OCI_DTYPE_PARAM, db->errhp,
                                 (dvoid **)&method_parmp, (ub4)pos));
    describe_typemethod(method_parmp, comment, db);
  }
}

static void   describe_typemethod(OCIParam *method_parmp, text *comment, DATABASE *db)
{
  sword          retval;
  text          *method_name;
  ub4            size;
  ub2            ovrid;
  ub4            num_arg;
  ub1            has_result,
                 is_map,
                 is_order;
  OCITypeEncap   encap;
  OCIParam      *list_arg;

                                                            /* print header */
  printf("\n%s\n", comment);

  checkerr (db->errhp, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&method_name, (ub4 *)&size,
                    (ub4)OCI_ATTR_NAME, (OCIError *)db->errhp));
  printf("Method Name:       %s\n", method_name);

  checkerr (db->errhp, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&ovrid, (ub4 *)0,
                    (ub4)OCI_ATTR_OVERLOAD_ID, (OCIError *)db->errhp));
  printf("Overload ID:       %d\n", ovrid);

  checkerr (db->errhp, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&encap, (ub4 *)0,
                    (ub4)OCI_ATTR_ENCAPSULATION, (OCIError *)db->errhp));
  printf("Encapsulation:     %s\n",
         (encap == OCI_TYPEENCAP_PUBLIC) ? "public" : "private");

  checkerr (db->errhp, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&is_map, (ub4 *)0,
                    (ub4)OCI_ATTR_IS_MAP, (OCIError *)db->errhp));
  printf("Is map:            %d\n", is_map);

  checkerr (db->errhp, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&is_order, (ub4 *)0,
                    (ub4)OCI_ATTR_IS_ORDER, (OCIError *)db->errhp));
  printf("Is order:          %d\n", is_order);

                             /* retrieve the argument list, includes result */
  checkerr (db->errhp, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&list_arg, (ub4 *)0,
                    (ub4)OCI_ATTR_LIST_ARGUMENTS, (OCIError *)db->errhp));

               /* if this is a function (has results, then describe results */
  checkerr (db->errhp, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&has_result, (ub4 *)0,
                    (ub4)OCI_ATTR_HAS_RESULT, (OCIError *)db->errhp));
  printf("Has result:        %d\n", has_result);
  if (has_result)
  {
    describe_typearg(list_arg, OCI_PTYPE_TYPE_RESULT, 0, 1, db);
  }

                                                  /* describe each argument */
  checkerr (db->errhp, OCIAttrGet((dvoid *)method_parmp, (ub4)OCI_DTYPE_PARAM,
                    (dvoid *)&num_arg, (ub4 *)0,
                    (ub4)OCI_ATTR_NUM_ARGS, (OCIError *)db->errhp));
  printf("Number of args:    %d\n", num_arg);
  if (num_arg > 0)
  {
    describe_typearg(list_arg, OCI_PTYPE_TYPE_ARG, 1, num_arg+1, db);
  }
}

static void   describe_typearg (OCIParam *arglist_parmp, ub1 type, ub4 start, ub4 end, DATABASE *db)
{
  OCIParam          *arg_parmp;
  sword              retval;
  text              *arg_name,
                    *schema_name,
                    *type_name;
  ub2                position;
  ub2                level;
  ub1                has_default;
  OCITypeParamMode   iomode;
  ub4                size;
  OCITypeCode        typecode;
  ub2                datatype;
  ub4                i,
                     pos;

  /* print header */
  printf("Name    Pos   Type Datatype Lvl Def Iomode SchName TypeName\n");
  printf(
      "________________________________________________________________\n");

  for (pos = start; pos < end; pos++)
  {
                  /* get the attribute's describe handle from the parameter */
    checkerr (db->errhp, OCIParamGet((dvoid *)arglist_parmp, (ub4)OCI_DTYPE_PARAM,
    		db->errhp, (dvoid **)&arg_parmp, (ub4)pos));

                       /* obtain attribute values for the type's attributes */
                  /* if this is a result, it has no name, so we give it one */
    if (type == OCI_PTYPE_TYPE_RESULT)
    {
      arg_name = (text *)"RESULT";
    }
    else if (type == OCI_PTYPE_TYPE_ARG)
    {
      checkerr (db->errhp, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
                        (dvoid *)&arg_name, (ub4 *)&size,
                        (ub4)OCI_ATTR_NAME, (OCIError *)db->errhp));
    }
    checkerr (db->errhp, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&position, (ub4 *)0,
                      (ub4)OCI_ATTR_POSITION, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&typecode, (ub4 *)0,
                      (ub4)OCI_ATTR_TYPECODE, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&datatype, (ub4 *)0,
                      (ub4)OCI_ATTR_DATA_TYPE, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&level, (ub4 *)0,
                      (ub4)OCI_ATTR_LEVEL, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&has_default, (ub4 *)0,
                      (ub4)OCI_ATTR_HAS_DEFAULT, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&iomode, (ub4 *)0,
                      (ub4)OCI_ATTR_IOMODE, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&schema_name, (ub4 *)&size,
                      (ub4)OCI_ATTR_SCHEMA_NAME, (OCIError *)db->errhp));
    checkerr (db->errhp, OCIAttrGet((dvoid *)arg_parmp, (ub4)OCI_DTYPE_PARAM,
                      (dvoid *)&type_name, (ub4 *)&size,
                      (ub4)OCI_ATTR_TYPE_NAME, (OCIError *)db->errhp));

    /* if typecode == OCI_TYPECODE_OBJECT, you can proceed to describe it
       recursively by calling describe_type() with its name; or you can
       obtain its OCIRef by using OCI_ATTR_REF_TDO, and then describing the
       type by REF                                                          */

                                           /* print values for the argument */
    printf ("%8s%5d%5d%9d%4d%3c%7d%8s%14s\n", arg_name, position,
                   typecode, datatype, level, has_default ? 'y' : 'n',
                   iomode, schema_name, type_name);
  }
}
