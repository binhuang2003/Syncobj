/*
 * cmpSeq.cpp
 *
 *  Created on: 2013-4-26
 *      Author: huangbin
 */

#include "comm.h"

extern login login_info1;
extern login login_info2;

typedef struct _SEQCACHE{
	char seqname[128];
	sb4 cache;
}SEQCACHE;

list<SEQCACHE> seqcachelist;

static void get_cache(char *seqname, int *cache){
	list<SEQCACHE>::iterator it;
	SEQCACHE seqcache;
	for(it=seqcachelist.begin(); it!=seqcachelist.end(); it++){
		seqcache= (*it);
		if(strcmp(seqcache.seqname, seqname)==0){
			*cache= seqcache.cache;
			break;
		}
	}
}

static int alter_nextval(char *seqname, sb4 increment, DATABASE *db, char *schema){
	char sqlbuf[128]={0};
	sb4 value= 0;
	sword ret= 0;

	OCIDefine *defcolp[2];
	sword indp[2];
	int cache= 0;
	get_cache(seqname, &cache);
	if(cache!=0){
		sprintf(sqlbuf, "alter sequence %s.%s increment by %d cache %d", schema, seqname, increment+cache, cache);
	}else{
		sprintf(sqlbuf, "alter sequence %s.%s increment by %d nocache", schema, seqname, increment);
	}
	msg("sqlbuf=%s\n", sqlbuf);
	prepareExecute8(db, &db->stmt, (text *)sqlbuf);

	memset(sqlbuf, 0, sizeof(sqlbuf));
	sprintf(sqlbuf, "select %s.%s.nextval from dual", schema, seqname);

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
			(ub4) 1, (dvoid *) &value, (sb4) sizeof(int),
			SQLT_INT, (dvoid *) &indp[0], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 1,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeUpdate8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute 333:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
	msg("%s.%s.nextval=%d\n", schema, seqname, value);

	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);

	memset(sqlbuf, 0, sizeof(sqlbuf));
	if(cache!=0){
		sprintf(sqlbuf, "alter sequence %s.%s increment by 1 cache %d", schema, seqname, cache);
	}else{
		sprintf(sqlbuf, "alter sequence %s.%s increment by 1 nocache", schema, seqname);
	}

	prepareExecute8(db, &db->stmt, (text *)sqlbuf);

	return ret;
}

static int select_nextval(char *seqname, sb4 *nextval, DATABASE *db, char *schema){
	char sqlbuf[128]={0};
	sb4 value= 0;
	sword ret;

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf, "select %s.%s.nextval from dual", schema, seqname);

//	msg("sqlbuf=%s\n", sqlbuf);
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
			(ub4) 1, (dvoid *) &value, (sb4) sizeof(int),
			SQLT_INT, (dvoid *) &indp[0], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 1,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeUpdate8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute 333:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}

	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
	msg("%s.%s.nextval=%d\n", schema, seqname, value);
	*nextval= value;
	return ret;
}

static int create_seq_byselect(char *seqname, char *ddl_sql, DATABASE *db, sb4 next_val){
	char sqlbuf[1024]={0};
	int ret;
	sb4 min_value= 0;
	char max_value[30]= {0};
	sb4 incr= 0;
	char cycle[3]={0};
	char order[3]={0};
	sb4 cache_size= 0;
	sb4 last_num= 0;

	int pos=0,cpos= 0;
	OCIDefine *defcolp[10];
	sword indp[10];

	sprintf(sqlbuf,"SELECT MIN_VALUE,MAX_VALUE,INCREMENT_BY,CYCLE_FLAG,ORDER_FLAG,CACHE_SIZE,LAST_NUMBER FROM ALL_SEQUENCES\n\
			 WHERE SEQUENCE_OWNER='%s' AND SEQUENCE_NAME='%s'", login_info1.schema, seqname);
//	printf("sqlbuf=%s\n", sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2:%s\n", db->errmsg);
		ret = -2;
		return ret;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 1, (dvoid *) &min_value, (sb4) sizeof(int),
			SQLT_INT, (dvoid *) &indp[0], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[2], db->errhp,
			(ub4) 2, (dvoid *) &max_value, (sb4) 30, SQLT_STR,
			(dvoid *) &indp[1], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[3], db->errhp,
			(ub4) 3, (dvoid *) &incr, (sb4) sizeof(int), SQLT_INT,
			(dvoid *) &indp[2], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[4], db->errhp,
			(ub4) 4, (dvoid *) cycle, (sb4) 1,
			SQLT_CHR, (dvoid *) &indp[3], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[5], db->errhp,
			(ub4) 5, (dvoid *) order, (sb4) 1,
			SQLT_CHR, (dvoid *) &indp[4], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[6], db->errhp,
			(ub4) 6, (dvoid *) &cache_size, (sb4) sizeof(int),
			SQLT_INT, (dvoid *) &indp[5], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
	ret = OCIDefineByPos(db->stmt, &defcolp[7], db->errhp,
			(ub4) 7, (dvoid *) &last_num, (sb4) sizeof(int),
			SQLT_INT, (dvoid *) &indp[6], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute 333:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return ret;
	}
	pos= 0;
	cpos= 0;

	pos= pos + sprintf(ddl_sql+pos, "create sequence %s.\"%s\" \n", login_info2.schema, seqname);

	ret = OCIStmtFetch(db->stmt, db->errhp, 1,
			OCI_FETCH_NEXT, OCI_DEFAULT);
	if (ret == OCI_NO_DATA) {
		msg("NO DATA\n");
	} else {
		msg("min_value=%d, max_value=%s, incr=%d, cycle=%c, order=%c, cache_size=%d, last_num=%d\n",
				min_value, max_value, incr, cycle[0], order[0], cache_size, last_num);
//		if(cache_size==0)
//			cache_size=20;
		pos= pos + sprintf(ddl_sql+pos, "minvalue %d\n", min_value);
		pos= pos + sprintf(ddl_sql+pos, "maxvalue %s\n", max_value);
		pos= pos + sprintf(ddl_sql+pos, "increment by %d\n", incr);
		if(next_val==0){
			pos= pos + sprintf(ddl_sql+pos, "start with %d\n", last_num);
		}else{
			pos= pos + sprintf(ddl_sql+pos, "start with %d\n", next_val);
		}
		if(cache_size==0){
			pos= pos + sprintf(ddl_sql+pos, "nocache\n");
		}else{
			pos= pos + sprintf(ddl_sql+pos, "cache %d\n", cache_size);
		}
		if(cycle[0]=='Y'){
			pos= pos + sprintf(ddl_sql+pos, "cycle\n");
		}else{
			pos= pos + sprintf(ddl_sql+pos, "nocycle\n");
		}
		if(order[0]=='Y'){
			pos= pos + sprintf(ddl_sql+pos, "order\n");
		}else{
			pos= pos + sprintf(ddl_sql+pos, "noorder\n");
		}
		SEQCACHE seqcache;
		seqcache.cache= cache_size;
		memset(seqcache.seqname, 0, sizeof(seqcache.seqname));
		strcpy(seqcache.seqname, seqname);
		seqcachelist.push_back(seqcache);

	}

	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
	return ret;
}

void getSeqBySelect(DATABASE *db, list<string> *putList, char *schema_name){
	char sqlbuf[1024]={0};
	int ret;
	char trigger_name[128] = { 0 };

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,
			"SELECT SEQUENCE_NAME FROM ALL_SEQUENCES WHERE SEQUENCE_OWNER='%s'", schema_name);
	msg("sqlbuf=%s\n",sqlbuf);
	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sqlbuf, strlen(sqlbuf), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2:%s\n", db->errmsg);
		return;
	}

	ret = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1,
			(dvoid *) trigger_name,
			(sb4) 128, SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getSeqBySelect:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute getSeqBySelect:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			msg("sequence_name=%s\n", trigger_name);
			putList->push_back(trigger_name);
		}
		memset(trigger_name, 0, sizeof(trigger_name));
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

void compare_seq(list<string> *src_seq_def, list<string> *dst_seq_def,
		int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst){
	char ddl_seq_file[256]={0};
	char ddl_file[256]={0};

	list<string>::iterator it_src_def;
	list<string>::iterator it_dst_def;
	bool find= false;
	char ddl_sql[1024];
	char src_seq[256];
	char dst_seq[256];
	sb4 src_nextval= 0;
	sb4 dst_nextval= 0;
	sb4 increment= 0;

	sprintf(ddl_seq_file, "%s/dif_sequence", ddl_path);
	sprintf(ddl_file, "%s/ddl_sequence.sql", ddl_path);

	if(access(ddl_seq_file, F_OK)==0){
		remove(ddl_seq_file);
	}
	if(access(ddl_file, F_OK)==0){
		remove(ddl_file);
	}

	src_seq_def->clear();
	dst_seq_def->clear();

	getSeqBySelect(db, src_seq_def, login_info1.schema);
	if(do_level!=100){
	getSeqBySelect(dbdst, dst_seq_def, login_info2.schema);
	}
	seqcachelist.clear();

	for(it_src_def=src_seq_def->begin(); it_src_def!=src_seq_def->end(); it_src_def++){
		memset(src_seq, 0, sizeof(src_seq));
		strcpy(src_seq, (*it_src_def).c_str());
		find= false;
		for(it_dst_def=dst_seq_def->begin(); it_dst_def!=dst_seq_def->end(); it_dst_def++){
			memset(dst_seq, 0, sizeof(dst_seq));
			strcpy(dst_seq, (*it_dst_def).c_str());
			if(strcasecmp(src_seq, dst_seq)==0){
				find= true;
				break;
			}
		}
		memset(ddl_sql, 0, sizeof(ddl_sql));
		create_seq_byselect(src_seq, ddl_sql, db, 0);
		if(!find){
//			dif_seq.push_back(src_view);
			write_dif_obj(ddl_seq_file, src_seq);
			write_ddl(ddl_file, ddl_sql);
			if(strlen(ddl_sql)>0 && do_level==1){
				prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
			}
		}else{
			src_nextval= 0;
			dst_nextval= 0;
			select_nextval(src_seq, &src_nextval, db, login_info1.schema);
			select_nextval(src_seq, &dst_nextval, dbdst, login_info2.schema);
			if(src_nextval>dst_nextval && do_level==1){
				increment= src_nextval-dst_nextval;
				alter_nextval(src_seq, increment, dbdst, login_info2.schema);
			}
		}
	}

}

void compare_fun2seq(list<string> *src_seq_def, list<string> *dst_seq_def,
		int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst){
	char ddl_seq_file[256]={0};
	char ddl_file[256]={0};

	list<string>::iterator it_src_def;
	list<string>::iterator it_dst_def;
	bool find= false;
	char ddl_sql[1024];
	char src_seq[256];
	char dst_seq[256];
	sb4 src_nextval= 0;
	sb4 dst_nextval= 0;
	sb4 increment= 0;

	sprintf(ddl_seq_file, "%s/dif_sequence", ddl_path);
	sprintf(ddl_file, "%s/ddl_sequence.sql", ddl_path);

	if(access(ddl_seq_file, F_OK)==0){
		remove(ddl_seq_file);
	}
	if(access(ddl_file, F_OK)==0){
		remove(ddl_file);
	}

	src_seq_def->clear();
	dst_seq_def->clear();

	getSeqBySelect(db, src_seq_def, login_info1.schema);
	if(do_level!=100){
	getSeqBySelect(dbdst, dst_seq_def, login_info2.schema);
	}
	seqcachelist.clear();

	for(it_src_def=src_seq_def->begin(); it_src_def!=src_seq_def->end(); it_src_def++){
		memset(src_seq, 0, sizeof(src_seq));
		strcpy(src_seq, (*it_src_def).c_str());
		find= false;
		for(it_dst_def=dst_seq_def->begin(); it_dst_def!=dst_seq_def->end(); it_dst_def++){
			memset(dst_seq, 0, sizeof(dst_seq));
			strcpy(dst_seq, (*it_dst_def).c_str());
			if(strcasecmp(src_seq, dst_seq)==0){
				find= true;
				break;
			}
		}
		memset(ddl_sql, 0, sizeof(ddl_sql));
		src_nextval= 0;
		dst_nextval= 0;
		select_nextval(src_seq, &src_nextval, db, login_info1.schema);
		create_seq_byselect(src_seq, ddl_sql, db, src_nextval);
		if(!find){
//			dif_seq.push_back(src_view);
			write_dif_obj(ddl_seq_file, src_seq);
			write_ddl(ddl_file, ddl_sql);
			if(strlen(ddl_sql)>0 && do_level==1){
				prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
			}
		}else{
			//drop dest sequence
			select_nextval(src_seq, &dst_nextval, dbdst, login_info2.schema);
			if(src_nextval>dst_nextval && do_level==1){
				increment= src_nextval-dst_nextval;
				alter_nextval(src_seq, increment, dbdst, login_info2.schema);
			}
		}

	}

}

