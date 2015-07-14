/*
 * cmpDir.cpp
 *
 *  Created on: 2013-5-14
 *      Author: huangbin
 */

#include "comm.h"

extern login login_info1;
extern login login_info2;

void getDirBySelect(DATABASE *db, list<string> *putList, char *schema_name){
	char sqlbuf[1024]={0};
	int ret;
	char dir_name[128] = { 0 };

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,
			"SELECT DIRECTORY_NAME FROM ALL_DIRECTORIES");
	msg("sqlbuf=%s\n",sqlbuf);
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
			(dvoid *) dir_name,//OCIDefineByPos primary_key,30长度，若不够30则填充空格(十进制32)
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
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			msg("trigger_name=%s\n", dir_name);
			putList->push_back(dir_name);
		}
		memset(dir_name, 0, sizeof(dir_name));
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

void create_dir_byselect(char *src_source, char *ddl_sql, DATABASE *db){
	char sqlbuf[1024]={0};
	int ret;
	char buftext[256];

	int pos=0;
	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,"SELECT DIRECTORY_PATH FROM all_directories WHERE directory_name='%s'",
			src_source);
	msg("sqlbuf=%s\n", sqlbuf);
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
			(ub4) 1, (dvoid *) buftext, (sb4) sizeof(buftext),
			SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);

//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute 333:%s\n", db->errmsg);
		ret = -2;
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
	ret = OCIStmtFetch(db->stmt, db->errhp, 1,
			OCI_FETCH_NEXT, OCI_DEFAULT);

	if (ret != OCI_NO_DATA) {
		pos= 0;

		pos= pos + sprintf(ddl_sql+pos, "CREATE OR REPLACE DIRECTORY %s AS \n", src_source);
		pos= pos + sprintf(ddl_sql+pos, "'%s'\n", buftext);
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

void compare_dir(list<string> *src_source_def, list<string> *dst_source_def,
		int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst){
	char ddl_source_file[256]={0};
	char ddl_file[256]={0};

	list<string>::iterator it_src_def;
	list<string>::iterator it_dst_def;
	bool find= false;
	char ddl_sql[1024];
	char src_source[256];
	char dst_source[256];

	sprintf(ddl_source_file, "%s/dif_dir", ddl_path);
	sprintf(ddl_file, "%s/ddl_dir.sql", ddl_path);
	getDirBySelect(db, src_source_def, login_info1.schema);
	getDirBySelect(dbdst, dst_source_def, login_info2.schema);

	if(access(ddl_source_file, F_OK)==0){
		remove(ddl_source_file);
	}
	if(access(ddl_file, F_OK)==0){
		remove(ddl_file);
	}

	for(it_src_def=src_source_def->begin(); it_src_def!=src_source_def->end(); it_src_def++){
		memset(src_source, 0, sizeof(src_source));
		strcpy(src_source, (*it_src_def).c_str());
		find= false;
		for(it_dst_def=dst_source_def->begin(); it_dst_def!=dst_source_def->end(); it_dst_def++){
			memset(dst_source, 0, sizeof(dst_source));
			strcpy(dst_source, (*it_dst_def).c_str());
			if(strcmp(src_source, dst_source)==0){
				find= true;
				break;
			}
		}
		if(!find){
			write_dif_obj(ddl_source_file, src_source);
			memset(ddl_sql, 0, sizeof(ddl_sql));
			create_dir_byselect(src_source, ddl_sql, db);
			write_ddl(ddl_file, ddl_sql);
			if(do_level==1 && strlen(ddl_sql)>0){
				printf("%s\n", ddl_sql);
				prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
			}
		}
	}
}

void getJobBySelect(DATABASE *db, list<DEFJOB*> *putList, char *schema_name){
	char sqlbuf[1024]={0};
	int ret;
	int job= 0;
	char next_date[20]={0};
	char next_sec[20]={0};
	char broken[2]={0};
	char interval[200]={0};
	char what[4000]={0};
	char nls_env[2000]={0};
	char misc_env[256]={0};

	OCIDefine *defcolp[9];
	sword indp[9];

	sprintf(sqlbuf,
			"select JOB,to_char(NEXT_DATE, 'dd-mm-yyyy'),NEXT_SEC,BROKEN,INTERVAL,WHAT,NLS_ENV,MISC_ENV FROM DBA_JOBS WHERE SCHEMA_USER='%s'", schema_name);
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
			(dvoid *) &job,
			(sb4) sizeof(int), SQLT_INT, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getJobBySelect:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[1], db->errhp,
			(ub4) 2,
			(dvoid *) next_date,
			(sb4) sizeof(next_date), SQLT_STR, (dvoid *) &indp[1], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getJobBySelect:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[2], db->errhp,
			(ub4) 3,
			(dvoid *) next_sec,
			(sb4) sizeof(next_sec), SQLT_STR, (dvoid *) &indp[2], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getJobBySelect:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[3], db->errhp,
			(ub4) 4,
			(dvoid *) broken,
			(sb4) sizeof(broken), SQLT_STR, (dvoid *) &indp[3], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getJobBySelect:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[4], db->errhp,
			(ub4) 5,
			(dvoid *) interval,
			(sb4) sizeof(interval), SQLT_STR, (dvoid *) &indp[4], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getJobBySelect:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[5], db->errhp,
			(ub4) 6,
			(dvoid *) what,
			(sb4) sizeof(what), SQLT_STR, (dvoid *) &indp[5], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getJobBySelect:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[6], db->errhp,
			(ub4) 7,
			(dvoid *) nls_env,
			(sb4) sizeof(nls_env), SQLT_STR, (dvoid *) &indp[6], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getJobBySelect:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
	ret = OCIDefineByPos(db->stmt, &defcolp[7], db->errhp,
			(ub4) 8,
			(dvoid *) misc_env,
			(sb4) sizeof(misc_env), SQLT_STR, (dvoid *) &indp[7], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getJobBySelect:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute getJobBySelect:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}
	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
			break;
		} else {
			msg("job=%d, next_date=%s, next_sec=%s, broken=%s, interval=%s, what=%s\nnls_env=%s\nmisc_env=%s\n",
					job, next_date, next_sec, broken, interval, what, nls_env, misc_env);
			DEFJOB *node= new DEFJOB;
			node->job= job;
			memset(node->next_date, 0, sizeof(node->next_date));
			strcpy(node->next_date, next_date);
			memset(node->next_sec, 0, sizeof(node->next_sec));
			strcpy(node->next_sec, next_sec);
			memset(node->broken, 0, sizeof(node->broken));
			strcpy(node->broken, broken);
			memset(node->interval, 0, sizeof(node->interval));
			strcpy(node->interval, interval);
			memset(node->what, 0, sizeof(node->what));
			strcpy(node->what, what);
			memset(node->nls_env, 0, sizeof(node->nls_env));
			strcpy(node->nls_env, nls_env);
			memset(node->misc_env, 0, sizeof(node->misc_env));
			strcpy(node->misc_env, misc_env);
			putList->push_back(node);
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

int create_job(char *ddl_sql, DEFJOB *defjob, DATABASE *db, int *dst_job){

	int pos= 0;
	int ret= 0;
	OCIDefine *defcolp[2];
	sword indp[2];
	OCIBind *bndhp[2];
	int job= 0;

	msg("job=%s\n", ddl_sql);

	ret = OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) ddl_sql, strlen(ddl_sql), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2:%s\n", db->errmsg);
		return -1;
	}

//	ret= OCIBindByName(db->stmt, &(bndhp[0]),
//			db->errhp, (text *) "job1",
//			(sb4) strlen("job1"),
//			(dvoid *) &job,
//			(sb4) sizeof(int), SQLT_INT, (dvoid *) 0, (ub2 *) 0,
//			(ub2 *) 0, (ub4) 0, (ub4 *) 0,
//			(ub4) OCI_DEFAULT);
//	if (ret != OCI_SUCCESS) {
//		oraError8(db);
//		msg("error OCIDefineByPos create_job:%s\n", db->errmsg);
//		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
//		return -1;
//	}

	ret = executeUpdate8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute create_job:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return -1;
	}
	msg("job=%d\n", job);
	*dst_job= job;
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
	return 0;
}

void add_commer(char *dst, char *src){
	int i,j;
	int len= strlen(src);
	for(i=0,j=0; i<len; i++,j++){
		dst[j]=src[i];
		if(src[i]=='\''){
			dst[++j]= src[i];
		}
	}
}

void compare_job(int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst){
	char ddl_source_file[256]={0};
	char ddl_file[256]={0};
	list<DEFJOB*> src_source_def;
	list<DEFJOB*> dst_source_def;

	list<DEFJOB*>::iterator it_src_def;
	list<DEFJOB*>::iterator it_dst_def;
	bool find= false;
	char ddl_sql[1024];
	char src_source[4000];
	char dst_source[4000];
	DEFJOB *node1, *node2;
	int pos;

	sprintf(ddl_source_file, "%s/dif_job", ddl_path);
	sprintf(ddl_file, "%s/ddl_job.sql", ddl_path);

	getJobBySelect(db, &src_source_def, login_info1.schema);
	getJobBySelect(dbdst, &dst_source_def, login_info2.schema);

	for(it_src_def=src_source_def.begin(); it_src_def!=src_source_def.end(); it_src_def++){
		node1= (DEFJOB *)(*it_src_def);
		memset(src_source, 0, sizeof(src_source));
		strcpy(src_source, node1->what);
		find= false;
		for(it_dst_def=dst_source_def.begin(); it_dst_def!=dst_source_def.end(); it_dst_def++){
			node2= (DEFJOB *)(*it_dst_def);
			memset(dst_source, 0, sizeof(dst_source));
			strcpy(dst_source, node2->what);
			if(node1->job==node2->job){
				find= true;
				break;
			}
		}
		if(!find){
			memset(ddl_sql, 0, sizeof(ddl_sql));
			char *nls_env= NULL;
			if(strlen(node1->nls_env)==0)
				continue;
			nls_env= (char *)malloc(strlen(node1->nls_env)+20);
			if(!nls_env)
				continue;
			add_commer(nls_env, node1->nls_env);

			pos= 0;
			pos= pos + sprintf(ddl_sql+pos, "begin\n");
//			pos= pos + sprintf(ddl_sql+pos, "sys.dbms_job.isubmit(%d,\n", node1->job);
//			pos= pos + sprintf(ddl_sql+pos, "'%s',\n", node1->what);
//			pos= pos + sprintf(ddl_sql+pos, "to_date('01-01-4000 00:00:00', 'dd-mm-yyyy hh24:mi:ss'),\n");
//			pos= pos + sprintf(ddl_sql+pos, "'%s');\n", node1->interval);
			pos= pos + sprintf(ddl_sql+pos, "sys.dbms_ijob.submit(%d,\n", node1->job);
			pos= pos + sprintf(ddl_sql+pos, "'%s',", login_info2.schema);
			pos= pos + sprintf(ddl_sql+pos, "'%s',", login_info2.schema);
			pos= pos + sprintf(ddl_sql+pos, "'%s',", login_info2.schema);
			pos= pos + sprintf(ddl_sql+pos, "to_date('01-01-4000 00:00:00', 'dd-mm-yyyy hh24:mi:ss'),\n");
			pos= pos + sprintf(ddl_sql+pos, "'%s',\n", node1->interval);
			pos= pos + sprintf(ddl_sql+pos, "FALSE,\n");
			pos= pos + sprintf(ddl_sql+pos, "'%s',\n", node1->what);
			pos= pos + sprintf(ddl_sql+pos, "'%s',\n", nls_env);
			pos= pos + sprintf(ddl_sql+pos, "'%s');\n", node1->misc_env);
			pos= pos + sprintf(ddl_sql+pos, "commit;\n");
			pos= pos + sprintf(ddl_sql+pos, "end;");
			write_ddl(ddl_file, ddl_sql);
			if(do_level==1){
				alter_schema(login_info2.schema, dbdst);
				int dst_job= 0;
				create_job(ddl_sql, node1, dbdst, &dst_job);
				if(node1->broken[0]=='Y'){
					memset(ddl_sql, 0, sizeof(ddl_sql));
					pos= 0;
					pos= pos + sprintf(ddl_sql+pos, "begin\n");
					pos= pos + sprintf(ddl_sql+pos, "sys.dbms_ijob.broken(%d,true);\n", node1->job);
					pos= pos + sprintf(ddl_sql+pos, "commit;\n");
					pos= pos + sprintf(ddl_sql+pos, "end;");
					msg("ddl_sql=%s\n", ddl_sql);
					prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
				}
			}
			if(nls_env)
				free(nls_env);
		}
	}
}
