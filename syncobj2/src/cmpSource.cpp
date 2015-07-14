/*
 * cmpSource.cpp
 *
 *  Created on: 2013-4-26
 *      Author: huangbin
 */

#include "comm.h"

extern login login_info1;
extern login login_info2;

static int create_source_byselect(char *souname, char *ddl_sql, DATABASE *db, int soutype){
	char sqlbuf[1024]={0};
	int ret;
	char buftext[40000];

	int pos=0,cpos= 0;
	OCIDefine *defcolp[2];
	sword indp[2];

	char soutype_name[32]={0};
	if(soutype==7){
		strcpy(soutype_name, "PROCEDURE");
	}else if(soutype==8){
		strcpy(soutype_name, "FUNCTION");
	}else if(soutype==9){
		strcpy(soutype_name, "PACKAGE");
	}else if(soutype==11){
		strcpy(soutype_name, "PACKAGE BODY");
	}else if(soutype==12){
		strcpy(soutype_name, "TRIGGER");
	}else if(soutype==13){
		strcpy(soutype_name, "TYPE");
	}else if(soutype==14){
		strcpy(soutype_name, "TYPE BODY");
	}else{
		strcpy(soutype_name, "UNKNOWN");
	}

	sprintf(sqlbuf,"SELECT TEXT FROM ALL_SOURCE WHERE OWNER='%s' AND TYPE='%s' AND NAME='%s' order by line",
			login_info1.schema, soutype_name, souname);
	msg("sqlbuf=%s\n", sqlbuf);
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
			(ub4) 1, (dvoid *) buftext, (sb4) 4000,
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
		return ret;
	}
	pos= 0;
	cpos= 0;

	pos= pos + sprintf(ddl_sql+pos, "CREATE OR REPLACE ");


	for (;;) {
		ret = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (ret == OCI_NO_DATA) {
//			msg("NO DATA\n");
			break;
		} else {
			msg("%s", buftext);
			char bepart[4000]={0};

			if(cpos==0){
				//todo buftext="TRIGGER Test_Increase" souname="TEST_INCREASE"
				char *p= strstr(buftext, souname);
				char tmp[4000]={0};
				if(p){
					strncpy(tmp, buftext, p-buftext);
					char *pa= strchr(p, ' ');
					p= strchr(tmp, '.');
					if(p && pa){
						p= strrchr(tmp, ' ');
						strncpy(bepart, tmp, p-tmp);
						pos= pos + sprintf(ddl_sql+pos, "%s \"%s\" %s", bepart, souname, pa+1);
					}else if(p){
						p= strrchr(tmp, ' ');
						strncpy(bepart, tmp, p-tmp);
						pos= pos + sprintf(ddl_sql+pos, "%s \"%s\" ", bepart, souname);
					}else{
						pos= pos + sprintf(ddl_sql+pos, "%s", buftext);
					}
				}else{
					pos= pos + sprintf(ddl_sql+pos, "%s", buftext);
				}
			}else{
				pos= pos + sprintf(ddl_sql+pos, "%s", buftext);
			}
		}
		memset(buftext, 0, sizeof(buftext));
		cpos++;
	}
//	pos= pos + sprintf(ddl_sql+pos, "/");

	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
	return ret;
}

void getTriggerBySelect(DATABASE *db, list<string> *putList, char *schema_name){
	char sqlbuf[1024]={0};
	int ret;
	char trigger_name[128] = { 0 };

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,
			"SELECT DISTINCT(name) FROM ALL_SOURCE WHERE OWNER='%s' AND TYPE='TRIGGER'", schema_name);
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
			(dvoid *) trigger_name,//OCIDefineByPos primary_key,30长度，若不够30则填充空格(十进制32)
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
			msg("trigger_name=%s\n", trigger_name);
			if(strncmp(trigger_name, "BIN$", 4)!=0)
				putList->push_back(trigger_name);
		}
		memset(trigger_name, 0, sizeof(trigger_name));
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

void getTypeBySelect(DATABASE *db, list<string> *putList, char *schema_name){
	char sqlbuf[1024]={0};
	int ret;
	char trigger_name[128] = { 0 };

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,
			"SELECT DISTINCT(name) FROM ALL_SOURCE WHERE OWNER='%s' AND TYPE='TYPE'", schema_name);
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
			(dvoid *) trigger_name,//OCIDefineByPos primary_key,30长度，若不够30则填充空格(十进制32)
			(sb4) 128, SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getTypeBySelect:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute primary_key:%s\n", db->errmsg);
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
			msg("type_name=%s\n", trigger_name);
			putList->push_back(trigger_name);
		}
		memset(trigger_name, 0, sizeof(trigger_name));
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

void getPackBodyBySelect(DATABASE *db, list<string> *putList, char *schema_name){
	char sqlbuf[1024]={0};
	int ret;
	char trigger_name[128] = { 0 };

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,
			"SELECT DISTINCT name FROM ALL_SOURCE WHERE OWNER='%s' AND TYPE='PACKAGE BODY'", schema_name);
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
			(dvoid *) trigger_name,//OCIDefineByPos primary_key,30长度，若不够30则填充空格(十进制32)
			(sb4) 128, SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getPackBodyBySelect:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute getPackBodyBySelect:%s\n", db->errmsg);
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
			msg("packagebody_name=%s\n", trigger_name);
			putList->push_back(trigger_name);
		}
		memset(trigger_name, 0, sizeof(trigger_name));
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

void getPackBySelect(DATABASE *db, list<string> *putList, char *schema_name){
	char sqlbuf[1024]={0};
	int ret;
	char trigger_name[128] = { 0 };

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,
			"SELECT DISTINCT name FROM ALL_SOURCE WHERE OWNER='%s' AND TYPE='PACKAGE'", schema_name);
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
			(dvoid *) trigger_name,//OCIDefineByPos primary_key,30长度，若不够30则填充空格(十进制32)
			(sb4) 128, SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getPackBySelect:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute getPackBySelect:%s\n", db->errmsg);
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
			msg("package_name=%s\n", trigger_name);
			putList->push_back(trigger_name);
		}
		memset(trigger_name, 0, sizeof(trigger_name));
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

void getFuncBySelect(DATABASE *db, list<string> *putList, char *schema_name){
	char sqlbuf[1024]={0};
	int ret;
	char trigger_name[128] = { 0 };

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,
			"SELECT DISTINCT name FROM ALL_SOURCE WHERE OWNER='%s' AND TYPE='FUNCTION'", schema_name);
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
			(dvoid *) trigger_name,//OCIDefineByPos primary_key,30长度，若不够30则填充空格(十进制32)
			(sb4) 128, SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getFunctionBySelect:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute getFunctionBySelect:%s\n", db->errmsg);
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
			msg("function_name=%s\n", trigger_name);
			putList->push_back(trigger_name);
		}
		memset(trigger_name, 0, sizeof(trigger_name));
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

void getProcedureBySelect(DATABASE *db, list<string> *putList, char *schema_name){
	char sqlbuf[1024]={0};
	int ret;
	char trigger_name[128] = { 0 };

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,
			"SELECT DISTINCT name FROM ALL_SOURCE WHERE OWNER='%s' AND TYPE='PROCEDURE'", schema_name);
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
			(dvoid *) trigger_name,//OCIDefineByPos primary_key,30长度，若不够30则填充空格(十进制32)
			(sb4) 128, SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getProcedureBySelect:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute getProcedureBySelect:%s\n", db->errmsg);
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
			msg("procedure_name=%s\n", trigger_name);
			putList->push_back(trigger_name);
		}
		memset(trigger_name, 0, sizeof(trigger_name));
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

void getTypeBodyBySelect(DATABASE *db, list<string> *putList, char *schema_name){
	char sqlbuf[1024]={0};
	int ret;
	char trigger_name[128] = { 0 };

	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sqlbuf,
			"SELECT DISTINCT name FROM ALL_SOURCE WHERE OWNER='%s' AND TYPE='TYPE BODY'", schema_name);
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
			(dvoid *) trigger_name,//OCIDefineByPos primary_key,30长度，若不够30则填充空格(十进制32)
			(sb4) 128, SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0,
			(ub2 *) 0, (ub4) OCI_DEFAULT);
	if (ret != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos getTypeBodyBySelect:%s\n", db->errmsg);
		ret = -2;
	}
//	ret = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	ret = executeQuery8(db, db->stmt);
	if (ret != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute getTypeBodyBySelect:%s\n", db->errmsg);
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
			msg("packagebody_name=%s\n", trigger_name);
			putList->push_back(trigger_name);
		}
		memset(trigger_name, 0, sizeof(trigger_name));
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

void compare_source(list<string> *src_source_def, list<string> *dst_source_def,
		int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst, int source_type){
	char ddl_source_file[256]={0};
	char ddl_file[256]={0};

	list<string>::iterator it_src_def;
	list<string>::iterator it_dst_def;
	bool find= false;
	char ddl_sql[1024000];
	char src_source[256];
	char dst_source[256];

//SOURCE_PROCEDURE 7
//SOURCE_FUNCTION 8
//SOURCE_PACKAGE 9
//SOURCE_PACKAGE_BODY 11
//SOURCE_TRIGGER 12
//SOURCE_TYPE 13
//SOURCE_TYPE_BODY 14
	switch(source_type){
	case SOURCE_PROCEDURE:
		src_source_def->clear();
		dst_source_def->clear();
		sprintf(ddl_source_file, "%s/dif_procedure", ddl_path);
		sprintf(ddl_file, "%s/ddl_procedure.sql", ddl_path);
		getProcedureBySelect(db, src_source_def, login_info1.schema);
		if(do_level!=100){
			getProcedureBySelect(dbdst, dst_source_def, login_info2.schema);
		}
		break;
	case SOURCE_FUNCTION:
		src_source_def->clear();
		dst_source_def->clear();
		sprintf(ddl_source_file, "%s/dif_function", ddl_path);
		sprintf(ddl_file, "%s/ddl_function.sql", ddl_path);
		getFuncBySelect(db, src_source_def, login_info1.schema);
		if(do_level!=100){
		getFuncBySelect(dbdst, dst_source_def, login_info2.schema);
		}
		break;
	case SOURCE_PACKAGE:
		src_source_def->clear();
		dst_source_def->clear();
		sprintf(ddl_source_file, "%s/dif_package", ddl_path);
		sprintf(ddl_file, "%s/ddl_package.sql", ddl_path);
		getPackBySelect(db, src_source_def, login_info1.schema);
		if(do_level!=100){
		getPackBySelect(dbdst, dst_source_def, login_info2.schema);
		}
		break;
	case SOURCE_PACKAGE_BODY:
		sprintf(ddl_source_file, "%s/dif_packagebody", ddl_path);
		sprintf(ddl_file, "%s/ddl_packagebody.sql", ddl_path);
		getPackBodyBySelect(db, src_source_def, login_info1.schema);
		if(do_level!=100){
		getPackBodyBySelect(dbdst, dst_source_def, login_info2.schema);
		}
		break;
	case SOURCE_TRIGGER:
		sprintf(ddl_source_file, "%s/dif_trigger", ddl_path);
		sprintf(ddl_file, "%s/ddl_trigger.sql", ddl_path);
		getTriggerBySelect(db, src_source_def, login_info1.schema);
		if(do_level!=100){
		getTriggerBySelect(dbdst, dst_source_def, login_info2.schema);
		}
		break;
	case SOURCE_TYPE:
		sprintf(ddl_source_file, "%s/dif_type", ddl_path);
		sprintf(ddl_file, "%s/ddl_type.sql", ddl_path);
		getTypeBySelect(db, src_source_def, login_info1.schema);
		if(do_level!=100){
		getTypeBySelect(dbdst, dst_source_def, login_info2.schema);
		}
		break;
	case SOURCE_TYPE_BODY:
		sprintf(ddl_source_file, "%s/dif_typebody", ddl_path);
		sprintf(ddl_file, "%s/ddl_typebody.sql", ddl_path);
		getTypeBodyBySelect(db, src_source_def, login_info1.schema);
		if(do_level!=100){
		getTypeBodyBySelect(dbdst, dst_source_def, login_info2.schema);
		}
		break;
	default:
		break;
	}


	if(access(ddl_source_file, F_OK)==0){
		remove(ddl_source_file);
	}
	if(access(ddl_file, F_OK)==0){
		remove(ddl_file);
	}

//	if(source_type==SOURCE_TRIGGER){
//		getTriggerBySelect(db, src_source_def, login_info1.schema);
//		getTriggerBySelect(dbdst, dst_source_def, login_info2.schema);
//	}else if(source_type==SOURCE_TYPE){
//		getTypeBySelect(db, src_source_def, login_info1.schema);
//		getTypeBySelect(dbdst, dst_source_def, login_info2.schema);
//	}else if(source_type==SOURCE_PACKAGE_BODY){
//		getPackBodyBySelect(db, src_source_def, login_info1.schema);
//		getPackBodyBySelect(dbdst, dst_source_def, login_info2.schema);
//	}

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
//			dif_seq.push_back(src_view);
			write_dif_obj(ddl_source_file, src_source);
			memset(ddl_sql, 0, sizeof(ddl_sql));
			create_source_byselect(src_source, ddl_sql, db, source_type);
			write_ddl(ddl_file, ddl_sql);
			if(do_level==1){
				msg("%s\n", ddl_sql);
				alter_schema(login_info2.schema, dbdst);
				prepareExecute8(dbdst, &dbdst->stmt, (text *)ddl_sql);
			}
		}
	}

}
