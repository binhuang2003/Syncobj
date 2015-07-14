/*
 * comm.cpp
 *
 *  Created on: 2013-4-22
 *      Author: huangbin
 */

#include "comm.h"

char err_file[256];
char msg_file[256];

char *getcurtime() {
	static char localInfo[100]={0};
	struct timeval t;
	struct tm *ptm;
	memset(localInfo,0,sizeof(localInfo));
	gettimeofday(&t, NULL);
	ptm = localtime(&t.tv_sec);
	sprintf(localInfo, "%04d-%02d-%02d %02d:%02d:%02d.%06ld", ptm->tm_year
			+ 1900, ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min,
			ptm->tm_sec, t.tv_usec);
	return localInfo;
}

void initDB(DATABASE *db) {
	db->envhp = NULL;
	db->errhp = NULL;
	db->svchp = NULL;
	db->seshp = NULL;
	db->srvhp = NULL;
	db->dschp = NULL;
	db->prefetch = 100;
	db->charset = 0;
	db->ncharset = 0;
	db->fp_log = stdout;
	db->context = NULL;
	db->msgfunc = NULL;
	memset(db->charsets, 0, sizeof(db->charsets));
	memset(db->ncharsets, 0, sizeof(db->ncharsets));
}

sb4 oraError8(DATABASE *db) {
	int len;
	db->errcode = 0;
	memset((void *) (db->errmsg), (int) '\0', (size_t) 512);
	OCIErrorGet((dvoid *) (db->errhp), (ub4) 1, (text *) NULL, &(db->errcode),
			db->errmsg, (ub4) sizeof(db->errmsg), (ub4) OCI_HTYPE_ERROR);
//	if (strcasecmp(db->charsets, "UTF-8") != 0) {
//		utf_flg = 1;
//		len = strlen((char*) db->errmsg);
//		columnNameConvert((char*) db->errmsg, &len, db->charsets);
//		utf_flg = 0;
//	}
	if (db->errcode) {
		if (db->msgfunc != NULL) {
			(*(db->msgfunc))(db->context, db->errmsg);
		}
	}
	return db->errcode;
}

sword prepareStmt8(DATABASE *db, OCIStmt **stmt, text *sql) {

	if(OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			sql, strlen((char *)sql), NULL, 0,
				OCI_NTV_SYNTAX, OCI_DEFAULT)){
		return oraError8(db);
	}

	return OCI_SUCCESS;
}

sword executeUpdate8(DATABASE *db, OCIStmt *stmt) {
	alarm(30);
	if (OCIStmtExecute(db->svchp, stmt, db->errhp, 1, 0, 0, 0, OCI_DEFAULT)) {
		alarm(0);
		return oraError8(db);
	}
	alarm(0);
	return OCI_SUCCESS;
}

sword releaseStmt8(DATABASE *db, OCIStmt *stmt) {
	if (OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT)) {
		return OCI_ERROR;
	}
	return OCI_SUCCESS;
}

int is_filter_errcode(int err_code){
	if(err_code==1408){
		return 1;
	}
	if(err_code==955){
		return 1;
	}
	if(err_code==2275){
		return 1;
	}
	if(err_code==942){
		return 1;
	}
	if(err_code==24344){
		return 1;
	}
	if(err_code==942){
		return 1;
	}
	return 0;
}

sword prepareExecute8(DATABASE *db, OCIStmt **stmt, text *sql) {
	sword rtncode = OCI_SUCCESS;
	if ((rtncode = prepareStmt8(db, stmt, sql)) == OCI_SUCCESS) {
		rtncode = executeUpdate8(db, *stmt);
		if(rtncode!=OCI_SUCCESS){
			oraError8(db);
			msg("%s\n", db->errmsg);
			if(!is_filter_errcode(db->errcode)){
				xlog_out("sql=%s\nerror=%s\n", (char *)sql, db->errmsg);
			}
		}
	}
	releaseStmt8(db, *stmt);
	return rtncode;
}

sword executeQuery8(DATABASE *db, OCIStmt *stmt) {
	alarm(30);
	if (OCIStmtExecute(db->svchp, stmt, db->errhp, 0, 0, 0, 0, OCI_DEFAULT)) {
		alarm(0);
		return oraError8(db);
	}
	alarm(0);
	return OCI_SUCCESS;
}

sword prepareQuery8(DATABASE *db, OCIStmt **stmt, text *sql) {
	if (prepareStmt8(db, stmt, sql) == OCI_SUCCESS) {
		return executeQuery8(db, *stmt);
	}
	return OCI_ERROR;
}

sword disconnectDB(DATABASE *db) {
	if (db->seshp)
		OCISessionEnd(db->svchp, db->errhp, db->seshp, OCI_DEFAULT);
	if (db->srvhp)
		OCIServerDetach(db->srvhp, db->errhp, OCI_DEFAULT);
	if (db->srvhp)
		OCIHandleFree((dvoid *) db->srvhp, (ub4) OCI_HTYPE_SERVER);
	if (db->svchp)
		OCIHandleFree((dvoid *) db->svchp, (ub4) OCI_HTYPE_SVCCTX);
	if (db->errhp)
		OCIHandleFree((dvoid *) db->errhp, (ub4) OCI_HTYPE_ERROR);
	if (db->seshp)
		OCIHandleFree((dvoid *) db->seshp, (ub4) OCI_HTYPE_SESSION);
	if (db->envhp)
		OCIHandleFree((dvoid *) (db->envhp), (ub4) OCI_HTYPE_ENV);
//	if (db->dschp)
//		OCIHandleFree((dvoid *) db->dschp, (ub4) OCI_HTYPE_DESCRIBE);
	return OCI_SUCCESS;
}

sword connectDB(DATABASE *db, text *username, text *password, text *tnsname,
		ub4 authmode, sword mode) {
	if (OCIEnvNlsCreate(&(db->envhp), OCI_DEFAULT | OCI_OBJECT, NULL, NULL,
			NULL, NULL, 0, NULL, db->charset, db->ncharset)) {
		printf("OCIEnvNlsCreate failed\n");
		return OCI_ERROR;
	}
	/*int ret= 0;
	 ret= OCIEnvInit( (OCIEnv **) &(db->envhp), OCI_DEFAULT, (size_t) 0, (dvoid **) 0 );
	 printf("OCIEnvInit %d\n", ret);
	 OCIEnvCreate(&(db->envhp),OCI_DEFAULT,0,0,0,0,0,0);
	 printf("OCIEnvCreate %d\n", ret);*/
	/* Initialize the OCI Process */
	//	if (OCIInitialize(OCI_DEFAULT, (dvoid *)0,a文件中commit=false的 sql语句个数
	//					(dvoid * (*)(dvoid *, size_t)) 0,
	//					(dvoid * (*)(dvoid *, dvoid *, size_t))0,
	//					(void (*)(dvoid *, dvoid *)) 0 ))
	//	{
	//		(void) printf("FAILED: OCIInitialize()\n");
	//		return OCI_ERROR;
	//	}

	/* Inititialize the OCI Environment */
	//	if (OCIEnvInit(&(db->envhp), (ub4) OCI_DEFAULT,
	//				 (size_t) 0, (dvoid **) 0 ))
	//	{
	//		(void) printf("FAILED: OCIEnvInit()\n");
	//		return OCI_ERROR;
	//	}
	if (OCIHandleAlloc((dvoid *) (db->envhp), (dvoid **) &(db->errhp),
			(ub4) OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0) || OCIHandleAlloc(
			(dvoid *) (db->envhp), (dvoid **) &(db->srvhp),
			(ub4) OCI_HTYPE_SERVER, (size_t) 0, (dvoid **) 0)
			|| OCIHandleAlloc((dvoid *) (db->envhp), (dvoid **) &(db->svchp),
					(ub4) OCI_HTYPE_SVCCTX, (size_t) 0, (dvoid **) 0)) {
		return OCI_ERROR;
	}
	if (OCIServerAttach(db->srvhp, db->errhp, tnsname,
			strlen((char *) tnsname), OCI_DEFAULT) || OCIAttrSet(
			(dvoid *) (db->svchp), OCI_HTYPE_SVCCTX, (dvoid *) (db->srvhp), 0,
			OCI_ATTR_SERVER, db->errhp) || OCIHandleAlloc(
			(dvoid *) (db->envhp), (dvoid **) &(db->seshp),
			(ub4) OCI_HTYPE_SESSION, (size_t) 0, (dvoid **) 0) || OCIAttrSet(
			(dvoid *) (db->seshp), OCI_HTYPE_SESSION, username, (ub4) (strlen(
					(char *) username)), OCI_ATTR_USERNAME, db->errhp)
			|| OCIAttrSet((dvoid *) (db->seshp), OCI_HTYPE_SESSION, password,
					(ub4) (strlen((char *) password)), OCI_ATTR_PASSWORD,
					db->errhp)) {
		oraError8(db);
		msg("connect database:%d,%s",db->errcode,db->errmsg);
		return OCI_ERROR;
	}
	if (OCISessionBegin(db->svchp, db->errhp, db->seshp, authmode,
			OCI_STMT_CACHE) || OCIAttrSet((dvoid *) (db->svchp),
			OCI_HTYPE_SVCCTX, db->seshp, 0, OCI_ATTR_SESSION, db->errhp)) {
		oraError8(db);
		msg("OCISessionBegin database:%d,%s",db->errcode,db->errmsg);
		return OCI_ERROR;
	}
	return OCI_SUCCESS;
}

sword connectSYS(DATABASE *db) {
	return connectDB(db, (text *) "", (text *) "", (text *) "", OCI_CRED_EXT,
			OCI_SYSDBA);
}

sword connectDB2(DATABASE *db, text *username, text *password, text *tnsname,
		ub4 authmode, sword mode) {
//		printf("username=%s,password=%s,tnsname=%s\n",username,password,tnsname);
	if (OCIEnvNlsCreate(&(db->envhp),OCI_DEFAULT, NULL, NULL, NULL, NULL, 0,
			NULL, db->charset, db->ncharset)) {
		return OCI_ERROR;
	}

	if (OCIHandleAlloc((dvoid *) (db->envhp), (dvoid **) &(db->errhp),
			(ub4) OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0) || OCIHandleAlloc(
			(dvoid *) (db->envhp), (dvoid **) &(db->srvhp),
			(ub4) OCI_HTYPE_SERVER, (size_t) 0, (dvoid **) 0)
			|| OCIHandleAlloc((dvoid *) (db->envhp), (dvoid **) &(db->svchp),
					(ub4) OCI_HTYPE_SVCCTX, (size_t) 0, (dvoid **) 0)) {
		return OCI_ERROR;
	}
	if (OCIServerAttach(db->srvhp, db->errhp, tnsname, strlen((char*)tnsname),
			OCI_DEFAULT) || OCIAttrSet((dvoid *) (db->svchp), OCI_HTYPE_SVCCTX,
			(dvoid *) (db->srvhp), 0, OCI_ATTR_SERVER, db->errhp)
			|| OCIHandleAlloc((dvoid *) (db->envhp), (dvoid **) &(db->seshp),
					(ub4) OCI_HTYPE_SESSION, (size_t) 0, (dvoid **) 0)
			|| OCIAttrSet((dvoid *) (db->seshp), OCI_HTYPE_SESSION, username,
					(ub4) (strlen((char*)username)), OCI_ATTR_USERNAME, db->errhp)
			|| OCIAttrSet((dvoid *) (db->seshp), OCI_HTYPE_SESSION, password,
					(ub4) (strlen((char*)password)), OCI_ATTR_PASSWORD, db->errhp)) {
		oraError8(db);
		return OCI_ERROR;
	}
	if (OCISessionBegin(db->svchp, db->errhp, db->seshp, authmode, mode)
			|| OCIAttrSet((dvoid *) (db->svchp), OCI_HTYPE_SVCCTX, db->seshp,
					0, OCI_ATTR_SESSION, db->errhp)) {
		oraError8(db);
		return OCI_ERROR;
	}

	return OCI_SUCCESS;
}

sword LogonDB(DATABASE *db, login *login_info) {
	if (strlen(login_info->user_name) == 0)
		return connectDB2(db, (text *)"", (text *)"", (text *)"", OCI_CRED_EXT, OCI_DEFAULT);
	else if (strlen(login_info->user_name) == 3 && strncasecmp(login_info->user_name, "SYS", 3) == 0)
		return connectSYS(db);
	else
		return connectDB2(db, (text *)login_info->user_name, (text *)login_info->password,
				(text *)login_info->tnsname, OCI_CRED_RDBMS, OCI_DEFAULT);
}

void checkerr(OCIError *errhp, sword status)
{
  text errbuf[512];
  sb4 errcode = 0;

  switch (status)
  {
  case OCI_SUCCESS:
    break;
  case OCI_SUCCESS_WITH_INFO:
    (void) printf("Error - OCI_SUCCESS_WITH_INFO\n");
    break;
  case OCI_NEED_DATA:
    (void) printf("Error - OCI_NEED_DATA\n");
    break;
  case OCI_NO_DATA:
    (void) printf("Error - OCI_NODATA\n");
    break;
  case OCI_ERROR:
    (void) OCIErrorGet ((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
                    errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
    (void) printf("Error - %.*s\n", 512, errbuf);
    break;
  case OCI_INVALID_HANDLE:
    (void) printf("Error - OCI_INVALID_HANDLE\n");
    break;
  case OCI_STILL_EXECUTING:
    (void) printf("Error - OCI_STILL_EXECUTE\n");
    break;
  case OCI_CONTINUE:
    (void) printf("Error - OCI_CONTINUE\n");
    break;
  default:
    break;
  }
}

void alter_schema(char *schema_name, DATABASE *db) {
	char sqlcmd[1024] = { 0 };
	sprintf(sqlcmd, "alter session set current_schema=%s", schema_name);
	prepareExecute8(db, &db->stmt, (text *)sqlcmd);
}

void printtypename (ub2 type, char *type_name)
{
  switch (type)
  {
    case SQLT_CHR: sprintf (type_name, "VARCHAR2");
              break;
    case SQLT_AFC: sprintf (type_name, "CHAR");
              break;
    case SQLT_DAT: sprintf (type_name, "DATE");
              break;
    case SQLT_TIMESTAMP: sprintf (type_name, "TIMESTAMP");
              break;
    case SQLT_INT: sprintf (type_name, "SIGNED INTEGER");
              break;
    case SQLT_UIN: sprintf (type_name, "UNSIGNED INTEGER");
              break;
    case SQLT_FLT: sprintf (type_name, "REAL");
              break;
    case SQLT_PDN: sprintf (type_name, "PACKED DECIMAL");
              break;
    case SQLT_BIN: sprintf (type_name, "BINARY DATA");
              break;
    case SQLT_NUM: sprintf (type_name, "NUMBER");
              break;
    case SQLT_BLOB : sprintf (type_name, "BLOB");
              break;
    case SQLT_CLOB : sprintf (type_name, "CLOB");
              break;
    case SQLT_LNG : sprintf(type_name, "LONG");
			  break;
    case SQLT_FILE : sprintf (type_name, "BFILE");
              break;
    case SQLT_NTY : sprintf (type_name, "NAMED DATA TYPE");
              break;
    case SQLT_REF : sprintf (type_name, "REF to a Named Data Type");
              break;
    case SQLT_RDD : sprintf (type_name, "ROWID");
			  break;
    default : sprintf (type_name, "UNKNOWN_%d\n", type);
              break;
  }
} /* end of printtypebyname () */

void write_ddl(char *file_path, char *ddl) {
	FILE *fp;
//	char buf[20480]={0};
	//TODO:need to use OCINlsCharSetConvert to conver charset to UTF8?
	char *buf = NULL;
	int len = strlen(ddl);
	if (len > 0) {
		buf = (char *) malloc(len + 5);
		if(!buf){
			return;
		}
		memset(buf, 0, len + 5);
		sprintf(buf, "%s;\n\n", ddl);
		if ((fp = fopen(file_path, "a+")) == NULL) {
			msg("Open or create: %s failed\n", file_path);
			free(buf);
			return;
		}
		fwrite(buf, 1, strlen(buf), fp);
		fclose(fp);
		free(buf);
	}
}

void write_dif_obj(char *file_path, char *objname){
	FILE *fp;
	char buf[128]={0};
	sprintf(buf, "%s\n", objname);
	if ((fp = fopen(file_path, "a+"))==NULL) {
		msg("Open or create: %s failed\n", file_path);
	}
	fwrite(buf, 1, strlen(buf), fp);
	fclose(fp);
}

void init_err_file(char *fname){
	memset(err_file, 0, sizeof(err_file));
	if(fname){
		memset(err_file, 0, sizeof(err_file));
		strcpy(err_file, fname);
	}
	if(access(err_file, F_OK)==0)
		remove(err_file);
//	FILE *fp;
//	char buf[128]={0};
//	sprintf(buf, "%s---start sync schema---\n\n", getcurtime());
//	if ((fp = fopen(err_file, "a+"))==NULL) {
//		msg("Open or create: %s failed\n", err_file);
//	}
//	fwrite(buf, 1, strlen(buf), fp);
//	fclose(fp);
}

void init_msg_file(char *fname){
	memset(msg_file, 0, sizeof(msg_file));
	if(access(fname, F_OK)==0){
		remove(fname);
	}
	if(fname){
		memset(msg_file, 0, sizeof(msg_file));
		strcpy(msg_file, fname);
	}
}

void start_status_file(char *fname){

	if(access(fname, F_OK)==0)
		remove(fname);
	FILE *fp;
	char buf[128]={0};
	sprintf(buf, "starttime=%s\n", getcurtime());
	if ((fp = fopen(fname, "a+"))==NULL) {
		msg("Open or create: %s failed\n", fname);
	}
	fwrite(buf, 1, strlen(buf), fp);
	fclose(fp);
}

void end_status_file(char *fname){

	FILE *fp;
	char buf[128]={0};
	sprintf(buf, "endtime=%s\n", getcurtime());
	if ((fp = fopen(fname, "a"))==NULL) {
		msg("Open or create: %s failed\n", fname);
	}
	fwrite(buf, 1, strlen(buf), fp);
	fclose(fp);
}

void finish_err_file(){
	FILE *fp;
	char buf[128]={0};
	sprintf(buf, "%s---finish sync schema---\n", getcurtime());
	if ((fp = fopen(err_file, "a+"))==NULL) {
		msg("Open or create: %s failed\n", err_file);
	}
	fwrite(buf, 1, strlen(buf), fp);
	fclose(fp);
}

void write_err_file(char *objtype, char *objname, char *errmessage){
	FILE *fp;
	char buf[1024]={0};
	int pos= 0;
	pos= pos+sprintf(buf+pos, "object_type=%s\n", objtype);
	pos= pos+sprintf(buf+pos, "object_name=%s\n", objname);
	pos= pos+sprintf(buf+pos, "err_mesage=%s\n", errmessage);
	if ((fp = fopen(err_file, "a+"))==NULL) {
		msg("Open or create: %s failed\n", err_file);
	}
	fwrite(buf, 1, strlen(buf), fp);
	fclose(fp);
}

void write_info_file(char *message){
	FILE *fp;
	char buf[1024]={0};
	int pos= 0;
	pos= pos+sprintf(buf+pos, "%s\n", message);
	if ((fp = fopen(err_file, "a+"))==NULL) {
		msg("Open or create: %s failed\n", err_file);
	}
	fwrite(buf, 1, strlen(buf), fp);
	fclose(fp);
}

void xlog_out(char *fmt, ...) {
    FILE *f1;
    va_list args;

    f1 = fopen(err_file, "a+");
    if (f1) {
        va_start(args, fmt);
        vfprintf(f1, fmt, args);
        va_end(args);
        fclose(f1);
    }
}

void xlog(char *fmt, ...) {
    FILE *f1;
    va_list args;

    f1 = fopen(msg_file, "a+");
    if (f1) {
        va_start(args, fmt);
        vfprintf(f1, fmt, args);
        va_end(args);
        fclose(f1);
    }
}

