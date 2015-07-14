/*
 * main.cpp
 *
 *  Created on: 2013-4-22
 *      Author: huangbin
 */
#include "comm.h"
#include <sys/types.h>
#include <sys/wait.h>

static char alphabet[] = "0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^-,abcdefghijklmnopqrstuvwxyz";//解密用
static char key[76] = "unimasrdb369";

extern void describe_table (text *objname, DATABASE *db, TABLE_DEFINE *table_def);
extern void compare_table(map<string, TABLE_DEFINE*> *src_table_def,
		map<string, TABLE_DEFINE*> *dst_table_def, int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst);
extern void compare_view(list<string> *src_view_def, list<string> *dst_view_def,
		int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst);
extern void describe_syn (text *objname, DATABASE *db, SYNONYMS_DEFINE *syn_def);
extern void compare_syn(list<SYNONYMS_DEFINE *> *src_syn_def, list<SYNONYMS_DEFINE *> *dst_syn_def,
		int do_level, char *ddl_path, DATABASE *db);
extern void compare_seq(list<string> *src_seq_def, list<string> *dst_seq_def,
		int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst);
extern void compare_fun2seq(list<string> *src_seq_def, list<string> *dst_seq_def,
		int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst);
extern void compare_source(list<string> *src_source_def, list<string> *dst_source_def,
		int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst, int source_type);
extern void create_rdbcopy_config(map<string, TABLE_DEFINE*> *src_table_def,
		map<string, TABLE_DEFINE*> *dst_table_def, int do_level, char *config_file,
		DATABASE *db, DATABASE *dbdst, char *ddl_path);
extern void compare_dir(list<string> *src_source_def, list<string> *dst_source_def,
		int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst);
extern void alter_schema(char *schema_name, DATABASE *db);
extern void disable_dst_fk(map<string, TABLE_DEFINE*> *dst_table_def, int do_level,
		char *ddl_path, DATABASE *dbdst);
extern void compare_job(int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst);

int parseconfile(char *confile, login *login_info1, login *login_info2,
		list<char *> *cmp_lst, char *ddl_path, int *do_level);
void describe_schema(char *schemaname, DATABASE *db, SCHEMA_OBJECT *schema_obj, list<char *> *cmplist);
int subprocess_fun(PROCPARAM *procParam);

login login_info1;
login login_info2;
int do_foreign_key= 0;
int do_index= 0;
int do_create_dictionary= 0;
int do_create_loadconf=0;
int do_create_copyconf=0;
int do_check= 0;
int do_table_privilege= 0;
int do_default= 0;
int do_unique= 0;
int init= 0;
int continuous= 0;
list<char *> cmp_lst;
list<string> view_list;

static void strtoupper(char *str, int str_len) {
	int i;
	for (i = 0; i < str_len; i++) {
		str[i] = toupper(str[i]);
	}
}

/*根据单词生成密钥*/
int constructkey(char *key) {
	int character;
	char *keyp;
	char *duplicate;

	if (*key == '\0') {
		return FALSE;
	}

	/*消除重复的字符*/
	for (keyp = key; (character = *keyp) != '\0';) {
		duplicate = ++keyp;
		while ((duplicate = strchr(duplicate, character)) != NULL) {
			strcpy(duplicate, duplicate + 1);
		}
	}

	/*将剩余的字符添加到key后面*/
	/*keyp这会正好指向数组的终结点上*/
	for (character = '0'; character <= 'z'; character += 1) {
		if (strchr(key, character) == NULL) {
			*keyp++ = character;
			*keyp = '\0';/*空字节结尾*/
		}
	}
	return TRUE;
}

/*解密,保留信息大小写*/
void decrypt(char *data, char const *key) {
	int character;
	for (; (character = *data) != '\0'; ++data) {
		if (character >= '0' && character <= 'z') {
			*data = alphabet[strchr(key, character) - key];
		} else {
			*data = character;
		}
//		printf("data=%c\n", *data);
	}
}

void free_cmplist(list<char *> *cmp_lst){
	list<char *>::iterator it;
	for(it=cmp_lst->begin(); it!=cmp_lst->end(); it++){
		char *tmp= (*it);
//		printf("%s\n", tmp);
		if(tmp)
			free(tmp);
	}
}

void free_synlist(list<SYNONYMS_DEFINE *> *syn_lst){
	list<SYNONYMS_DEFINE *>::iterator it;
	for(it=syn_lst->begin(); it!=syn_lst->end(); it++){
		SYNONYMS_DEFINE *tmp= (*it);
		if(tmp)
			free(tmp);
	}
}

void free_table_obj(map<string, TABLE_DEFINE*> *src_table_def){
	map<string, TABLE_DEFINE*>::iterator it;
	TABLE_DEFINE *src_td;
	list<COLUMN_DEFINE*>::iterator it_src_def;
	COLUMN_DEFINE *src_col;
	if(src_table_def->size()>0){
		for (it = src_table_def->begin(); it != src_table_def->end(); it++) {
			src_td = it->second;
			if(src_td && src_td->col_count>0){
				for(it_src_def=src_td->columns->begin(); it_src_def!=src_td->columns->end(); it_src_def++){
					src_col= (*it_src_def);
					if(src_col)
						delete src_col;
				}
				if(src_td)
					free(src_td);
			}
		}
	}
}

int isCmpObject(list<char *> *mylist, char *keystr){
	int ret= 0;
	list<char *>::iterator it;

	if(mylist->empty()){
		return ret;
	}
	char tmpstr[128]={0};
	for(it=mylist->begin(); it!=mylist->end(); it++){
		memset(tmpstr, 0, sizeof(tmpstr));
		strcpy(tmpstr, (*it));
		if(strcasecmp(tmpstr, keystr)==0){
			ret= 1;
			break;
		}
	}
	return ret;
}

CMP2SCHEMA *get_schema_node(char *schema, list<CMP2SCHEMA *> *schmalist){
	list<CMP2SCHEMA *>::iterator it;
	CMP2SCHEMA *node, *ret= NULL;

	for(it=schmalist->begin(); it!=schmalist->end(); it++){
		node= (*it);
		if(node && strcmp(node->src_schema, schema)==0){
			ret= node;
			break;
		}
	}
	return ret;
}

int readConfile(char *confile, login *login_info1, login *login_info2, list<char *> *cmp_lst,
		PROCPARAM *procParam){

	FILE *fp;
	char buf[256] = { 0 };
	char content1[256]={0};
	unsigned int len= 0;
	int srcinst[10] = { 0 };
	char srctmpip[160] = { 0 };
	char srcdbservicename[100]={0};
	char srcport[100]={0};
	char srcuser[100]={0};
	char srcpassword[100]={0};

	int dstinst[10] = { 0 };
	char dsttmpip[160] = { 0 };
	char dstdbservicename[100]={0};
	char dstport[100]={0};
	char dstuser[100]={0};
	char dstpassword[100]={0};

	DATABASE_LOGIN_STRUCT db_login1;
	DATABASE_LOGIN_STRUCT db_login2;
	db_login1.instance_no= 1;
	db_login2.instance_no= 1;
	char schema_name[256];

	int i;
	char *p;

	fp = fopen(confile, "r");
	if (fp == NULL) {
		printf("Can not open confile:%s\n", confile);
		return -1;
	}


	while (fgets(buf, sizeof(buf), fp)) {
		if (sscanf(buf, "src_dbinstindex=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			char *p = content1;
			int i = 0;
			while (*p) {
				if (*p != ';') {
					srcinst[i++] = *p - 48;
				} else {
					db_login1.instance_no++;
				}
				p++;
			}
		}else if (sscanf(buf, "src_dbinstip=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(srctmpip, content1);
		}else if (sscanf(buf, "src_dbservicename=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(srcdbservicename, content1);
		}else if (sscanf(buf, "src_dbinstport=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(srcport, content1);
		}else if (sscanf(buf, "src_dbinstuser=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(srcuser, content1);
		}else if (sscanf(buf, "src_dbinstpassword=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			if(!strncmp(content1,"{encrypted}",strlen("{encrypted}"))){    //解密
				char passbuf[100] = {0};
				strcpy(passbuf,content1+strlen("{encrypted}"));
				if (constructkey(key)){
					decrypt(passbuf,key);
//				   printf("decrypt: %s\n",data);
					strcpy(srcpassword, passbuf);
				}
			}else{
				strcpy(srcpassword, content1);
			}
		}else if (sscanf(buf, "src_schema=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			memset(schema_name, 0, sizeof(schema_name));
//			strtoupper(content1, strlen(content1));
			strcpy(schema_name, content1);
			p = strtok(schema_name, ",");
			while (p) {
				CMP2SCHEMA *cmpSchema= (CMP2SCHEMA *)malloc(sizeof(CMP2SCHEMA));
				memset(cmpSchema->src_schema, 0, sizeof(cmpSchema->src_schema));
				strcpy(cmpSchema->src_schema, p);
				cmpSchema->do_sync= 0;
				procParam->cmpchemalist.push_back(cmpSchema);
				p = strtok(NULL, ",");
			}
		}else if (sscanf(buf, "dst_dbinstindex=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			char *p = content1;
			int i = 0;
			while (*p) {
				if (*p != ';') {
					dstinst[i++] = *p - 48;
				} else {
					db_login2.instance_no++;
				}
				p++;
			}
		}else if (sscanf(buf, "dst_dbinstip=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(dsttmpip, content1);
		}else if (sscanf(buf, "dst_dbservicename=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(dstdbservicename, content1);
		}else if (sscanf(buf, "dst_dbinstport=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(dstport, content1);
		}else if (sscanf(buf, "dst_dbinstuser=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(dstuser, content1);
		}else if (sscanf(buf, "dst_dbinstpassword=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			if(!strncmp(content1,"{encrypted}",strlen("{encrypted}"))){    //解密
				char passbuf[100] = {0};
				strcpy(passbuf,content1+strlen("{encrypted}"));
				if (constructkey(key)){
					decrypt(passbuf,key);
//				   printf("decrypt: %s\n",data);
					strcpy(dstpassword, passbuf);
				}
			}else{
				strcpy(dstpassword, content1);
			}
		}else if (sscanf(buf, "dst_schema=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			memset(schema_name, 0, sizeof(schema_name));
//			strtoupper(content1, strlen(content1));
			strcpy(schema_name, content1);
			p = strtok(schema_name, ",");
			list<CMP2SCHEMA *>::iterator it;
			it= procParam->cmpchemalist.begin();
			while (p && it!=procParam->cmpchemalist.end()) {
				CMP2SCHEMA *cmpSchema= (*it);
				memset(cmpSchema->dst_schema, 0, sizeof(cmpSchema->dst_schema));
				strcpy(cmpSchema->dst_schema, p);
				p = strtok(NULL, ",");
				it++;
			}
		}else if (sscanf(buf, "compare_object=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			p = strtok(content1, ",,");
			while (p) {
				char *obj_type= (char *)malloc(strlen(p)+1);
				memset(obj_type, 0, strlen(p)+1);
				strcpy(obj_type, p);
				cmp_lst->push_back(obj_type);
				p = strtok(NULL, ",,");
			}
		}else if (sscanf(buf, "ddl_path=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			memset(procParam->ddl_path, 0, sizeof(procParam->ddl_path));
			strcpy(procParam->ddl_path, content1);
		}else if (sscanf(buf, "do_level=%s\n%n", content1, &len) == 1
						&& len == strlen(buf)) {
			procParam->do_level= atoi(content1);
		}else if (sscanf(buf, "do_foreign_key=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_foreign_key= atoi(content1);
		}else if (sscanf(buf, "do_index=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_index= atoi(content1);
		}else if (sscanf(buf, "do_create_dictionary=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_create_dictionary= atoi(content1);
		}else if (sscanf(buf, "do_create_loadconf=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_create_loadconf= atoi(content1);
		}else if (sscanf(buf, "do_create_copyconf=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_create_copyconf= atoi(content1);
		}else if (sscanf(buf, "do_check=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_check= atoi(content1);
		}else if (sscanf(buf, "do_table_privilege=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_table_privilege= atoi(content1);
		}else if (sscanf(buf, "do_default=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_default= atoi(content1);
		}else if (sscanf(buf, "do_unique=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_unique= atoi(content1);
		}else if (sscanf(buf, "threads=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			procParam->threads= atoi(content1);
		}else if (sscanf(buf, "init=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			init= atoi(content1);
		}else if (sscanf(buf, "conti_seq=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			continuous= atoi(content1);
		}
		memset(content1, 0, sizeof(content1));
	}

	fclose(fp);

	for (i = 0; i < 10; i++) {
		memset(db_login1.dbinstip[i], 0, 16);
	}
	i = 0;
	//分割ip
	p = strtok(srctmpip, ";");
	while (p) {
		strcpy(db_login1.dbinstip[(srcinst[i++] - 1)], p);
		p = strtok(NULL, ";");
	}

	for (i = 0; i < 10; i++) {
		memset(db_login1.dbinstport[i], 0, 6);
	}
	i = 0;
	p = strtok(srcport, ";");
	while (p) {
		strcpy(db_login1.dbinstport[(srcinst[i++] - 1)], p);
		p = strtok(NULL, ";");
	}

	memset(login_info1->user_name, 0, sizeof(login_info1->user_name));
	memset(login_info1->password, 0, sizeof(login_info1->password));
	memset(login_info1->tnsname, 0, sizeof(login_info1->tnsname));
	strcpy(login_info1->user_name, srcuser);
	strcpy(login_info1->password, srcpassword);

	strcpy(login_info1->tnsname, "(DESCRIPTION=(ADDRESS_LIST=");
	for (i = 0; i < db_login1.instance_no; i++) {
		char tmpaddress[100] = { 0 };
		sprintf(tmpaddress, "(ADDRESS=(HOST=%s)(PROTOCOL=TCP)(PORT=%s))",
				db_login1.dbinstip[i], db_login1.dbinstport[0]);
		strcat(login_info1->tnsname, tmpaddress);
	}
	strcat(login_info1->tnsname, "(FAILOVER=yes)(LOAD_BALANCE=yes))");
	char tmpsname[100] = { 0 };
	sprintf(tmpsname, "(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=%s)))",
			srcdbservicename);
	strcat(login_info1->tnsname, tmpsname);

	//dst
	for (i = 0; i < 10; i++) {
		memset(db_login2.dbinstip[i], 0, 16);
	}
	i = 0;
	//分割ip
	p = strtok(dsttmpip, ";");
	while (p) {
		strcpy(db_login2.dbinstip[(dstinst[i++] - 1)], p);
		p = strtok(NULL, ";");
	}

	for (i = 0; i < 10; i++) {
		memset(db_login2.dbinstport[i], 0, 6);
	}
	i = 0;
	p = strtok(dstport, ";");
	while (p) {
		strcpy(db_login2.dbinstport[(dstinst[i++] - 1)], p);
		p = strtok(NULL, ";");
	}

	memset(login_info2->user_name, 0, sizeof(login_info2->user_name));
	memset(login_info2->password, 0, sizeof(login_info2->password));
	memset(login_info2->tnsname, 0, sizeof(login_info2->tnsname));
	strcpy(login_info2->user_name, dstuser);
	strcpy(login_info2->password, dstpassword);

	strcpy(login_info2->tnsname, "(DESCRIPTION=(ADDRESS_LIST=");
	for (i = 0; i < db_login2.instance_no; i++) {
		char tmpaddress[100] = { 0 };
		sprintf(tmpaddress, "(ADDRESS=(HOST=%s)(PROTOCOL=TCP)(PORT=%s))",
				db_login2.dbinstip[i], db_login2.dbinstport[0]);
		strcat(login_info2->tnsname, tmpaddress);
	}
	strcat(login_info2->tnsname, "(FAILOVER=yes)(LOAD_BALANCE=yes))");
	memset(tmpsname, 0, sizeof(tmpsname));
	sprintf(tmpsname, "(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=%s)))",
			dstdbservicename);
	strcat(login_info2->tnsname, tmpsname);

//	printf("db1user=%s,pwd=%s\ntnsname=%s\n", login_info1->user_name, login_info1->password, login_info1->tnsname);
//	printf("db2user=%s,pwd=%s\ntnsname=%s\n", login_info2->user_name, login_info2->password, login_info2->tnsname);

	return 0;

}

int get_schema_from_list(CMP2SCHEMA *cmp2schema, list<CMP2SCHEMA *> *schemalist){
	list<CMP2SCHEMA *>::iterator it;
	CMP2SCHEMA *node;
	int ret= 0;
	for(it=schemalist->begin(); it!=schemalist->end(); it++){
		node= (*it);
		if(node->do_sync==0){
			sprintf(cmp2schema->src_schema, "%s", node->src_schema);
			sprintf(cmp2schema->dst_schema, "%s", node->dst_schema);
			msg("%s=>%s\n", cmp2schema->src_schema, cmp2schema->dst_schema);
			ret= 1;
			node->do_sync= 1;
			break;
		}
	}
	return ret;
}

int main(int argc, char *argv[]){
	char config_file[256]={0};
	char msg_file[256]={0};
	PROCPARAM procParam;
	int ret;
	if(argc<2){
		printf("Usage:syncObj configfile\n\n");
		exit(0);
	}
	strcpy(config_file, argv[1]);
	view_list.clear();
	readConfile(config_file, &login_info1, &login_info2, &cmp_lst, &procParam);
	if(strlen(procParam.ddl_path)==0){
		printf("please give output ddl path\n");
		return OCI_ERROR;
	}
	if(access(procParam.ddl_path, F_OK)!=0){
		char buffer[256];
		sprintf(buffer, "mkdir -p %s", procParam.ddl_path);
		system(buffer);
	}
	if(cmp_lst.size()==0){
		printf("Compare objest is null, exit normally\n");
		return 0;
	}
	sprintf(msg_file, "%s/log", procParam.ddl_path);
	init_msg_file(msg_file);

	CMP2SCHEMA cmp2schema;
	int all_tab_finish= 0;
	int all_proc_num= 0;
	pid_t pid_r;
	pid_t sync_pid[100]={0};
	int sync_status[100]={0};
	int i;

	for(i=0; i<procParam.threads; i++){
		ret= get_schema_from_list(&cmp2schema, &procParam.cmpchemalist);
//		msg("ret=%d\n", ret);
		if(ret==1){
			pid_r = fork();
			switch (pid_r) {
			case -1:
				printf("Create start_recv failed for instance %d\n", i);
				exit(1);
			case 0:
				memset(login_info1.schema, 0, sizeof(login_info1.schema));
				memset(login_info2.schema, 0, sizeof(login_info2.schema));
				strcpy(login_info1.schema, cmp2schema.src_schema);
				strcpy(login_info2.schema, cmp2schema.dst_schema);
				subprocess_fun(&procParam);
				break;
			default:
				break;
			}
			sync_pid[i] = pid_r;
			all_proc_num++;
		}else{
			all_tab_finish= 1;
		}
	}
	while(1){
		for(i=0; i<procParam.threads; i++){
			if(sync_pid[i]!=0){
				ret = waitpid(sync_pid[i], &sync_status[i], WNOHANG);
				if(ret!=0){
					all_proc_num--;
//						printf("ret_proc=%d, %d\n", ret_proc, sync_status[i]);
					if (WEXITSTATUS(sync_status[i]) == 0 && WIFEXITED(sync_status[i])) {
						msg("pid=%d exit successfully\n", sync_pid[i]);
					}else{
						msg("pid=%d exit failed\n", sync_pid[i]);
						exit(-1);
					}
					//if ret_proc < 0
					sync_pid[i]= 0;
					ret= get_schema_from_list(&cmp2schema, &procParam.cmpchemalist);
					msg("ret=%d\n", ret);
					if(ret==1){
						pid_r = fork();
						switch (pid_r) {
						case -1:
							printf("Create start_recv failed for instance %d\n", i);
							exit(1);
						case 0:
							memset(login_info1.schema, 0, sizeof(login_info1.schema));
							memset(login_info2.schema, 0, sizeof(login_info2.schema));
							strcpy(login_info1.schema, cmp2schema.src_schema);
							strcpy(login_info2.schema, cmp2schema.dst_schema);
							subprocess_fun(&procParam);
							break;
						default:
							break;
						}
						sync_pid[i] = pid_r;
						all_proc_num++;
					}
				}
			}
		}
		if(all_proc_num==0){
			printf("all schemas sync finished\n");
			break;
		}
		usleep(1000);
	}
	return 0;
}

void myAlarm(int sig){
	char err_msg[256]={0};
	sprintf(err_msg, "An error happened in connecting database.");
	xlog_out("sql=%s\nerror=%s\n", "null", err_msg);
	exit(-1);
}

void dealSegv(int sig){
	int i=3;
	while(i<65535){
		close(i);
		i++;
	}
	char err_msg[256]={0};
	sprintf(err_msg, "A Segment default error happened.");
	xlog_out("sql=%s\nerror=%s\n", "null", err_msg);
	exit(-1);
}

int subprocess_fun(PROCPARAM *procParam){
	char ddl_path[256]={0};
	char err_file[256]={0};
	char finish_file[256]={0};
	char status_file[256]={0};

	list<char *>::iterator it;
	int do_level= procParam->do_level;
	signal(SIGALRM, myAlarm);
	signal(SIGSEGV, dealSegv);

	msg("sync_schema=%s to %s, do_level=%d\n", login_info1.schema, login_info2.schema, do_level);
	sprintf(ddl_path, "%s/%s", procParam->ddl_path, login_info1.schema);
	if(strlen(ddl_path)==0){
		msg("please give output ddl path\n");
		return OCI_ERROR;
	}
	if(access(ddl_path, F_OK)!=0){
		char buffer[256];
		sprintf(buffer, "mkdir -p %s", ddl_path);
		system(buffer);
	}
	if(cmp_lst.size()==0){
		msg("Compare objest is null, exit normally\n");
		return 0;
	}

	sprintf(err_file, "%s/error", ddl_path);
	sprintf(finish_file, "%s/finish", ddl_path);
	sprintf(status_file, "%s/status", ddl_path);
	init_err_file(err_file);
	start_status_file(status_file);
	//src_schema,dst_schema,do_level,ddl_path
	DATABASE db1, db2;
	SCHEMA_OBJECT src_schema_obj;
	SCHEMA_OBJECT dst_schema_obj;
	src_schema_obj.obj_num= 0;
	src_schema_obj.tablemap.clear();
	dst_schema_obj.obj_num= 0;
	dst_schema_obj.tablemap.clear();

	initDB(&db1);
	initDB(&db2);

//	strtoupper(login_info1.schema, strlen(login_info1.schema));
//	strtoupper(login_info2.schema, strlen(login_info2.schema));

	db1.charset = 873;//utf8
	db1.ncharset = 2000;

	if(!(do_level>=300 && do_level<400)){
		if (LogonDB(&db1, &login_info1) != OCI_SUCCESS) {
			msg("connect to database failed, tns=%s\n", login_info1.tnsname);
			xlog_out("connect to database failed, tns=%s\n", login_info1.tnsname);
			finish_err_file();
			exit(-1);
		}
		msg("connect to source database success\n");
		alter_schema(login_info1.schema, &db1);
		msg("\n------------------------describe schema %s----------------------\n", login_info1.schema);
		describe_schema(login_info1.schema, &db1, &src_schema_obj, &cmp_lst);
	}

	if(do_level!=100){
		db2.charset = 873;//utf8
		db2.ncharset = 2000;
		if (LogonDB(&db2, &login_info2) != OCI_SUCCESS) {
			msg("connect to database failed, tns=%s\n", login_info2.tnsname);
			xlog_out("connect to database failed, tns=%s\n", login_info1.tnsname);
			finish_err_file();
			disconnectDB(&db1);
			exit(-1);
		}
		msg("connect destination database success\n");
		alter_schema(login_info2.schema, &db2);
		msg("\n------------------------describe schema %s----------------------\n", login_info2.schema);
		describe_schema(login_info2.schema, &db2, &dst_schema_obj, &cmp_lst);
	}

	if(do_level>=200 && do_level<300){
		msg("do_level=%d, compare table count\n", do_level);
		create_rdbcopy_config(&src_schema_obj.tablemap, &dst_schema_obj.tablemap, do_level, ddl_path, &db1, &db2, ddl_path);
		goto MAIN_EXIT;
	}
	if(do_level>=300 && do_level<400){
		disable_dst_fk(&dst_schema_obj.tablemap, do_level, ddl_path, &db2);
		goto MAIN_EXIT;
	}

	//compare type
	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "type")==0){
			msg("do_level=%d, compare type\n", do_level);
			compare_source(&src_schema_obj.typeBodyList, &dst_schema_obj.typeBodyList,
					do_level, ddl_path, &db1, &db2, SOURCE_TYPE);
		}
	}

	//compare typebody
	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "typebody")==0){
			msg("do_level=%d, compare typebody\n", do_level);
			compare_source(&src_schema_obj.typeList, &dst_schema_obj.typeList,
					do_level, ddl_path, &db1, &db2, SOURCE_TYPE_BODY);
		}
	}

	//compare table first
	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "table")==0){
			msg("do_level=%d, compare table\n", do_level);
			compare_table(&src_schema_obj.tablemap, &dst_schema_obj.tablemap, do_level, ddl_path, &db1, &db2);
		}
	}

	//compare view
	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "view")==0){
			msg("do_level=%d, compare view\n", do_level);
			compare_view(&src_schema_obj.viewList, &dst_schema_obj.viewList, do_level, ddl_path, &db1, &db2);
		}
	}

	//compare package
	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "package")==0){
			msg("do_level=%d, compare package\n", do_level);
			compare_source(&src_schema_obj.packageList, &dst_schema_obj.packageList,
					do_level, ddl_path, &db1, &db2, SOURCE_PACKAGE);
		}
	}

	//compare packageBody
	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "packagebody")==0){
			msg("do_level=%d, compare packagebody\n", do_level);
			compare_source(&src_schema_obj.packageBodyList, &dst_schema_obj.packageBodyList,
					do_level, ddl_path, &db1, &db2, SOURCE_PACKAGE_BODY);
		}
	}

	//compare syn
	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "synonym")==0){
			msg("do_level=%d, compare synonym\n", do_level);
			compare_syn(&src_schema_obj.synList, &dst_schema_obj.synList, do_level, ddl_path, &db2);
		}
	}

	//compare seq
	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "sequence")==0){
			msg("do_level=%d, compare sequence\n", do_level);
			if(continuous){
			compare_fun2seq(&src_schema_obj.seqList, &dst_schema_obj.seqList,
					do_level, ddl_path, &db1, &db2);
			}else{
				compare_seq(&src_schema_obj.seqList, &dst_schema_obj.seqList,
						do_level, ddl_path, &db1, &db2);
			}
		}
	}

	//compare function
	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "function")==0){
			msg("do_level=%d, compare function\n", do_level);
			compare_source(&src_schema_obj.functionList, &dst_schema_obj.functionList,
					do_level, ddl_path, &db1, &db2, SOURCE_FUNCTION);
		}
	}

	//compare procedure
	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "procedure")==0){
			msg("do_level=%d, compare procedure\n", do_level);
			compare_source(&src_schema_obj.procedureList, &dst_schema_obj.procedureList,
					do_level, ddl_path, &db1, &db2, SOURCE_PROCEDURE);
		}
	}

	//compare trigger
	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "trigger")==0){
			msg("do_level=%d, compare trigger\n", do_level);
			compare_source(&src_schema_obj.triggerList, &dst_schema_obj.triggerList,
					do_level, ddl_path, &db1, &db2, SOURCE_TRIGGER);
		}
	}

	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "directory")==0){
			msg("do_level=%d, compare directory\n", do_level);
			compare_dir(&src_schema_obj.dirList, &dst_schema_obj.dirList,
					do_level, ddl_path, &db1, &db2);
		}
	}

	for(it=cmp_lst.begin();it!=cmp_lst.end();it++){
		char type[32]={0};
		strcpy(type, (*it));
		if(strcasecmp(type, "job")==0){
			msg("do_level=%d, compare job\n", do_level);
			compare_job(do_level, ddl_path, &db1, &db2);
		}
	}

	MAIN_EXIT:

	disconnectDB(&db1);
	disconnectDB(&db2);

	//free src_schema_obj
	free_table_obj(&src_schema_obj.tablemap);
	//free dst_schema_obj
	free_table_obj(&dst_schema_obj.tablemap);
	//free cmp_lst
	free_cmplist(&cmp_lst);
	//free syn
	free_synlist(&src_schema_obj.synList);
	free_synlist(&dst_schema_obj.synList);
	msg("finish sync Object\n");
//	finish_err_file();
	FILE *fp= fopen(finish_file, "a+");
	fclose(fp);
	end_status_file(status_file);
	exit(0);
}

int parseconfile(char *confile, login *login_info1, login *login_info2, list<char *> *cmp_lst,
		char *ddl_path, int *do_level){
	FILE *fp;
	char buf[256] = { 0 };
	char content1[256]={0};
	unsigned int len= 0;
	int srcinst[10] = { 0 };
	char srctmpip[160] = { 0 };
	char srcdbservicename[100]={0};
	char srcport[100]={0};
	char srcuser[100]={0};
	char srcpassword[100]={0};

	int dstinst[10] = { 0 };
	char dsttmpip[160] = { 0 };
	char dstdbservicename[100]={0};
	char dstport[100]={0};
	char dstuser[100]={0};
	char dstpassword[100]={0};

	DATABASE_LOGIN_STRUCT db_login1;
	DATABASE_LOGIN_STRUCT db_login2;
	db_login1.instance_no= 1;
	db_login2.instance_no= 1;

	int i;
	char *p;

	fp = fopen(confile, "r");
	if (fp == NULL) {
		printf("Can not open confile:%s\n", confile);
		return -1;
	}


	while (fgets(buf, sizeof(buf), fp)) {
		if (sscanf(buf, "src_dbinstindex=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			char *p = content1;
			int i = 0;
			while (*p) {
				if (*p != ';') {
					srcinst[i++] = *p - 48;
				} else {
					db_login1.instance_no++;
				}
				p++;
			}
		}else if (sscanf(buf, "src_dbinstip=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(srctmpip, content1);
		}else if (sscanf(buf, "src_dbservicename=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(srcdbservicename, content1);
		}else if (sscanf(buf, "src_dbinstport=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(srcport, content1);
		}else if (sscanf(buf, "src_dbinstuser=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(srcuser, content1);
		}else if (sscanf(buf, "src_dbinstpassword=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			if(!strncmp(content1,"{encrypted}",strlen("{encrypted}"))){    //解密
				char passbuf[100] = {0};
				strcpy(passbuf,content1+strlen("{encrypted}"));
				if (constructkey(key)){
					decrypt(passbuf,key);
//				   printf("decrypt: %s\n",data);
					strcpy(srcpassword, passbuf);
				}
			}else{
				strcpy(srcpassword, content1);
			}
		}else if (sscanf(buf, "src_schema=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			memset(login_info1->schema, 0, sizeof(login_info1->schema));
//			strtoupper(content1, strlen(content1));
			strcpy(login_info1->schema, content1);
		}else if (sscanf(buf, "dst_dbinstindex=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			char *p = content1;
			int i = 0;
			while (*p) {
				if (*p != ';') {
					dstinst[i++] = *p - 48;
				} else {
					db_login2.instance_no++;
				}
				p++;
			}
		}else if (sscanf(buf, "dst_dbinstip=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(dsttmpip, content1);
		}else if (sscanf(buf, "dst_dbservicename=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(dstdbservicename, content1);
		}else if (sscanf(buf, "dst_dbinstport=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(dstport, content1);
		}else if (sscanf(buf, "dst_dbinstuser=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(dstuser, content1);
		}else if (sscanf(buf, "dst_dbinstpassword=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			if(!strncmp(content1,"{encrypted}",strlen("{encrypted}"))){    //解密
				char passbuf[100] = {0};
				strcpy(passbuf,content1+strlen("{encrypted}"));
				if (constructkey(key)){
					decrypt(passbuf,key);
//				   printf("decrypt: %s\n",data);
					strcpy(dstpassword, passbuf);
				}
			}else{
				strcpy(dstpassword, content1);
			}
		}else if (sscanf(buf, "dst_schema=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			memset(login_info2->schema, 0, sizeof(login_info2->schema));
//			strtoupper(content1, strlen(content1));
			strcpy(login_info2->schema, content1);
		}else if (sscanf(buf, "compare_object=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			p = strtok(content1, ",,");
			while (p) {
//				strcpy(db_login1.dbinstip[(srcinst[i++] - 1)], p);
				char *obj_type= (char *)malloc(strlen(p)+1);
				memset(obj_type, 0, strlen(p)+1);
				strcpy(obj_type, p);
				cmp_lst->push_back(obj_type);
				p = strtok(NULL, ",,");
			}
		}else if (sscanf(buf, "ddl_path=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			strcpy(ddl_path, content1);
		}else if (sscanf(buf, "do_level=%s\n%n", content1, &len) == 1
						&& len == strlen(buf)) {
			*do_level= atoi(content1);
		}else if (sscanf(buf, "do_foreign_key=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_foreign_key= atoi(content1);
		}else if (sscanf(buf, "do_index=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_index= atoi(content1);
		}else if (sscanf(buf, "do_create_dictionary=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_create_dictionary= atoi(content1);
		}else if (sscanf(buf, "do_create_loadconf=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_create_loadconf= atoi(content1);
		}else if (sscanf(buf, "do_create_copyconf=%s\n%n", content1, &len) == 1
				&& len == strlen(buf)) {
			do_create_copyconf= atoi(content1);
		}
		memset(content1, 0, sizeof(content1));
	}

	fclose(fp);

	for (i = 0; i < 10; i++) {
		memset(db_login1.dbinstip[i], 0, 16);
	}
	i = 0;
	//分割ip
	p = strtok(srctmpip, ";");
	while (p) {
		strcpy(db_login1.dbinstip[(srcinst[i++] - 1)], p);
		p = strtok(NULL, ";");
	}

	for (i = 0; i < 10; i++) {
		memset(db_login1.dbinstport[i], 0, 6);
	}
	i = 0;
	p = strtok(srcport, ";");
	while (p) {
		strcpy(db_login1.dbinstport[(srcinst[i++] - 1)], p);
		p = strtok(NULL, ";");
	}

	memset(login_info1->user_name, 0, sizeof(login_info1->user_name));
	memset(login_info1->password, 0, sizeof(login_info1->password));
	memset(login_info1->tnsname, 0, sizeof(login_info1->tnsname));
	strcpy(login_info1->user_name, srcuser);
	strcpy(login_info1->password, srcpassword);

	strcpy(login_info1->tnsname, "(DESCRIPTION=(ADDRESS_LIST=");
	for (i = 0; i < db_login1.instance_no; i++) {
		char tmpaddress[100] = { 0 };
		sprintf(tmpaddress, "(ADDRESS=(HOST=%s)(PROTOCOL=TCP)(PORT=%s))",
				db_login1.dbinstip[i], db_login1.dbinstport[0]);
		strcat(login_info1->tnsname, tmpaddress);
	}
	strcat(login_info1->tnsname, "(FAILOVER=yes)(LOAD_BALANCE=yes))");
	char tmpsname[100] = { 0 };
	sprintf(tmpsname, "(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=%s)))",
			srcdbservicename);
	strcat(login_info1->tnsname, tmpsname);

	//dst
	for (i = 0; i < 10; i++) {
		memset(db_login2.dbinstip[i], 0, 16);
	}
	i = 0;
	//分割ip
	p = strtok(dsttmpip, ";");
	while (p) {
		strcpy(db_login2.dbinstip[(dstinst[i++] - 1)], p);
		p = strtok(NULL, ";");
	}

	for (i = 0; i < 10; i++) {
		memset(db_login2.dbinstport[i], 0, 6);
	}
	i = 0;
	p = strtok(dstport, ";");
	while (p) {
		strcpy(db_login2.dbinstport[(dstinst[i++] - 1)], p);
		p = strtok(NULL, ";");
	}

	memset(login_info2->user_name, 0, sizeof(login_info2->user_name));
	memset(login_info2->password, 0, sizeof(login_info2->password));
	memset(login_info2->tnsname, 0, sizeof(login_info2->tnsname));
	strcpy(login_info2->user_name, dstuser);
	strcpy(login_info2->password, dstpassword);

	strcpy(login_info2->tnsname, "(DESCRIPTION=(ADDRESS_LIST=");
	for (i = 0; i < db_login2.instance_no; i++) {
		char tmpaddress[100] = { 0 };
		sprintf(tmpaddress, "(ADDRESS=(HOST=%s)(PROTOCOL=TCP)(PORT=%s))",
				db_login2.dbinstip[i], db_login2.dbinstport[0]);
		strcat(login_info2->tnsname, tmpaddress);
	}
	strcat(login_info2->tnsname, "(FAILOVER=yes)(LOAD_BALANCE=yes))");
	memset(tmpsname, 0, sizeof(tmpsname));
	sprintf(tmpsname, "(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=%s)))",
			dstdbservicename);
	strcat(login_info2->tnsname, tmpsname);

	printf("db1user=%s,pwd=%s\ntnsname=%s\n", login_info1->user_name, login_info1->password, login_info1->tnsname);
	printf("db2user=%s,pwd=%s\ntnsname=%s\n", login_info2->user_name, login_info2->password, login_info2->tnsname);

	return 0;
}

void describe_schema(char *schemaname, DATABASE *db, SCHEMA_OBJECT *schema_obj, list<char *> *cmplist){
	sword retval;
	OCIParam *parmp;
	text *namep;
	ub4 sizep;
	OCIParam *objlist;
	ub2 objnum=0;
	TABLE_DEFINE *table_def= NULL;

	checkerr (db->errhp, OCIHandleAlloc((dvoid *) db->envhp, (dvoid **) &db->dschp,
	                           (ub4) OCI_HTYPE_DESCRIBE,
	                           (size_t) 0, (dvoid **) 0));
	alarm(30);
	if ((retval = OCIDescribeAny(db->svchp, db->errhp, (dvoid *)schemaname,
							   (ub4) strlen((char *) schemaname),
							   OCI_OTYPE_NAME, (ub1)1,
							   OCI_PTYPE_SCHEMA, db->dschp)) != OCI_SUCCESS)
	{
		alarm(0);
		if (retval == OCI_NO_DATA)
		{
		  printf("NO DATA: OCIDescribeAny on %s\n", schemaname);
		}
		else                                                      /* OCI_ERROR */
		{
		  printf( "ERROR: OCIDescribeAny on %s\n", schemaname);
		  checkerr(db->errhp, retval);
		  return;
		}
	}
	else
	{
		alarm(0);
	    checkerr (db->errhp, OCIAttrGet((dvoid *)db->dschp, (ub4)OCI_HTYPE_DESCRIBE,
	                         (dvoid *)&parmp, (ub4 *)0, (ub4)OCI_ATTR_PARAM,
	                         (OCIError *)db->errhp));
	    checkerr (db->errhp, OCIAttrGet((dvoid *)parmp, (ub4)OCI_DTYPE_PARAM,
	                         (dvoid *)&objlist, (ub4 *)0, (ub4)OCI_ATTR_LIST_OBJECTS,
	                         (OCIError *)db->errhp));
	    checkerr (db->errhp, OCIAttrGet((dvoid *)objlist, (ub4)OCI_DTYPE_PARAM,
	                         (dvoid *)&objnum, (ub4 *)0, (ub4)OCI_ATTR_NUM_PARAMS,
	                         (OCIError *)db->errhp));

	    schema_obj->obj_num= objnum;
	    msg("objnum=%d\n", objnum);
	    ub4 i;
	    for(i=0; i<objnum; i++){
	    	OCIParam *objh= NULL;
	        checkerr (db->errhp, OCIParamGet((dvoid *)objlist, (ub4)OCI_DTYPE_PARAM, db->errhp,
	                           (dvoid **)(&objh), (ub4) i));
		    checkerr (db->errhp, OCIAttrGet((dvoid *)objh, (ub4)OCI_DTYPE_PARAM,
		                         (dvoid *)&namep, (ub4 *)&sizep, (ub4)OCI_ATTR_OBJ_NAME,
		                         (OCIError *)db->errhp));
		    char objname[256]={0};
		    strcpy(objname, (char *)namep);
		    objname[sizep]='\0';

		    ub1 objtype;
		    checkerr (db->errhp, OCIAttrGet((dvoid *)objh, (ub4)OCI_DTYPE_PARAM,
		                         (dvoid *)&objtype, (ub4 *)0, (ub4)OCI_ATTR_PTYPE,
		                         (OCIError *)db->errhp));
		    if(objtype!=0){
		    	//printf("\n------------------------------\n");

		    	if(strncasecmp(objname, "BIN$", 4)==0){
		    		continue;
		    	}

		    	switch(objtype){
		    	case OCI_PTYPE_TABLE:
		    		if(isCmpObject(cmplist, "table")){
//			    		msg("objnum=%d, objname=%s, objtype=%d\n", objnum, objname, objtype);
//			    		msg("compare table object\n");
						table_def= (TABLE_DEFINE *)malloc(sizeof(TABLE_DEFINE));
						table_def->table_id= 0;
						memset(table_def->owner, 0, sizeof(table_def->owner));
						strcpy(table_def->owner, schemaname);
						memset(table_def->name, 0, sizeof(table_def->name));
						strcpy(table_def->name, objname);
						table_def->col_count= 0;
						table_def->has_object_type= false;
						table_def->columns= new list<COLUMN_DEFINE *>;
						table_def->columns->clear();
						describe_table((text *)objname, db, table_def);
						schema_obj->tablemap[objname]= table_def;
		    		}
		    		break;
		    	case OCI_PTYPE_VIEW:
		    		//SELECT TEXT FROM ALL_VIEWS WHERE OWNER='HB';
		    		if(isCmpObject(cmplist, "view")){
		    			schema_obj->viewList.push_back(objname);
		    		}
		    		view_list.push_back(objname);
		    		break;
		    	case OCI_PTYPE_PROC:
		    		if(isCmpObject(cmplist, "procedure")){
			    		msg("objnum=%d, objname=%s, objtype=%d\n", objnum, objname, objtype);
			    		msg("compare procedure object\n");
		    			schema_obj->procedureList.push_back(objname);
		    		}
		    		break;
		    	case OCI_PTYPE_FUNC:
		    		if(isCmpObject(cmplist, "function")){
			    		msg("objnum=%d, objname=%s, objtype=%d\n", objnum, objname, objtype);
			    		msg("compare function object\n");
		    			schema_obj->functionList.push_back(objname);
		    		}
		    		break;
		    	case OCI_PTYPE_PKG:
		    		if(isCmpObject(cmplist, "package")){
			    		msg("objnum=%d, objname=%s, objtype=%d\n", objnum, objname, objtype);
			    		msg("compare package object\n");
		    			schema_obj->packageList.push_back(objname);
		    		}
		    		break;
		    	case OCI_PTYPE_TYPE:
		    		if(isCmpObject(cmplist, "type")){

		    		}
		    		break;
		    	case OCI_PTYPE_SYN:
		    		if(isCmpObject(cmplist, "synonym")){
	//		    		msg("objnum=%d, objname=%s, objtype=%d\n", objnum, objname, objtype);
	//		    		msg("compare synonyms object\n");
						SYNONYMS_DEFINE *syn_def;
						syn_def = (SYNONYMS_DEFINE *)malloc(sizeof(SYNONYMS_DEFINE));
						describe_syn((text *)objname, db, syn_def);
						if(strlen(syn_def->syn_name)>0){
							schema_obj->synList.push_back(syn_def);
						}else{
							free(syn_def);
						}

		    		}
		    		break;
		    	case OCI_PTYPE_SEQ:
		    		if(isCmpObject(cmplist, "sequence")){
			    		msg("objnum=%d, objname=%s, objtype=%d\n", objnum, objname, objtype);
			    		msg("compare synonyms object\n");
			    		schema_obj->seqList.push_back(objname);
		    		}
		    		//break;
		    	case OCI_PTYPE_COL:
		    		//break;
		    	case OCI_PTYPE_ARG:
		    		//break;
		    	case OCI_PTYPE_LIST:
		    		//break;
		    	default:
		    		break;
		    	}
		    }
	    }
	}
	OCIHandleFree((dvoid *) db->dschp, (ub4) OCI_HTYPE_DESCRIBE);
}
