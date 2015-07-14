/*
 * cmpView.cpp
 *
 *  Created on: 2013-4-24
 *      Author: huangbin
 */

#include "comm.h"

#define PIECE_SIZE 1000
#define DATA_SIZE 10240

extern login login_info1;
extern login login_info2;

list<string> dif_view;

void create_or_replace_view(DATABASE *db, char *sql_text, char *view_name,
		char *file_path, int text_len, int do_level){
	char *sqlbuf= NULL;
	if(text_len==0){
		return;
	}
	sqlbuf= (char *)malloc(text_len+256);
	memset(sqlbuf, 0, text_len+256);
	sprintf(sqlbuf, "CREATE OR REPLACE FORCE VIEW %s.%s AS %s", login_info2.schema, view_name, sql_text);
	write_ddl(file_path, sqlbuf);
	if(do_level==1){
		prepareExecute8(db, &db->stmt, (text *)sqlbuf);
	}
	free(sqlbuf);
}

void select_view(DATABASE *db, char *view_name, ub4 *text_len, char **sql_context){
	char tmp[1024000]={0};
	char sel_stmt[256]={0};
	sword status;
	OCIDefine *defcolp[2];
	sword indp[2];

	sprintf(sel_stmt, "SELECT TEXT FROM ALL_VIEWS WHERE OWNER='%s' and view_name='%s'", login_info1.schema, view_name);

	status= OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sel_stmt, strlen(sel_stmt), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (status != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 %s\n", db->errmsg);
		return;
	}

	status = OCIDefineByPos(db->stmt, &defcolp[0], db->errhp,
			(ub4) 1, (dvoid *) tmp, (sb4) sizeof(tmp),
			SQLT_STR, (dvoid *) &indp[0], (ub2 *) 0, (ub2 *) 0,
			(ub4) OCI_DEFAULT);

	if (status != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}

//	status = OCIStmtExecute(db->svchp, db->stmt, db->errhp, (ub4) 0,
//			0, 0, 0, OCI_DEFAULT);
	status = executeQuery8(db, db->stmt);
	if (status != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute 333:%s\n", db->errmsg);
		OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
		return;
	}

	for (;;) {
		status = OCIStmtFetch(db->stmt, db->errhp, 1,
				OCI_FETCH_NEXT, OCI_DEFAULT);
		if (status == OCI_NO_DATA) {
//			msg("NO DATA\n");
			break;
		} else {
			int len= strlen(tmp);
			char *retsql= (char *)malloc(len+1);
			memcpy(retsql, tmp, len);
			retsql[len]='\0';
			*sql_context= retsql;
			*text_len= len+1;
		}
	}
	OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

/*perform piecewise select with polling*/
void select_piecewise_polling(DATABASE *db, char *view_name, ub4 *text_len, char **sql_context)
{
	OCIDefine *defnp1 = (OCIDefine *) NULL;
	ub4 i;
	sword status;
	char buf1[PIECE_SIZE];
	ub4   alen  = PIECE_SIZE;
	ub1   piece = OCI_FIRST_PIECE;
	dvoid *hdlptr = (dvoid *) 0;
	ub4 hdltype = OCI_HTYPE_DEFINE, iter = 0, idx = 0;
	ub1   in_out = 0;
	sb2   indptr = 0;
	ub2   rcode = 0;
	ub2 ret= 0;
	char *ret_text= NULL;
	ub4 all_len= 0;
	char sel_stmt[256]={0};
	*text_len= 0;

	sprintf(sel_stmt, "SELECT TEXT FROM ALL_VIEWS WHERE OWNER='%s' and view_name='%s'", login_info1.schema, view_name);

	status= OCIStmtPrepare2(db->svchp, &db->stmt, db->errhp,
			(text *) sel_stmt, strlen(sel_stmt), NULL, 0,
			OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (status != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIStmtPrepare2 %s\n", db->errmsg);
		return;
	}


	status= OCIDefineByPos(db->stmt, &defnp1,
				db->errhp, (ub4) 1, (dvoid *) 0,
                (sb4) DATA_SIZE, (ub2)SQLT_CHR, (dvoid *)0,
                (ub2 *) 0, (ub2 *)0, (ub4)OCI_DYNAMIC_FETCH);
	if (status != OCI_SUCCESS) {
		oraError8(db);
		msg("error OCIDefineByPos %s\n", db->errmsg);
		goto select_view_exit;
	}
//	status= OCIStmtExecute(db->svchp,
//                          db->stmt,
//                          db->errhp, (ub4) 0, (ub4)0,
//                          (OCISnapshot *) NULL, (OCISnapshot *) NULL,
//                          OCI_DEFAULT);
	status = executeQuery8(db, db->stmt);
	if (status != OCI_SUCCESS) {
//		oraError8(db);
		msg("error OCIStmtExecute %s\n", db->errmsg);
		goto select_view_exit;
	}
	status = OCIStmtFetch(db->stmt, db->errhp,
                            (ub4) 1, (ub2) OCI_FETCH_NEXT, (ub4) OCI_DEFAULT);

    printf("checking contents of RESUME piece by piece\n");
    while (status == OCI_NEED_DATA)
    {
      ret= OCIStmtGetPieceInfo(db->stmt,
                              db->errhp, &hdlptr, &hdltype,
                              &in_out, &iter, &idx, &piece);
      if(ret!=OCI_SUCCESS){
    	  oraError8(db);
    	  msg("error OCIStmtGetPieceInfo %s\n", db->errmsg);
    	  status= ret;
    	  break;
      }

      ret= OCIStmtSetPieceInfo((dvoid *)hdlptr, (ub4)hdltype,
                                    db->errhp, (dvoid *) &buf1, &alen, piece,
                                    (dvoid *)&indptr, &rcode);
      if(ret!=OCI_SUCCESS){
    	  oraError8(db);
    	  msg("error OCIStmtSetPieceInfo %s\n", db->errmsg);
    	  status= ret;
    	  break;
      }

      status = OCIStmtFetch(db->stmt,db->errhp, (ub4) 1,
                            (ub2) OCI_FETCH_NEXT, (ub4) OCI_DEFAULT);

      if(status!=OCI_SUCCESS){
    	  oraError8(db);
    	  msg("error OCIStmtFetch %s\n", db->errmsg);
    	  break;
      }
      if(alen>0){
    	  if(!ret_text){
    		  ret_text= (char *)malloc(alen);
    	  }else{
    		  ret_text= (char *)realloc(ret_text, all_len+alen);
    	  }
	      for (i = 0; i < alen; i++){
	    	  ret_text[all_len+i]= buf1[i];
	      }
		  all_len += alen;
      }

    }
    if(status == OCI_SUCCESS){
		printf("SUCCESS: fetched all pieces of RESUME CORRECTLY\n");
		char *tmp= (char *)malloc(all_len+1);
		memcpy(tmp, ret_text, all_len);
		tmp[all_len]='\0';
		free(ret_text);
		*sql_context= tmp;
		*text_len= all_len+1;
    }else{
    	if(ret_text)
    		free(ret_text);
    	*text_len= 0;
    }
    select_view_exit:
    OCIStmtRelease(db->stmt, db->errhp, NULL, 0, OCI_DEFAULT);
}

void compare_view(list<string> *src_view_def, list<string> *dst_view_def,
		int do_level, char *ddl_path, DATABASE *db, DATABASE *dbdst){
	char ddl_view_file[256]={0};
	char ddl_file[256]={0};

	list<string>::iterator it_src_def;
	list<string>::iterator it_dst_def;
	bool find= false;
	char *ddl_sql= NULL;
	ub4 ddl_len= 0;
	char src_view[256];
	char dst_view[256];

	sprintf(ddl_view_file, "%s/dif_view", ddl_path);
	sprintf(ddl_file, "%s/ddl_view.sql", ddl_path);

	if(access(ddl_view_file, F_OK)==0){
		remove(ddl_view_file);
	}
	if(access(ddl_file, F_OK)==0){
		remove(ddl_file);
	}

	dif_view.clear();

	for(it_src_def=src_view_def->begin(); it_src_def!=src_view_def->end(); it_src_def++){
		memset(src_view, 0, sizeof(src_view));
		strcpy(src_view, (*it_src_def).c_str());
		find= false;
		for(it_dst_def=dst_view_def->begin(); it_dst_def!=dst_view_def->end(); it_dst_def++){
			memset(dst_view, 0, sizeof(dst_view));
			strcpy(dst_view, (*it_dst_def).c_str());
			if(strcasecmp(src_view, dst_view)==0){
				find= true;
				break;
			}
		}
		if(!find){
			dif_view.push_back(src_view);
		}
	}

	if(dif_view.size()>0){
		list<string>::iterator it;
		for(it=dif_view.begin(); it!=dif_view.end(); it++){
			memset(src_view, 0, sizeof(src_view));
			strcpy(src_view, (*it).c_str());
			msg("%s\n", src_view);
			write_dif_obj(ddl_view_file, src_view);
			select_view(db, src_view, &ddl_len, &ddl_sql);
			if(ddl_len>0){
				msg("%s\n", ddl_sql);
				create_or_replace_view(dbdst, ddl_sql, src_view,
						ddl_file, ddl_len, do_level);
				if(ddl_sql){
					free(ddl_sql);
					ddl_sql= NULL;
				}
			}
		}
	}else{
		msg("src views are all in dest already.\n");
	}
}
