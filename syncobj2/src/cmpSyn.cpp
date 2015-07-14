/*
 * cmpSyn.cpp
 *
 *  Created on: 2013-4-26
 *      Author: huangbin
 */

#include "comm.h"

extern login login_info1;
extern login login_info2;

static void create_or_replace_syn(DATABASE *db, char *sqlbuf, SYNONYMS_DEFINE *syn_def,
		char *file_path, int do_level){
	if(strlen(syn_def->syn_link)==0){
		sprintf(sqlbuf, "CREATE OR REPLACE SYNONYM \"%s\".\"%s\" FOR \"%s\".\"%s\"",
				login_info2.schema, syn_def->obj_name, syn_def->syn_schema, syn_def->syn_name);
	}else{
		sprintf(sqlbuf, "CREATE OR REPLACE SYNONYM \"%s\".\"%s\" FOR \"%s\".\"%s\"@\"%s\"",
				login_info2.schema, syn_def->obj_name, syn_def->syn_schema, syn_def->syn_name, syn_def->syn_link);
	}
	write_ddl(file_path, sqlbuf);
	if(do_level==1){
		prepareExecute8(db, &db->stmt, (text *)sqlbuf);
	}
}

void compare_syn(list<SYNONYMS_DEFINE *> *src_syn_def, list<SYNONYMS_DEFINE *> *dst_syn_def,
		int do_level, char *ddl_path, DATABASE *db){
	char ddl_syn_file[256]={0};
	char ddl_file[256]={0};

	list<SYNONYMS_DEFINE *>::iterator it_src_def;
	list<SYNONYMS_DEFINE *>::iterator it_dst_def;
	bool find= false;
	char ddl_sql[1024]= {0};
	char src_syn[256];
	char dst_syn[256];
	SYNONYMS_DEFINE *src_def;

	sprintf(ddl_syn_file, "%s/dif_synonym", ddl_path);
	sprintf(ddl_file, "%s/ddl_synonym.sql", ddl_path);

	if(access(ddl_syn_file, F_OK)==0){
		remove(ddl_syn_file);
	}
	if(access(ddl_file, F_OK)==0){
		remove(ddl_file);
	}

	for(it_src_def=src_syn_def->begin(); it_src_def!=src_syn_def->end(); it_src_def++){
		src_def= (*it_src_def);
		memset(src_syn, 0, sizeof(src_syn));
		strcpy(src_syn, src_def->obj_name);
		find= false;
		for(it_dst_def=dst_syn_def->begin(); it_dst_def!=dst_syn_def->end(); it_dst_def++){
			memset(dst_syn, 0, sizeof(dst_syn));
			strcpy(dst_syn, (*it_dst_def)->obj_name);
			if(strcmp(src_syn, dst_syn)==0){
				find= true;
				break;
			}
		}
		if(!find){
			msg("syn %s is different\n", src_syn);
			write_dif_obj(ddl_syn_file, src_syn);
			memset(ddl_sql, 0, sizeof(ddl_sql));
			create_or_replace_syn(db, ddl_sql, src_def,
					ddl_file, do_level);
		}
	}

}

void describe_syn (text *objname, DATABASE *db, SYNONYMS_DEFINE *syn_def)
{
  sword     retval;
  OCIParam *parmp;
  OCIDescribe *dschp;
  char chagen_name[256]={0};
  char syn_schema[256]={0};
  char syn_name[256]={0};
  char syn_link[256]={0};
  text *namep;
  ub4 sizep;

  memset(syn_def->syn_link, 0, sizeof(syn_def->syn_link));
  memset(syn_def->syn_name, 0, sizeof(syn_def->syn_name));
  memset(syn_def->syn_schema, 0, sizeof(syn_def->syn_schema));
  memset(syn_def->obj_name, 0, sizeof(syn_def->obj_name));

  checkerr (db->errhp, OCIHandleAlloc((dvoid *) db->envhp, (dvoid **) &dschp,
	                           (ub4) OCI_HTYPE_DESCRIBE,
	                           (size_t) 0, (dvoid **) 0));
  sprintf(chagen_name, "\"%s\"", objname);
  alarm(30);
  if ((retval = OCIDescribeAny(db->svchp, db->errhp, (dvoid *)chagen_name,
                               (ub4) strlen((char *) chagen_name),
                               OCI_OTYPE_NAME, (ub1)OCI_DEFAULT,
                               OCI_PTYPE_SYN, dschp)) != OCI_SUCCESS)
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

    checkerr (db->errhp, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &namep, (ub4 *) &sizep,
                         (ub4) OCI_ATTR_SCHEMA_NAME, (OCIError *)db->errhp));
    memcpy(syn_schema, namep, sizep);

    checkerr (db->errhp, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &namep, (ub4 *) &sizep,
                         (ub4) OCI_ATTR_NAME, (OCIError *)db->errhp));
    memcpy(syn_name, namep, sizep);

    checkerr (db->errhp, OCIAttrGet((dvoid*) parmp, (ub4) OCI_DTYPE_PARAM,
                         (dvoid*) &namep, (ub4 *) &sizep,
                         (ub4) OCI_ATTR_LINK, (OCIError *)db->errhp));
    memcpy(syn_link, namep, sizep);

    strcpy(syn_def->syn_name, syn_name);
    strcpy(syn_def->syn_schema, syn_schema);
    strcpy(syn_def->syn_link, syn_link);
    strcpy(syn_def->obj_name, (char *)objname);
    if(strlen(syn_def->syn_link)==0){
    	msg("%s for %s.%s\n", objname, syn_schema, syn_name);
    }else{
    	msg("%s for %s.%s@%s\n", objname, syn_schema, syn_name, syn_link);
    }

  }
                                               /* free the describe handle */
  OCIHandleFree((dvoid *) dschp, (ub4) OCI_HTYPE_DESCRIBE);
}
