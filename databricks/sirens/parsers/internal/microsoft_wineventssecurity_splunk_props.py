from pyspark.sql.functions import conv, col, split, substring, coalesce, when, regexp_extract, concat, lit, length
from pyspark.sql.types import LongType

def get_winevent_props():
    return {
        "authentication_method": when(col("EventID._VALUE").isin(['4624', '4625']), col("EventDataDct.AuthenticationPackageName")).otherwise(col("EventDataDct.authentication_method")),
 
        "object_attrs": when((col("EventID._VALUE").isin(['4720', '4738'])), None)
                    .when((col("EventID._VALUE") == '4717'), lit("AccessGranted"))
                    .when((col("EventID._VALUE") == '4718'), lit("AccessRemoved"))
                    .when((col("EventDataDct.SamAccountName").isNotNull() & ~col("EventID._VALUE").isin(['4727', '4730', '4731', '4734', '4735', '4737', '4754', '4755', '4758', '4764', '4799'])), col("EventDataDct.SamAccountName"))
                    .when((col("EventID._VALUE").isin(['4728', '4729', '4732', '4733', '4756', '4757'])), col("EventDataDct.TargetUserName"))
                    .when((col("EventID._VALUE").isin(['4698', '4700', '4701'])), col("EventDataDct.TaskContent"))
                    .when((col("EventID._VALUE") == '4702'), col("EventDataDct.TaskContentNew"))
                    .when((col("EventID._VALUE") == '4719'), concat(lit("Category="), col("EventDataDct.CategoryId"), lit(",Subcategory="), col("EventDataDct.SubcategoryId"), lit(",Subcategory GUID="), col("EventDataDct.SubcategoryGuid"), lit(",Changes="), col("EventDataDct.AuditPolicyChanges")))
                    .otherwise(col("EventDataDct.object_attrs")),
                
        "file_name": when(
                        (col("EventDataDct.Object_Type") == "File") & (col("EventDataDct.object_file_name").isNotNull()),
                        col("EventDataDct.object_file_name")
                    ).otherwise(
                        when(
                            col("EventDataDct.Share_Name").isNotNull(),
                            col("EventDataDct.Share_Name")
                        ).otherwise(col("EventDataDct.file_name"))
                    ),
 
        "file_path": when(
                        (col("EventDataDct.Object_Type") == "File") & (col("EventDataDct.object_file_path").isNotNull()),
                        col("EventDataDct.object_file_path")
                    ).otherwise(
                        when(
                            col("EventDataDct.Share_Path").isNotNull(),
                            col("EventDataDct.Share_Path")
                        ).otherwise(col("EventDataDct.file_path"))
                    ),
        
        "registry_path": when((col("EventID._VALUE") == "4657"), col("EventDataDct.ObjectName")
                    ).otherwise(col("EventDataDct.registry_path")),
        
        "registry_value_name": when(
                        (col("EventID._VALUE") == '4657'), col("EventDataDct.ObjectValueName")
                    ).otherwise(col("EventDataDct.registry_value_name")),

        "process_path": when(col("EventID._VALUE")=='4688', col("EventDataDct.NewProcessName"))
                                                                .when(col("EventID._VALUE")=='4696', col("EventDataDct.TargetProcessName"))
                                                                .otherwise(coalesce(col("EventDataDct.ProcessId"), col("EventDataDct.ProcessName"))),                                  

        "process_id": when(
            (col("EventID._VALUE") == '4688') & (col("EventDataDct.NewProcessId").isNotNull()),
            conv(substring(col("EventDataDct.NewProcessId"), 3, length(col("EventDataDct.NewProcessId"))), 16, 10).cast(LongType())
        ).when(
            (col("EventID._VALUE") == '4696') & (col("EventDataDct.TargetProcessId").isNotNull()),
            conv(substring(col("EventDataDct.TargetProcessId"), 3, length(col("EventDataDct.NewProcessId"))), 16, 10).cast(LongType())
        ).when(
            col("EventDataDct.ProcessId").isNotNull(),
            conv(substring(col("EventDataDct.ProcessId"), 3, length(col("EventDataDct.NewProcessId"))), 16, 10).cast(LongType())
        ).otherwise(
            lit(None)
        ),

        "parent_process": when(col("EventID._VALUE")=='4696', col("EventDataDct.ProcessName")).otherwise(col("EventDataDct.ParentProcessName")),

        "parent_process_id": when(
                        (col("EventID._VALUE") == '4688'), col("EventDataDct.CreatorProcessID")
                    ).when(
                        (col("EventID._VALUE") == '4696'), col("EventDataDct.ProcessID")
                    ),

        "parent_process_name": when(
                        (col("EventID._VALUE") == '4696'), regexp_extract(col("EventDataDct.ProcessName"), r"(?:.*\\)?(.*)", 1)
                    ).otherwise(col("EventDataDct.ParentProcessName")),

        "process_path": when(
                        (col("EventID._VALUE") == '4688'), col("EventDataDct.NewProcessName")
                    ).when(
                        (col("EventID._VALUE") == '4696'), col("EventDataDct.TargetProcessName")
                    ).when(
                        (col("EventID._VALUE").isin(['4689', '4674', '4673'])), col("EventDataDct.ProcessName")
                    ),

        "parent_process_path": when(
                        (col("EventID._VALUE") == '4688'), col("EventDataDct.CreatorProcessName")
                    ).when(
                        (col("EventID._VALUE") == '4696'), col("EventDataDct.Process_Name")
                    ),

        "process_exec": when(
                        (col("EventID._VALUE") == '4688'), col("EventDataDct.NewProcessName")
                    ).when(
                        (col("EventID._VALUE") == '4696'), col("EventDataDct.TargetProcessName")
                    ).when(
                        (col("EventID._VALUE").isin(['4689', '4674', '4673'])), regexp_extract(col("EventDataDct.ProcessName"), r"(?:.*\\)?(.*)", 1)
                    ),

        "process_name": when(
                        (col("EventID._VALUE") == '4688'), col("EventDataDct.NewProcessName")
                    ).when(
                        (col("EventID._VALUE") == '4696'), col("EventDataDct.TargetProcessName")
                    ).when(
                        (col("EventID._VALUE").isin(['4689', '4674', '4673'])), regexp_extract(col("EventDataDct.ProcessName"), r"(?:.*\\)?(.*)", 1)
                    ).otherwise(col("EventDataDct.ProcessName")),

        "user": when(col("EventID._VALUE").isin(['4727', '4730', '4731', '4734', '4735', '4737', '4754', '4755', '4758', '4764']), lit(None))
                    .when((col("EventID._VALUE") == '4688') & ((col("EventDataDct.user") == "-") | col("EventDataDct.user").isNull()), col("EventDataDct.SrcUser"))
                    .when(col("EventID._VALUE").isin(['1102', '4673', '4674', '4689', '4697', '4698', '4700', '4701', '4702', '4719', '4799']), 
                        when(col("EventDataDct.SubjectUserName") != "-", col("EventDataDct.SubjectUserName")))
                    .when((col("EventID._VALUE") == '4696') & (col("EventDataDct.user") != "-"), col("EventDataDct.user"))
                    .when(col("EventID._VALUE").isin(['4703', '4704', '4705', '4720', '4722', '4723', '4724', '4725', '4726', '4738', '4767']), col("EventDataDct.TargetUserName"))
                    .when(col("EventID._VALUE") == '4781', col("EventDataDct.NewTargetUserName"))
                    .when(col("EventID._VALUE").isin(['4728', '4729', '4732', '4733', '4756', '4757']), 
                        when(col("EventDataDct.MemberSid").like("%\\%"), split(col("EventDataDct.MemberSid"), "\\\\").getItem(-1))
                        .otherwise(when(col("EventDataDct.MemberUserName").like("%\\%"), None).otherwise(col("EventDataDct.MemberUserName"))))
                    .otherwise(col("EventDataDct.user")),

        "user_name": when(col("EventID._VALUE").isin(['4634', '4703', '4704', '4705', '4720', '4722', '4723', '4724', '4725', '4726', '4738', '4740', '4767', '4800', '4801']), col("EventDataDct.TargetUserName"))
                    .when(col("EventID._VALUE") == '4781', col("EventDataDct.NewTargetUserName"))
                    .when(col("EventID._VALUE").isin(['1102', '4719', '4698', '4700', '4701', '4702', '4799']), col("EventDataDct.SubjectUserName"))
                    .when(col("EventID._VALUE").isin(['4728', '4729', '4732', '4733', '4756', '4757']), 
                        when(col("EventDataDct.MemberSid").like("%\\%"), split(col("EventDataDct.MemberSid"), "\\\\").getItem(-1))
                        .otherwise(when(col("EventDataDct.MemberUserName").like("%\\%"), lit(None)).otherwise(col("EventDataDct.MemberUserName"))))
                    .otherwise(col("EventDataDct.UserName")),

        "src_user": when(col("EventID._VALUE").isin(['4624', '4625', '4703', '4704', '4705', '4720', '4722', '4723', '4724', '4725', '4726', '4727', '4728', '4729', '4730', '4731', '4733', '4734', '4735', '4737', '4738', '4754', '4755', '4756', '4757', '4758', '4764', '4767', '4781']), 
                        when(col("EventDataDct.SubjectUserName") != "-", col("EventDataDct.SubjectUserName")))
                    .when(col("EventID._VALUE").isin(['4634', '4800', '4801']), col("EventDataDct.TargetUserName"))
                    .otherwise(col("EventDataDct.src_user")),

        "src_user_name": when(col("EventID._VALUE").isin(['4703', '4704', '4705', '4720', '4722', '4723', '4724', '4725', '4726', '4727', '4728', '4729', '4730', '4731', '4732', '4733', '4734', '4735', '4737', '4738', '4740', '4754', '4755', '4756', '4757', '4758', '4764', '4767', '4781']), col("EventDataDct.SubjectUserName"))
                    .when(col("EventID._VALUE").isin(['4634', '4800', '4801']), col("EventDataDct.TargetUserName"))
                    .otherwise(col("EventDataDct.src_user_name")),

        "src_nt_domain": when(col("EventID._VALUE").isin(['4634', '4800', '4801']), col("EventDataDct.TargetDomainName"))
                    .when(col("EventID._VALUE").isin(['4727', '4728', '4729', '4730', '4731', '4733', '4734', '4735', '4737', '4754', '4755', '4756', '4757', '4758', '4764']), col("EventDataDct.SubjectDomainName"))
                    .otherwise(col("EventDataDct.src_nt_domain")),

        "object": when(col("EventID._VALUE").isin(['4634', '4703', '4704', '4705', '4720', '4722', '4723', '4724', '4725', '4726', '4727', '4730', '4731', '4734', '4735', '4737', '4738', '4740', '4754', '4755', '4758', '4764', '4767', '4799']), col("EventDataDct.TargetUserName"))
                    .when(col("EventID._VALUE") == '4781', col("EventDataDct.NewTargetUserName"))
                    .when(col("EventID._VALUE").isin(['4728', '4729', '4732', '4733', '4756', '4757']), 
                        when(col("EventDataDct.MemberSid").like("%\\%"), split(col("EventDataDct.MemberSid"), "\\\\").getItem(-1))
                        .otherwise(when(col("EventDataDct.member_user_name").like("%\\%"), None).otherwise(col("EventDataDct.member_user_name"))))
                    .when(col("EventID._VALUE").isin(['4800', '4801']), col("EventDataDct.Computer"))
                    .when(col("EventID._VALUE").isin(['4698', '4700', '4701', '4702']), col("EventDataDct.TaskName"))
                    .when(col("EventID._VALUE") == '1102', lit("audit log"))
                    .when(col("EventID._VALUE") == '4719', lit("Windows Security Audit Policy"))
                    .otherwise(col("EventDataDct.object")),

        "object_id": when(col("EventID._VALUE").isin(['4704', '4705', '4720', '4722', '4723', '4724', '4725', '4726', '4727', '4730', '4731', '4734', '4735', '4737', '4738', '4754', '4755', '4758', '4764', '4767', '4781', '4799']), col("EventDataDct.TargetSid"))
                    .when(col("EventID._VALUE").isin(['4634', '4703']), col("EventDataDct.TargetUserSid"))
                    .when(col("EventID._VALUE").isin(['4728', '4729', '4732', '4733', '4756', '4757']), col("EventDataDct.MemberSid"))
                    .otherwise(col("EventDataDct.object_id")),

        "action": when((col("EventID._VALUE") == '4688') & (col("EventDataDct.Keywords") == "0x8020000000000000"), "allowed")
                    .otherwise(col("EventDataDct.action")),

        "Group_Name": when((col("EventDataDct.TargetUserName").isNotNull()) & 
                        ((col("EventID._VALUE") >= '4727') & (col("EventID._VALUE") <= '4735') | 
                        (col("EventID._VALUE") == '4737') | 
                        (col("EventID._VALUE") >= '4744') & (col("EventID._VALUE") <= '4764') | 
                        (col("EventID._VALUE") == '4799')), col("EventDataDct.TargetUserName"))
                    .otherwise(col("EventDataDct.Group_Name")),

        "Group_Domain": when((col("EventDataDct.TargetDomainName").isNotNull()) & 
                        ((col("EventID._VALUE") >= '4727') & (col("EventID._VALUE") <= '4735') | 
                        (col("EventID._VALUE") == '4737') | 
                        (col("EventID._VALUE") >= '4744') & (col("EventID._VALUE") <= '4764') | 
                        (col("EventID._VALUE") == '4799')), col("EventDataDct.TargetDomainName"))
                    .otherwise(col("EventDataDct.Group_Domain")),

        "user_type": when(
                        (col("EventID._VALUE").isin(['4741', '4742', '4743'])), lit("computer")
                    ),

        "event_id": col("EventID._VALUE").cast("int"),
        "Level": col("Level").cast("int")
    }