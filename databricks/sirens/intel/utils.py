def write_ignore_duplicates(spark, df, table):
    database, tbl = table.split('.')
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")
    if spark.sql(f'SHOW TABLES IN {database}').where(f'tableName = "{tbl}"').count() > 0:
        non_dupes = df.where(f'_raw_record NOT IN (select _raw_record from {table})')
        print(f'Writing {non_dupes.count()} records to {table} ...')
        non_dupes.write.mode('append').saveAsTable(table)
    else:
        print(f'Writing initial {df.count()} records to {table} ...')
        df.write.mode('append').saveAsTable(table)


def write_replace_where(spark, df, table, replace_where):
    database, tbl = table.split('.')
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")
    if spark.sql(f'SHOW TABLES IN {database}').where(f'tableName = "{tbl}"').count() > 0:
        (df.write
            .mode('overwrite')
            .option('replaceWhere', replace_where)
            .saveAsTable(table)
        )
    else:
        print(f'Writing initial {df.count()} records to {table} ...')
        df.write.mode('append').saveAsTable(table)
