from sqlalchemy import create_engine, text
import pandas as pd

def update_table_control(constr, df, from_date, to_date, object_for_update, chunk_size):
    table_name = object_for_update['table_name']
    list_field_name = object_for_update['field_name']

    if object_for_update and table_name and list_field_name:
        num_rows = len(df)

        # open connection
        engine = create_engine(constr)
        with engine.connect() as connection:

            for start in range(0, num_rows, chunk_size):
                df_chunk = df.iloc[start:start + chunk_size]

                values = " UNION ALL ".join([
                    "SELECT " + ", ".join([
                        f"'{row[field_name]}'" for field_name in list_field_name
                    ]) + " FROM DUAL \n"
                    for _, row in df_chunk.iterrows()
                ])

                on_conditions = " AND ".join([
                    f"target.keyid{i + 1} = temp.{field_name}" for i, field_name in enumerate(list_field_name)
                ])

                sql = text(f"""
                BEGIN
                    MERGE INTO etl.etlchangedata target
                    USING (
                        WITH temp_data ({", ".join(list_field_name)}) AS (
                            {values}
                        )
                        SELECT DISTINCT * FROM temp_data
                    ) temp
                    ON ({on_conditions})
                    WHEN MATCHED THEN
                        UPDATE SET target.uploaddate = SYSDATE,
                                   target.uploadstatus = 'S'
                        WHERE NVL(target.uploadstatus, 'XXX') != 'S'
                          AND target.changedate >= TO_DATE('{from_date}', 'YYYY-MM-DD')
                          AND target.changedate < TO_DATE('{to_date}', 'YYYY-MM-DD')
                          AND lower(target.tablename) = '{table_name}';

                    commit;
                END;
                """)

                transaction = connection.begin()
                try:
                    connection.execute(sql)
                    transaction.commit()
                #     print(f"Successfully updated rows {start} to {start + len(df_chunk) - 1}")

                except Exception as e:
                    print(f"An error occurred while updating: {e}")
                    transaction.rollback()
                    raise

        # close connection
        engine.dispose()

        df_chunk = pd.DataFrame()
        df = pd.DataFrame()