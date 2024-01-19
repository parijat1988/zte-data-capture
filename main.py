import pandas as pd


if __name__ == '__main__':
    columns =['month','rent_revenue', 'cam_revenue', 'insurance_revenue','property_tax_revenue',
             'landscaping_contract','electricity_expense','gas_expense',
             'water_drainage_expense','water_sewer_expense','property_insurance_expense',
             'property_tax_expense','property_tax_consultant','management_fee_expense',
             'accounting_service_expense','legal_services','note_1_interest',
             ]
    df = pd.read_excel('Sample.xlsx', header=None )
    cleansed_df = df.iloc[4:, 2:-1].reset_index(drop=True)
    print(cleansed_df)
    sql_insert_statements = []
    for column, value in cleansed_df.items():
        values = []
        for row, value in value.items():
            if pd.notna(value):
                value_str = str(value) if not isinstance(value, str) else f"'{value}'"
                if row in [0,7,11,12,13,25,29,30,31,32,36,40,41,45,49,50,54]:
                    values.append(str(value_str))
        if values:
            sql_insert = f"INSERT INTO your_table_name ({', '.join(columns)}) VALUES ({', '.join(values)});"
        sql_insert_statements.append(sql_insert)

    for statement in sql_insert_statements:
        print(statement)







