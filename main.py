import pandas as pd

if __name__ == '__main__':

    columns =['month','rent_revenue', 'cam_revenue', 'insurance_revenue','property_tax_revenue',
             'landscaping_contract','electricity_expense','gas_expense',
             'water_drainage_expense','water_sewer_expense','property_insurance_expense',
             'property_tax_expense','property_tax_consultant','management_fee_expense',
             'accounting_service_expense','legal_services','note_1_interest',
             ]
    df_to_load = pd.DataFrame(columns=columns)
    rows_df =[]
    pd.set_option('display.max_columns', None)
    df = pd.read_excel('Sample.xlsx', header=None )
    print(df.head(20))
    cleansed_df = df.iloc[4:, 2:-1].reset_index(drop=True)
    print("--------print(cleansed_df--------")
    print(cleansed_df.head(20))
    sql_insert_statements = []
    for column, value in cleansed_df.items():
        values = []
        for row, value in value.items():
            if pd.notna(value):
                value_str = str(value) if not isinstance(value, str) else f"'{value}'"
                if row in [0,7,11,12,13,25,29,30,31,32,36,40,41,45,49,50,54]:
                    values.append(str(value_str))
        rows_df.append(values)
    #print(rows_df)
    df = pd.DataFrame(rows_df, columns=columns)
    print(df)









