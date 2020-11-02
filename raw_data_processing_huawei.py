import pandas as pd


def input_format(input_path):
    df = pd.read_csv(input_path)
    df['Result Time'] = pd.to_datetime(df['Result Time']).dt.strftime('%d-%m-%Y %H:%M')
    df['Object Name'] = df['Object Name'].str.replace('/Cell:', '/GCELL:', regex=False)
    df['BSC'] = df['Object Name'].str.split('/', expand=True)[0]
    df['Cell Name'] = df['Object Name'].str.split(',', expand=True)[0].str.split('=', expand=True)[1]
    return df


df1 = input_format('./huawei/input/GDC_NPM_2G_Hourly_1.csv')
df2 = input_format('./huawei/input/GDC_NPM_2G_Hourly_2.csv')
df3 = input_format('./huawei/input/GDC_NPM_2G_Hourly_3.csv')
df3['TRX_Raw'] = df3['Object Name'].str.split('/', expand=True)[2]
df3['Object Name'] = (df3['Object Name'].str.split('/', expand=True)[0]+'/'+df3['Object Name'].str.split('/', expand=True)[1])[0]
df4 = input_format('./huawei/input/GDC_NPM_2G_Hourly_4.csv')

cols = []

for col in df1.columns:
    if col in df2.columns and col in df3.columns and col in df4.columns:
        cols.append(col)

res1 = pd.merge(df1, pd.merge(df2, pd.merge(df3, df4, how='outer', on=cols), how='outer', on=cols), how='outer', on=cols).fillna(0)

df5 = pd.read_csv('./huawei/input/SiteDB_2G_CH_17-08-2020.csv')
df5 = df5[['OSS_Cell_ID-Name', 'HQ_TOWN', 'Net_TRXs_Available', 'Total_TS_Available', 'BCCH', 'NOOFSDCCH', 'FPDCH', 'TCH_Available_For_Voice', 'Equipped_Erlang_Capacity_forVoice', 'SitePreference', 'REGION', 'CI-Name', 'Active_1800_TRX', 'Vendor', 'roaming_inroaming', 'Acceptance_Status']]
df5 = df5.rename(columns = {'OSS_Cell_ID-Name' : 'Cell Name'})

res2 = pd.merge(res1, df5, how='left', on=['Cell Name']).fillna(0)

res2.to_csv('./huawei/output/gdc_npm_2g_hourly_huawei.csv', index=False)
