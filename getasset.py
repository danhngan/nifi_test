import pandas as pd
import requests 
import time
url = 'https://www.stockbiz.vn/Stocks/{symbol}/FinancialStatements.aspx'
payload='Cart_ctl00_webPartManager_wp603001723_wp866410259_cbFinanceReport_Callback_Param=0&Cart_ctl00_webPartManager_wp603001723_wp866410259_cbFinanceReport_Callback_Param=0&Cart_ctl00_webPartManager_wp603001723_wp866410259_cbFinanceReport_Callback_Param=1000000'
comp_list = pd.read_csv('comp_list.csv')
for i in range(0,comp_list.shape[0]-40, 20):
    df_total = pd.DataFrame(columns=['Loai tai san', 'Q3 2021', 'Q4 2021', 'Q1 2022', 'Q2 2022', 'Q3 2022'])
    for symbol in comp_list['Symbol'][i+40:i+60]:
        print(symbol)
        for n_try in range(3):
            a = requests.post(url=url.format(symbol=symbol), params=payload)
            if a.status_code == 200:
                data = a.text[a.text.find('<table'):a.text.find(']]></Callback')]
                try:
                    df = pd.read_html(data, encoding='utf-8', header=0)
                    if len(df) > 0 and df[0].shape[0]>1:
                        df = df[0]
                        df.to_csv(f'{symbol}.csv', index=False)
                        df_equity = df[df['Unnamed: 0'] == 'TỔNG CỘNG TÀI SẢN']
                        for quy in ['Q3 2021', 'Q4 2021', 'Q1 2022', 'Q2 2022', 'Q3 2022']:
                            if quy in df_equity:
                                df_total.loc[symbol, quy] = df_equity.loc[df_equity.index[0], quy]
                                break
                except:
                    time.sleep((n_try)*20+10)
            else:
                time.sleep((n_try**2)*20+10)
                            
        time.sleep(30)
    df_total.to_csv(f'From_{comp_list["Symbol"][i]}.csv', index=True)
    time.sleep(300)
                            
                            
                             
