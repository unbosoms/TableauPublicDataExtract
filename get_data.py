import requests
import pandas as pd
import datetime

# 取得するauthorの情報を指定
profile_name = 'yuta1985'

# profile_nameのすべてのworkbookのworkbookRepoUrlを取得する
workbookRepoUrl_list = []

# 初期値の設定
start = 0 # 0番目から読み込み開始
count = 50 # 50個ずつ読み込んでいく

# workbookRepoUrlの一覧を取得する
while(1):
    print(start)
    url = f'https://public.tableau.com/public/apis/workbooks?profileName={profile_name}&start={start}&count={count}&visibility=NON_HIDDEN'
    result = requests.get(url=url)
    json_data = result.json()
    
    # workbookRepoUrlを取得する
    for content in json_data['contents']:
        workbookRepoUrl_list.append(content['workbookRepoUrl'])
    
    # 次の開始番号を取得する
    next_num = json_data['next']
    
    # 次の番号がない場合はループを抜ける
    if next_num == None:
        break
    # 次の開始番号を記録して繰り返す
    else:
        start = next_num

# workbookのdetailを取得する
workbook_details = []
for workbookRepoUrl in workbookRepoUrl_list:
    url = f'https://public.tableau.com/profile/api/single_workbook/{workbookRepoUrl}?'
    result = requests.get(url=url)
    json = result.json()
    workbook_details.append(json)

t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, 'JST')
now = datetime.datetime.now(JST) 
d = now.strftime('%Y%m%d_%H%M%S')
df = pd.json_normalize(workbook_details)
df['get date'] = d
df.to_csv(f'./data/{d}_data.csv')