import pandas as pd
import glob
import re


used_columns_files = '数据逻辑梳理.xlsx'
item_def = pd.read_excel(used_columns_files)
# 职位变动数据
movement_src = pd.read_excel("data/PROCUREMENT_China_23 Nov.xlsx", sheet_name='Movement')
engagement = pd.read_excel("data/engagement_data.xlsx")  # ID 有重复

roam_to_num = pd.read_excel('数据逻辑梳理.xlsx', sheet_name='rome_to_num')    # 罗马数字转数字
roam_to_num = roam_to_num.set_index('band')
roam_to_num_dict = dict(roam_to_num.to_records())
fte = pd.read_excel('data/Turnover Report_Procurement_202310 APAC.xlsx', sheet_name='FTE') # 截止到2023年10月底的在职员工数据
org_data_list = glob.glob("data/org_data/*.xlsx")
org_data_use_columns = ['Organizational Unit', 'ID', 'Line Manager ID', 'Job Family', '员工子组(OM)', '工作地 ID(OM)', 'Functional Area']

# 转换ID
movement_src['ID'] = movement_src.apply(lambda x: "-".join([str(x['ID1']), str(x['ID2'])[:6]]), axis=1)
engagement['ID'] = engagement['ID'].astype(str).str.split('.').apply(
            lambda x: "-".join([str(list(x)[0]), str(list(x)[1])[:6]]))
fte['ID'] = fte['ID'].astype(str).str.split('.').apply(
            lambda x: "-".join([str(list(x)[0]), str(list(x)[1])[:6]]))

movement_po_band = movement_src.copy()
movement_po_band = movement_po_band.query("EmployeeStatus == 'Active'")
# movement_po_band["BusinessType"].unique()
movement_po_band = movement_po_band[movement_po_band['BusinessType'].isin(['change po', 'change band','change po&band','Change Po'])]

# #薪水（相对薪水）
def get_cr_src(runtime=1):
    if runtime == 1:
        cr_src = pd.DataFrame()
        for year in [2018, 2019, 2020, 2021, 2022, 2023]:
            cr_src_yr = pd.read_excel("data/PROCUREMENT_China_23 Nov.xlsx", sheet_name=f"{year}")
            cr_src_yr['year'] = year
            cr_src = pd.concat([cr_src, cr_src_yr])
        cr_src['ID'] = cr_src['ID'].astype(str).str.split('.').apply(
            lambda x: "-".join([str(list(x)[0]), str(list(x)[1])[:6]]))
        cr_src.to_excel('step_data/cr_src.xlsx', index=False)
    else:
        cr_src = pd.read_excel('step_data/cr_src.xlsx')
    return cr_src


def get_org_data(runtime=1):
    if runtime == 1:
        org_data = pd.DataFrame()
        for file in org_data_list:
            print(file)
            year = int(re.split(r'/|\\', file)[2][:4])
            data = pd.read_excel(file)
            data = data[org_data_use_columns]
            data['year'] = year
            org_data = pd.concat([org_data, data])
        org_data['ID'] = org_data['ID'].astype(str).str.split('.').apply(
            lambda x: "-".join([str(list(x)[0]), str(list(x)[1])[:6]]))
        org_data.to_excel('step_data/org_data.xlsx', index=False)
    else:
        org_data = pd.read_excel('step_data/org_data.xlsx')
    return org_data


def get_turnover_data(runtime=1):
    if runtime == 1:
        # 拼离职数据
        turnover_items = item_def['DB_TO_FTE_LIST'].tolist()
        turnover_files = glob.glob('data/Turnover*')
        turnover_data = pd.DataFrame()
        for i,file in enumerate(turnover_files):
            print(file)
            data = pd.read_excel(file, sheet_name='DB_TO_FTE_LIST')
            data = data[turnover_items]
            data['file_id'] = i
            turnover_data = pd.concat([turnover_data, data])
        turnover_data['ID'] = turnover_data['ID'].astype(str).str.split('.').apply(
            lambda x: "-".join([str(list(x)[0]), str(list(x)[1])[:6]]))
        turnover_data.to_excel('step_data/turnover_data.xlsx', index=False)
    else:
        turnover_data = pd.read_excel('step_data/turnover_data.xlsx')
    return turnover_data


cr_src = get_cr_src(runtime=1)
org_data = get_org_data(runtime=1)
turnover_data = get_turnover_data(runtime=1)