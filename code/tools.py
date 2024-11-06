import glob
import numpy as np
from required_data_src import *


def define_used_region(df):
    # 判断是不是apac 员工
    flag = False
    if df['PAID']:
        if str(df["PAID"]).startswith('CN'):
            flag = True
    if df['MacroEntity'] in ['ZONE ASIA PACIFIC NORTH', 'BU CHINA', 'ZONE ASIA PACIFIC','ZONE ASIA PACIFIC HQ','CHINA HQ']:
        flag = True
    return flag


class MovementManager:

    def __init__(self, userid, onboard_date, current_band, end_date: str, position_name, entity, paid):
        self.userid = userid
        self.onboard_date = onboard_date
        self.end_date = end_date
        self.current_band = current_band
        self.position_name = position_name
        self.entity = entity
        self.paid = paid
        self.new_col = {}

    def get_promo_info(self):
        # 升级
        move_up_data = movement_po_band[movement_po_band['ID'] == self.userid].query(
            f'("{self.onboard_date}" < EffectiveDate) & (EffectiveDate <= "{self.end_date}")')
        move_up_data = move_up_data[move_up_data['ReasonofChange'].isin(['Promotion Band Up', 'Promotion within band'])]
        if len(move_up_data) == 0:
            move_up_times = 0
            days_since_recent_move_up = None
        else:
            move_up_times = len(move_up_data)
            days_since_recent_move_up = (pd.to_datetime(self.end_date) - move_up_data['EffectiveDate'].max()).days
        self.new_col['move_up_times'] = move_up_times
        self.new_col['days_since_recent_move_up'] = days_since_recent_move_up

    def get_demotion_info(self):
        # 降级
        move_down_data = movement_po_band[movement_po_band['ID'] == self.userid].query(
            f'("{self.onboard_date}" < EffectiveDate) & (EffectiveDate <= "{self.end_date}")')
        move_down_data = move_down_data.query("ReasonofChange == 'Demotion'")
        if len(move_down_data) == 0:
            move_down_times = 0
            days_since_recent_demotion = None
        else:
            move_down_times = len(move_down_data)
            days_since_recent_demotion = (pd.to_datetime(self.end_date) - move_down_data['EffectiveDate'].max()).days
        self.new_col['move_down_times'] = move_down_times
        self.new_col['days_since_recent_demotion'] = days_since_recent_demotion

    def get_other_move_info(self):
        move_data = movement_po_band[movement_po_band['ID'] == self.userid].query(
            f'("{self.onboard_date}" < EffectiveDate) & (EffectiveDate <= "{self.end_date}")')
        move_data = move_data[
            move_data['ReasonofChange'].isin(['Lateral Move', 'Internal Restructuring', 'Reverse Grandfathering',
                                              'Temporary Transfer', 'Job Rotation', 'Re-designation'])]
        if len(move_data) == 0:
            other_move_times = 0
            days_since_recent_other_move = None
        else:
            other_move_times = len(move_data)
            days_since_recent_other_move = (pd.to_datetime(self.end_date) - move_data['EffectiveDate'].max()).days
        self.new_col['other_move_times'] = other_move_times
        self.new_col['days_since_recent_other_move'] = days_since_recent_other_move

    def get_end_date_band(self):
        user_move = movement_src[movement_src['ID'] == self.userid].query(f'(EffectiveDate <= "{self.end_date}")')
        if len(user_move) > 0:
            end_of_date_band = user_move.sort_values(by='EffectiveDate', ascending=False)['BAND'].tolist()[0]
            end_of_date_band = roam_to_num_dict[end_of_date_band]
        else:
            end_of_date_band = self.current_band
        self.new_col['end_of_date_band'] = end_of_date_band

    def get_end_date_position_name(self):
        user_move = movement_src[movement_src['ID'] == self.userid].query(f'(EffectiveDate <= "{self.end_date}")')
        if len(user_move) > 0:
            end_of_date_position = user_move.sort_values(by='EffectiveDate', ascending=False)['PositionName'].tolist()[
                0]
        else:
            end_of_date_position = self.position_name
        self.new_col['end_of_date_position'] = end_of_date_position

    def get_end_date_entity(self):
        user_move = movement_src[movement_src['ID'] == self.userid].query(f'(EffectiveDate <= "{self.end_date}")')
        user_move = user_move[user_move['MacroEntity'].notnull()]
        if len(user_move) > 0:
            end_of_date_entity = user_move.sort_values(by='EffectiveDate', ascending=False)['MacroEntity'].tolist()[0]
        else:
            end_of_date_entity = self.entity
        self.new_col['end_of_date_MacroEntity'] = end_of_date_entity

    def get_end_date_paid(self):
        user_move = movement_src[movement_src['ID'] == self.userid].query(f'(EffectiveDate <= "{self.end_date}")')
        user_move = user_move[
            user_move['BusinessType'].isin(['change po&band', 'change po', 'Move to Assignee Organization (Expats)'])]
        if len(user_move) > 0:
            end_of_date_paid = user_move.sort_values(by='EffectiveDate', ascending=False)['COCD'].tolist()[0]
        else:
            end_of_date_paid = self.paid
        self.new_col['end_of_date_paid'] = end_of_date_paid

    def get_movement_cols(self):
        self.get_promo_info()
        self.get_demotion_info()
        self.get_other_move_info()
        self.get_end_date_band()
        self.get_end_date_position_name()
        self.get_end_date_entity()
        self.get_end_date_paid()
        return self.new_col


def get_one_fte_snapshot(fte):
    # 获取movement的标签
    final_fte = []
    for i, item in fte.iterrows():
        exist_items = dict(item)
        userid = exist_items['ID']
        current_band = exist_items['current_band']
        onboard_date = exist_items['OnboardDate']
        end_date = exist_items['current_year']
        position_name = exist_items['positionName']
        entity = exist_items['MacroEntity']
        paid = exist_items['PAID']
        movementFeatures = MovementManager(userid, onboard_date, current_band, end_date, position_name, entity, paid)
        movement_features = movementFeatures.get_movement_cols()
        exist_items.update(movement_features)
        final_fte.append(exist_items)
    final_fte = pd.DataFrame(final_fte)
    return final_fte


def get_salary_change(userid, current_date):
    salary_features = {}
    salary_data = cr_src[(cr_src['ID'] == userid) & (current_date > cr_src['Hire Date'])]
    if len(salary_data) > 0:
        recent_cr = salary_data.sort_values(by='Hire Date', ascending=False)['Compa Ratio'].tolist()[0]  # 最近薪资
        recent_cr_change_time = salary_data['Hire Date'].max()

        try:
            last_cr = salary_data.sort_values(by='Hire Date', ascending=False)['Compa Ratio'].tolist()[1]  # 再之前的薪资
            recent_cr_diff = recent_cr - last_cr
        except:
            last_cr = None
            recent_cr_diff = None

        salary_features['recent_cr'] = recent_cr
        salary_features['days_since_recent_cr_change'] = (pd.to_datetime(current_date) - recent_cr_change_time).days
        salary_features['cr_change_times'] = salary_data.shape[0]
        salary_features['recent_cr_diff'] = recent_cr_diff
    return salary_features


def get_salary_features(fte):
    # 获取salary的标签
    final_data = []
    for i, item in fte.iterrows():
        exist_items = dict(item)
        userid = exist_items['ID']
        end_date = exist_items['current_year']
        salary_features = get_salary_change(userid, end_date)
        exist_items.update(salary_features)
        final_data.append(exist_items)
    final_data = pd.DataFrame(final_data)
    final_data['cr_change_vs_term'] = final_data['cr_change_times'] / final_data['on_duty_days'] * 365
    return final_data


def get_org_features(train_data):
    train_data = pd.merge(train_data, org_data, how='left', on=['ID', 'year'])
    # 根据最近的org数据去补充那些空值
    missing_org_data = train_data[train_data['Line Manager ID'].isnull()]
    non_missing_org_data = train_data[train_data['Line Manager ID'].notnull()].sort_values(by='year', ascending=False)
    non_missing_org_unique_id = non_missing_org_data.drop_duplicates(subset=['ID'], keep='first')  # 保留最近的记录
    non_missing_org_unique_id = non_missing_org_unique_id[org_data_use_columns]
    missing_org_data.drop(set(org_data_use_columns) - set({'ID'}), axis=1, inplace=True)
    missing_org_data = pd.merge(missing_org_data, non_missing_org_unique_id, on='ID', how='left')
    return pd.concat([missing_org_data, non_missing_org_data])


def add_more_features(data_combined):
    # 当前年份，engagement数据, salary信息，组织架构信息
    data_combined['year'] = data_combined['current_year'].astype(str).str[:4].astype(int)
    train_data = get_salary_features(data_combined)
    train_data = get_org_features(train_data)
    train_data['Line Manager ID'] = train_data['Line Manager ID'].apply(split_id)
    engagement.rename(columns={'ID': 'Line Manager ID'}, inplace=True)
    train_data = pd.merge(train_data, engagement, on=['Line Manager ID', 'year'], how='left')  # engagement数据的ID是老板的ID
    return train_data


def find_org(org_data, userid, year):
    org_query = org_data[(org_data['ID'] == userid) & (org_data['year'] <= year)]
    if len(org_query) > 0:
        org_result = dict(org_query.iloc[0, :][org_data_use_columns])
    else:
        org_result = {'Organizational Unit': None,
                      'Line Manager ID': None,
                      'Job Family': None,
                      '员工子组(OM)': None,
                      '工作地 ID(OM)': None,
                      'Functional Area': None
                      }
    return org_result
#
#
def get_org_features_forecast(df, org_data):
    org_data = org_data.sort_values(by=['ID', 'year'], ascending=False)
    final_data = []
    for i, rows in df.iterrows():
        exist_items = dict(rows)
        userid = exist_items['ID']
        year = exist_items['year']
        org_features = find_org(org_data, userid, year)
        exist_items.update(org_features)
        final_data.append(exist_items)
    final_data = pd.DataFrame(final_data)
    return final_data

def find_engagement_index(engagement, manager_id, year):
    engagement = engagement.sort_values(by=['Line Manager ID', 'year'], ascending=False)
    engagement_query = engagement[(engagement['Line Manager ID'] == manager_id) & (engagement['year'] <= year)]
    if len(engagement_query) > 0:
        engagement_result = dict(engagement_query.iloc[0, :][['Employee Engagement Index','Manager Effectiveness Index']])
    else:
        engagement_result = {
            'Employee Engagement Index': None,
            'Manager Effectiveness Index': None
        }
    return engagement_result


def get_engagement_features(df, engagement):
    engagement.rename(columns={'ID': 'Line Manager ID'}, inplace=True)
    final_data = []
    for i, rows in df.iterrows():
        exist_items = dict(rows)
        manager_id = exist_items['Line Manager ID']
        year = exist_items['year']
        engagement_features = find_engagement_index(engagement, manager_id, year)
        exist_items.update(engagement_features)
        final_data.append(exist_items)
    final_data = pd.DataFrame(final_data)
    return final_data
#
#
def add_more_features_forecast(data_combined):
    # 当前年份，engagement数据, salary信息，组织架构信息
    data_combined['year'] = data_combined['current_year'].astype(str).str[:4].astype(int)
    train_data = get_salary_features(data_combined)
    train_data = get_org_features_forecast(train_data, org_data)
    train_data['Line Manager ID'] = train_data['Line Manager ID'].apply(split_id)
    train_data = get_engagement_features(train_data, engagement)
#     train_data = pd.merge(train_data, engagement, on=['Line Manager ID', 'year'], how='left')  # engagement数据的ID是老板的ID
    return train_data


def split_id(x):
    if np.isnan(x):
        return None
    else:
        return "-".join([str(x).split('.')[0], str(x).split('.')[1][:6]])

#### 2023-12-26补engagement,CR数据,加变量 days in position
def get_end_date_days_in_position(movement_src_copy, userid, end_date, onboard_date, current_band):
    # 补充在这个职位上/band 上的时间（年份）
    user_move = movement_src_copy[movement_src_copy['ID'] == userid].query(f'(EffectiveDate <= "{end_date}")')
    effective_date = user_move['EffectiveDate'].max() if len(user_move) > 0 else onboard_date   # 当前position的生效时间
    same_band_data = user_move.query("BAND == @current_band")
    same_band_start_date = same_band_data['EffectiveDate'].min()
    return {
        'time_in_position': (pd.to_datetime(end_date) - effective_date).days / 365,
        'time_in_band':(pd.to_datetime(end_date) - same_band_start_date).days / 365
    }


def get_days_in_position(train_data2, movement_src):
    final_train = []
    for i, item in train_data2.iterrows():
        exist_items = dict(item)
        userid = exist_items['ID']
        onboard_date = exist_items['OnboardDate']
        end_date = exist_items['current_year']
        current_band = exist_items['end_of_date_band']
        new_cols_dict = get_end_date_days_in_position(movement_src, userid, end_date, onboard_date, current_band)
        exist_items.update(new_cols_dict)
        final_train.append(exist_items)
    final_train = pd.DataFrame(final_train)
    return final_train
#### 12.27补engagement index 没有的话用line manager的line manager

def fill_engagement_blank(train_data, line_manager_id, that_year, e_e_index, m_e_index, org_unit):
    if not e_e_index or np.isnan(e_e_index):
        line_manager_data = train_data[train_data['ID'] == line_manager_id].sort_values(by='year', ascending=False)
        line_manager_data = line_manager_data[line_manager_data['Employee Engagement Index'].notnull() &
                                              (~np.isnan(line_manager_data['Employee Engagement Index']))]
        that_year_data = line_manager_data.query("year == @that_year")
        if len(line_manager_data) > 0:
            if len(that_year_data) > 0:
                e_e_index = that_year_data['Employee Engagement Index'].iloc[0]
                m_e_index = that_year_data['Manager Effectiveness Index'].iloc[0]
            else:
                e_e_index = line_manager_data['Employee Engagement Index'].iloc[0]
                m_e_index = line_manager_data['Manager Effectiveness Index'].iloc[0]
        else:
            org_unit_data = train_data[train_data['Organizational Unit'] == org_unit].sort_values(by='year',
                                                                                                  ascending=False)
            org_unit_data = org_unit_data[org_unit_data['Employee Engagement Index'].notnull() &
                                          (~np.isnan(org_unit_data['Employee Engagement Index']))]
            that_year_data = org_unit_data.query("year == @that_year")
            if len(org_unit_data) > 0:
                if len(that_year_data) > 0:
                    e_e_index = that_year_data['Employee Engagement Index'].iloc[0]
                    m_e_index = that_year_data['Manager Effectiveness Index'].iloc[0]
                else:
                    e_e_index = org_unit_data['Employee Engagement Index'].iloc[0]
                    m_e_index = org_unit_data['Manager Effectiveness Index'].iloc[0]

    return {
        'Employee Engagement Index': e_e_index,
        'Manager Effectiveness Index': m_e_index
    }


def fill_in_engagement_index_blank(train_data2, whole_data):
    """
    :param train_data2:当前正在处理的模型数据
    :param whole_data: 全量数据，用于查找line manager ID
    :return:
    """
    final_train = []
    for i, item in train_data2.iterrows():
        exist_items = dict(item)
        line_manager_id = exist_items['Line Manager ID']
        ee_index = exist_items['Employee Engagement Index']
        me_index = exist_items['Manager Effectiveness Index']
        org_unit = exist_items['Organizational Unit']
        that_year = exist_items['year']
        new_cols_dict = fill_engagement_blank(whole_data, line_manager_id, that_year, ee_index, me_index, org_unit)
        exist_items.update(new_cols_dict)
        final_train.append(exist_items)
    final_train = pd.DataFrame(final_train)
    return final_train


def add_combo_features(train_data):
    # 12.26应Sharon需要，加一些组合两个变量的features
    train_data['promo_freq_vs_tenure'] = train_data['move_up_times'] / train_data['on_duty_days']
    train_data['promo_days_vs_tenure'] = train_data['days_since_recent_move_up'] / train_data['on_duty_days']
    train_data = train_data.query('on_duty_days > 0')
    train_data['cr_diff_vs_time_in_band'] = train_data['recent_cr_diff'] / train_data['time_in_band']
    return train_data
