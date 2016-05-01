#coding=utf8

import numpy as np
import pandas as pd


##################################################################################
# 征信报告信用卡明细信息.csv
columns = u'客户id 编号 卡类型 担保方式 币种 开户日期 信用额度 共享授信额度 最大负债额 透支余额已使用额度 账户状态 本月应还款金额 本月实际还款金额 最近一次实际还款日期 当前逾期期数 当前逾期总额 准贷记卡透支180天以上未付余额 贷记卡12个月内未还最低还款额次数 信息获取时间'.split()

creditc_info = pd.read_csv(ur'Data/征信报告信用卡明细信息-utf8.csv', 
        index_col=False, names=columns, delimiter='\t', encoding='utf8', low_memory=False)
# 在excel中发现，做的处理：
# 除客户id、编号、币种、账户状态之外的所有值都为空的行：删除，
# 只要卡类型和担保方式为空，其他的就都为空了
# c = creditc_info.dropna(axis=0, how='all', subset=[u'卡类型', u'担保方式'])
# #row from 127809 to 88819
# 卡类型和担保方式为'——'，其他列都无意义，删除
# #row to 88435
# 卡类型为'——'，删除
# #row to 87888
# 信息获取时间为'——'，删除
# #row to 87306
# 担保方式为'——'，删除
# #row to 87302
# 信用额度是日期20140607之类的，删除
# #row to 87292
# 共享信用额度是日期，删除，仅1行
# 最大负债为''，删除
# #row to 87155
# 本月应还款金额为'',删除，仅3行
# 本云实际还款金额为''，删除，仅1行
# 当前逾期期数为'——'，删除，斤1行
# 当前逾期总额为''，1行
# 准贷记卡透支180天以上为''或'——'，太多，不考虑此项
# 贷记卡12月内。。。为'——'，约800行，设为0
# 保存在h:\share\dev\default_prediction\征信报告信用卡明细信息-sorted-dropedall.csv中，共87149行
creditc_info.drop(u'准贷记卡透支180天以上未付余额', axis=1, inplace=True)
creditc_info.replace({
    u'卡类型':{'--': np.nan}, 
    u'担保方式': {'--': np.nan}
    }, inplace=True)
creditc_info.dropna(axis='rows', subset=[u'卡类型', u'担保方式'], inplace=True)
# 87405
creditc_info.replace({ u'信息获取时间':{'--': np.nan}, }, inplace=True)
creditc_info.dropna(axis='rows', subset=[u'信息获取时间'], inplace=True)
# 87301
creditc_info = creditc_info[creditc_info[u'信用额度']<20010000]
creditc_info = creditc_info[creditc_info[u'共享授信额度']<20010000]
creditc_info = creditc_info[creditc_info[u'本月应还款金额']<20010000]
creditc_info = creditc_info[creditc_info[u'本月实际还款金额']<20010000]
creditc_info[u'编号'] = creditc_info[u'编号'].astype(int)
creditc_info = creditc_info[creditc_info[u'编号']<20010000]
creditc_info.dropna(axis='rows', subset=[u'最大负债额'], inplace=True)
# 87156
creditc_info.dropna(axis='rows', subset=[u'本月应还款金额', u'本月实际还款金额', u'当前逾期总额'], inplace=True)
creditc_info.replace({u'币种': {'--': u'人民币'}, u'当前逾期期数': {'--': 0}}, inplace=True)
creditc_info.replace({u'准贷记卡透支180天以上未付余额': {'--': 0}}, inplace=True)
creditc_info.replace({u'贷记卡12个月内未还最低还款额次数': {'--': 0}}, inplace=True)
creditc_info.dropna(how='any', inplace=True)
creditc_info[u'开户日期'] = creditc_info[u'开户日期'].str.slice(0, 10).str.replace('.', '').str.replace('-', '').astype(int)
creditc_info[u'最近一次实际还款日期'] = creditc_info[u'最近一次实际还款日期'].str.slice(0, 10).str.replace('.', '').str.replace('-', '').astype(int)
creditc_info[u'信息获取时间'] = creditc_info[u'信息获取时间'].str.slice(0, 10).str.replace('.', '').str.replace('-', '').astype(int)
creditc_info.to_csv(ur'Data/征信报告信用卡明细信息-1.csv', index=None, encoding='utf8')
creditc_info = pd.read_csv(ur'Data/征信报告信用卡明细信息-1.csv', index_col=None, encoding='utf8')


# 5.1讨论方法
# 每个人都有多个“批次”，只取每个人最近的一个批次
groups = {}
for row in creditc_info.itertuples():
    customer_id = row[1]
    if customer_id in groups:
        groups[customer_id].append(row)
    else:
        groups[customer_id] = [row]

groups_1bach = {}
save = np.zeros(creditc_info.shape[0])
for key in groups:
    query_list = groups[key]
    # 分成批次
    baches = []
    one_bach = []
    last_no = query_list[0][2]
    for query in query_list:
        current_no = query[2]
        if current_no < last_no:
            # 另一批数据
            baches.append(one_bach)
            one_bach = [query]
        else:
            # 同一批数据
            one_bach.append(query)
        last_no = current_no
    baches.append(one_bach)
    # 在各批次中找查询时间最近的
    latest_bach = []
    latest_time = 0
    for bach in baches:
        s = 0
        for q in bach:
            date = str(q[-1])
            s += int(date[:4])*365 + int(date[4:6])*31 + int(date[6:])
        avg = s / len(bach)
        if avg > latest_time:
            latest_time = avg
            latest_bach = bach
    for q in latest_bach:
        save[q[0]] = 1
    groups_1bach[key] = latest_bach

unique_c_info = creditc_info[save.astype(bool)]

# 统计信息
SUMMARY = pd.DataFrame()
gd = unique_c_info.groupby(u'客户id')
SUMMARY[u'开户日期-max'] = gd[u'开户日期'].max()
SUMMARY[u'开户日期-min'] = gd[u'开户日期'].min()
SUMMARY[u'信用额度-sum'] = gd[u'信用额度'].sum()
SUMMARY[u'信用额度-max'] = gd[u'信用额度'].max()
SUMMARY[u'信用额度-min'] = gd[u'信用额度'].min()
SUMMARY[u'信用额度-var'] = gd[u'信用额度'].var()
SUMMARY[u'共享授信额度-sum'] = gd[u'共享授信额度'].sum()
SUMMARY[u'共享授信额度-max'] = gd[u'共享授信额度'].max()
SUMMARY[u'共享授信额度-min'] = gd[u'共享授信额度'].min()
SUMMARY[u'共享授信额度-var'] = gd[u'共享授信额度'].var()
SUMMARY[u'最大负债额-sum'] = gd[u'最大负债额'].sum()
SUMMARY[u'最大负债额-max'] = gd[u'最大负债额'].max()
SUMMARY[u'最大负债额-min'] = gd[u'最大负债额'].min()
SUMMARY[u'最大负债额-var'] = gd[u'最大负债额'].var()
SUMMARY[u'透支余额已使用额度-sum'] = gd[u'透支余额已使用额度'].sum()
SUMMARY[u'透支余额已使用额度-max'] = gd[u'透支余额已使用额度'].max()
SUMMARY[u'透支余额已使用额度-min'] = gd[u'透支余额已使用额度'].min()
SUMMARY[u'透支余额已使用额度-var'] = gd[u'透支余额已使用额度'].var()
# 账户状态
statuses = u'销户 正常 未激活 冻结 呆帐 止付'.split()
SUMMARY[u'账户状态-销户'] = gd[u'账户状态'].agg(lambda x: (x==u'销户').sum())
SUMMARY[u'账户状态-正常'] = gd[u'账户状态'].agg(lambda x: (x==u'正常').sum())
SUMMARY[u'账户状态-未激活'] = gd[u'账户状态'].agg(lambda x: (x==u'未激活').sum())
SUMMARY[u'账户状态-冻结'] = gd[u'账户状态'].agg(lambda x: (x==u'冻结').sum())
SUMMARY[u'账户状态-呆账'] = gd[u'账户状态'].agg(lambda x: (x==u'呆账').sum())
SUMMARY[u'账户状态-止付'] = gd[u'账户状态'].agg(lambda x: (x==u'止付').sum())


SUMMARY[u'本月应还款金额-sum'] = gd[u'本月应还款金额'].sum()
SUMMARY[u'本月应还款金额-max'] = gd[u'本月应还款金额'].max()
SUMMARY[u'本月应还款金额-min'] = gd[u'本月应还款金额'].min()
SUMMARY[u'本月应还款金额-var'] = gd[u'本月应还款金额'].var()
SUMMARY[u'本月实际还款金额-sum'] = gd[u'本月实际还款金额'].sum()
SUMMARY[u'本月实际还款金额-max'] = gd[u'本月实际还款金额'].max()
SUMMARY[u'本月实际还款金额-min'] = gd[u'本月实际还款金额'].min()
SUMMARY[u'本月实际还款金额-var'] = gd[u'本月实际还款金额'].var()

SUMMARY[u'当前逾期期数-sum'] = gd[u'当前逾期期数'].sum()
SUMMARY[u'当前逾期期数-max'] = gd[u'当前逾期期数'].max()
SUMMARY[u'当前逾期期数-min'] = gd[u'当前逾期期数'].min()
SUMMARY[u'当前逾期期数-var'] = gd[u'当前逾期期数'].var()
SUMMARY[u'当前逾期总额-sum'] = gd[u'当前逾期总额'].sum()
SUMMARY[u'当前逾期总额-max'] = gd[u'当前逾期总额'].max()
SUMMARY[u'当前逾期总额-min'] = gd[u'当前逾期总额'].min()
SUMMARY[u'当前逾期总额-var'] = gd[u'当前逾期总额'].var()
SUMMARY[u'贷记卡12个月内未还最低还款额次数-sum'] = gd[u'贷记卡12个月内未还最低还款额次数'].sum()
SUMMARY[u'贷记卡12个月内未还最低还款额次数-max'] = gd[u'贷记卡12个月内未还最低还款额次数'].max()
SUMMARY[u'贷记卡12个月内未还最低还款额次数-min'] = gd[u'贷记卡12个月内未还最低还款额次数'].min()
SUMMARY[u'贷记卡12个月内未还最低还款额次数-var'] = gd[u'贷记卡12个月内未还最低还款额次数'].var()
SUMMARY[u'信息获取时间-max'] = gd[u'信息获取时间'].max()
SUMMARY[u'信息获取时间-min'] = gd[u'信息获取时间'].min()
SUMMARY[u'客户id'] = SUMMARY.index
SUMMARY.fillna(0, inplace=True)
SUMMARY.to_csv(ur'Data/征信报告信用卡明细信息-summary.csv', encoding='utf8')
