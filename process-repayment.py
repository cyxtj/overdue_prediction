#coding=utf8
import numpy as np
import pandas as pd

##################################################################################
## repayment table
repayment_status_columns = [u'还款日', u'还款情况', u'客户id', u'被查询次数', u'批贷笔数', u'拒贷笔数', u'金额范围', u'逾期笔数']
repayment_status = pd.read_csv('Data/repayment_status.csv', index_col=False, names=repayment_status_columns, encoding='utf8')

status_d = {u'逾期':1, u'正常': 0}
repayment_status.replace({u'还款情况': status_d}, inplace=True)

# y label
repayment_status = repayment_status[repayment_status[u'还款日']>'2015-00-00']
gd = repayment_status.groupby(u'客户id')
print gd.ngroups    # >>>10000, 每个人都有y
y = gd[u'还款情况'].sum()>0  # 1:4529
y.to_csv(ur'Data/y.csv', encoding='utf8')
