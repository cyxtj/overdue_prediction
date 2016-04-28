#coding=utf8

import numpy as np
import pandas as pd


##################################################################################
## repayment table
repayment_status_columns = ['还款日', '还款情况', '客户id', '被查询次数', '批贷笔数', '拒贷笔数', '金额范围', '逾期笔数']
repayment_status_columns = ['date', 'status', 'uid', 'query_times', 'accepted_times', 'denied_times', 'amount', 'delay_times']
repayment_status = pd.read_csv('Data/repayment_status.csv', index_col=False, names=repayment_status_columns, encoding='utf8')

# dictionaries that map text data to numerical data
u = []
for i, colname in enumerate(repayment_status):
    s = repayment_status[colname]
    unique_values = s.unique()
    u.append(unique_values)
    print unique_values
status_d = {u'逾期':1, u'正常': 0}
amount_text = [u'8~10', u'15~20', u'4~6', u'10~15', u'6~8', u'20~25', u'25~30', u'2~4', u'55~60',
        u'30~35', u'35~40', u'40~45', u'0~0.1', u'45~50', u'50~55', u'60~65', u'75~80', 
        u'1~2', u'70~75', u'80~85', u'90~95', u'100以上', u'65~70', u'85~90', u'95~100', u'100万以上']
amount_num = [9, 17, 5, 13, 7, 23, 27, 3, 57,
        33, 37, 43, 0.1, 47, 53, 63, 77, 
        1.5, 73, 83, 93, 150, 67, 87, 97, 150]
amount_d = {amount_text[i]: amount_num[i] for i in range(len(amount_text))}

repayment_status.replace({'status': status_d, 'amount': amount_d}, inplace=True)
d = repayment_status['date', 'uid', 'status']
# TODO how to deal with the date column?
repayment_status.drop('date', axis=1, inplace=True) 

# group by uid, so that we treat each user as one entry
gd = repayment_status.groupby('uid')
usummary = gd.aggregate({
    'status': sum, 
    # 'query_times': max, 
    # 'accepted_times': max, 
    # 'delay_times': max, 
    # 'amount': max
    })

ucount = gd['status'].count() # this is how many times each user repay
usummary['delay_rate'] = usummary['status'] / ucount


#####################################################################################
## basic table
basic_columns = '学历 所在城市 房产情况 居住情况 婚姻状态 \
    单位性质 合同金额 分期数 性别 年龄 户籍地址 客户id'
basic_columns =['edu', 'city', 'property_loan', 'live', 'marriage',
            'work', 'amount', 'stages', 'gender', 'age', 'register_addr', 'uid']
basic = pd.read_csv('Data/basic.csv', header=None, index_col=False, names=basic_columns, encoding='utf8')

# distinct values in each column
u = []
for i, colname in enumerate(basic):
    if i==10: break
    s = basic[colname]
    unique_values = s.unique()
    u.append(unique_values)
    print '----------------'
edu = [ u'高中及以下', u'专/本', u'硕士及以上']
property_loan = [ u'无房', u'有房无贷款', u'有房有贷款']
live = [ u'其他', u'自有住房']
marriage = [u'未婚', u'已婚', u'离异', u'丧偶', u'其他']
work = [u'个体工商户', u'民营企业', u'国有企业事业单位', u'自顾人士']

# TODO how to deal with address data?
basic.drop(['city', 'register_addr'], axis=1, inplace=True)
# amount column can be mapped to numerical, and gender data can convert to binary
basic.replace({'amount': amount_d, 'gender': {u'男':1, u'女':0}}, inplace=True)
to_dummy = ['edu', 'property_loan', 'live', 'marriage', 'work']
b = pd.get_dummies(basic, columns=to_dummy)


#################################################################################
## Basic pridiction
# 用basic信息作为Xfeature, usummary['delay_rate'] or ['status']作为y进行预测
usummary['uid'] = usummary.index
c = pd.merge(b, usummary[['uid', 'status']], on='uid')
y = c['status']>0
X = c.drop('status', axis=1)
a = float(y.sum())/len(y)
w1 = y * (1 - a)
w0 = (-y) * a
w = w0 + w1
from train_predict import test_xgb
test_xgb(X, y, w)
import h5py
fw = h5py.File('Data/Train/Xfeatures.h5', 'w')
fw.create_dataset('X', data=X)
fw.create_dataset('y', data=y)
fw.create_dataset('w', data=w)
fw.close()

c.to_csv('xy.csv', encoding='utf8')

###################################################################################
# 征信报告贷款最近24个月还款明细情况.csv
'''
符号说明：
	贷款还款状态	贷记卡
/	未开账户	
*	本月没有还款历史	未使用
N	Normal按时归还全部	
D	担保人代还	-
Z	以资抵债，部分	-
C	结清	结清的销户
G	除结清外的终止账户	
#	账户开立，但是状态未知	
1	逾期1-30天	未还最低还款1次
2	31-60	连续2次
3		
7	180天以上	连续7次及以上
'''
