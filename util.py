#coding=utf8
import numpy as np
import pandas as pd
import h5py


def load_train():
    X = pd.read_csv(ur'Data/征信报告信用卡明细信息-summary.csv', index_col=None, encoding='utf8')
    print X.head()
    y = pd.read_csv(ur'Data/y.csv', encoding='utf8', index_col=None, names=[u'客户id', 'y'])['y']
    return X, y


def load_test():
    fr = h5py.File(r'Data/Test/Xfeatures.h5', 'r')
    X = fr['X'].value
    print 'X.shape = ', X.shape
    return X
