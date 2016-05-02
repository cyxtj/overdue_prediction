# coding=utf8
import numpy as np
import pandas as pd

from util import load_train
X, y = load_train()

if __name__ == '__main__':
    import sys
    clf_name = sys.argv[1]
    print clf_name + '======================='

    if clf_name == 'XGB':
        from train_predict import test_xgb
        test_xgb(X, y)
