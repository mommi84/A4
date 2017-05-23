#!/usr/bin/env python
from sklearn import svm
import numpy as np

# SVC(C=1.0, cache_size=200, class_weight=None, coef0=0.0,
#     decision_function_shape=None, degree=3, gamma='auto', kernel='rbf',
#     max_iter=-1, probability=False, random_state=None, shrinking=True,
#     tol=0.001, verbose=False)
def learn(indices, feats, classes):
    X = list()
    y = list()
    for ex in feats:
        v = np.zeros(len(feats[ex]))
        i = 0
        for key in sorted(indices.iterkeys()):
            val = indices[key]
            v[i] = feats[ex][val]
            i += 1
        X.append(v)
        y.append(classes[ex])
    clf = svm.SVC(kernel='linear', verbose=True)
    clf.fit(X, y)
    return clf
    
# def accuracy(model, X, y):
#     return model.score(X, y)
    
def f_score(indices, model, feats, classes):
    X = list()
    y = list()
    for ex in feats:
        v = np.zeros(len(feats[ex]))
        i = 0
        for key in sorted(indices.iterkeys()):
            val = indices[key]
            v[i] = feats[ex][val]
            i += 1
        X.append(v)
        y.append(classes[ex])
    pred = model.predict(X)
    tp = 0
    fp = 0
    tn = 0
    fn = 0
    fn_list = list()
    fp_list = list()
    for i in range(len(pred)):
        if y[i] == 1:
            if pred[i] == 1:
                tp += 1
            else:
                fn += 1
                fn_list.append(i)
        else:
            if pred[i] == 1:
                fp += 1
                fp_list.append(i)
            else:
                tn += 1
    return tp, fp, tn, fn, fp_list, fn_list
    
if __name__ == '__main__':
    X = [[0, 0], [1, 1]]
    y = [0, 1]
    model = learn(X, y)
    print model.predict([[2., 2.]])
    # print accuracy(model, X, y)
