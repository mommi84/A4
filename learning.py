#!/usr/bin/env python
from sklearn import svm

# SVC(C=1.0, cache_size=200, class_weight=None, coef0=0.0,
#     decision_function_shape=None, degree=3, gamma='auto', kernel='rbf',
#     max_iter=-1, probability=False, random_state=None, shrinking=True,
#     tol=0.001, verbose=False)
def learn(X, y):
    clf = svm.SVC(kernel='linear', verbose=True)
    clf.fit(X, y)
    return clf
    
def accuracy(model, X, y):
    return model.score(X, y)
    
if __name__ == '__main__':
    X = [[0, 0], [1, 1]]
    y = [0, 1]
    model = learn(X, y)
    print model.predict([[2., 2.]])
    print accuracy(model, X, y)
