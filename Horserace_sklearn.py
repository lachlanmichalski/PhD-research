#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  2 16:48:52 2020
@author: lachlan
"""

import pandas as pd 
import numpy as np
import time

################################################################################

#Inherently multiclass classifiers
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import BernoulliNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.linear_model import LogisticRegression #(setting multi_class=”multinomial”)
from sklearn.neural_network import MLPClassifier
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.experimental import enable_hist_gradient_boosting
from sklearn.ensemble import HistGradientBoostingClassifier #new faster than original 
from sklearn.metrics import accuracy_score
from sklearn.metrics import roc_auc_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import classification_report
from sklearn.multiclass import OneVsRestClassifier
from sklearn.svm import SVC
from sklearn.svm import LinearSVC
from xgboost import XGBClassifier

from sklearn.model_selection import cross_val_score

from sklearn.model_selection import train_test_split
from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import PowerTransformer
from sklearn.utils import shuffle
##############################################################################   

''' Table 2 paper'''

def multiclass_inherent(data, string_us_gl, transform): 
    
    if transform=='False':
        data = shuffle(data, random_state=42).reset_index(drop=True)
        max_y = max(data.iloc[:,0]) # determine if data is binary or mc
        # split data into X and y (y has to be first col)
        Xdata = data.iloc[:,1:]
        ydata = data.iloc[:,0].values.ravel()
        # split data into train and test sets
        X_train, X_test, y_train, y_test = train_test_split(Xdata, ydata, random_state=42)
            
        #sklearns inherent multiclass classifiers
        classifiers = [(KNeighborsClassifier()), (BernoulliNB()),
                       (GaussianNB()),
                       LinearDiscriminantAnalysis(),
                       QuadraticDiscriminantAnalysis(),
                       DecisionTreeClassifier(random_state=0), 
                       RandomForestClassifier(random_state=0), 
                       ExtraTreesClassifier(random_state=0),
                       HistGradientBoostingClassifier(random_state=0), 
                       AdaBoostClassifier(random_state=0)]
        
    if transform=='True':
    
        data = shuffle(data, random_state=42).reset_index(drop=True)
        max_y = max(data.iloc[:,0]) # determine if data is binary or mc
        # split data into X and y (y has to be first col)
        Xdata = data.iloc[:,1:]
        ydata = data.iloc[:,0].values.ravel()
        
        # split data into train and test sets
        X_train, X_test, y_train, y_test = train_test_split(Xdata, ydata, random_state=42)
        
        power = PowerTransformer(method='yeo-johnson', standardize=True)
        X_train_trans = power.fit_transform(X_train)
        X_train_trans = pd.DataFrame(X_train_trans, columns=[X_train.columns])
        X_test_trans = power.fit_transform(X_test)
        X_test_trans = pd.DataFrame(X_test_trans, columns=[X_test.columns])
        X_train = X_train_trans
        X_test = X_test_trans
        
        classifiers = [(KNeighborsClassifier()), (BernoulliNB()),
                    (GaussianNB()),
                    LinearDiscriminantAnalysis(),
                    QuadraticDiscriminantAnalysis(),
                    DecisionTreeClassifier(random_state=0), 
                    RandomForestClassifier(random_state=0), 
                    ExtraTreesClassifier(random_state=0),
                    HistGradientBoostingClassifier(random_state=0), 
                    AdaBoostClassifier(random_state=0),
                    XGBClassifier(random_state=0),
                    LogisticRegression(random_state=0, multi_class='multinomial',
                                       solver='sag', verbose=3, max_iter=1000, tol=0.001),
                    MLPClassifier(hidden_layer_sizes=(int(len(X_train.columns)*(2/3)),
                                                      int(len(X_train.columns)*(2/3))),
                      random_state=0, max_iter=1000, verbose=3, early_stopping=True),
                    SVC(random_state=0, max_iter=10000, verbose=3,  probability=True) 
                    ]
    
    #dictionaries for loop    
    auc_dict = {}
    cv_auc_dict = {}
    cv_aucstd_dict = {}
    accuracy_dict = {}
    cv_acc_dict = {}
    cv_std_dict = {}
    classification_report_dict = {}
    confusion_matrix_dict = {}
    run_time_dict = {}
    
    #loop through classifiers and append results for auc, accuracy
    # classificaton report and confusion matrix
    for idx, cl in enumerate(classifiers):
        model = cl
        if type(model).__name__ == 'HistGradientBoostingClassifier':
            model_name='HistGBoostClassifier'
        else:
            model_name = type(model).__name__
        start = time.time()
        model.fit(X_train, y_train)
        end = time.time()
        run_time = end-start
        run_time_dict[model_name] = run_time
        print(str(run_time)+' seconds')
        y_pred = model.predict(X_test)
        predictions = [round(value) for value in y_pred]
        accuracy = accuracy_score(y_test, predictions)
        
        '''Cross validated accuracy'''
        k_folds = StratifiedKFold(n_splits=5)
        splits = list(k_folds.split(X_train, y_train))
        cv_acc = cross_val_score(model, X_train, y_train, cv=splits, scoring='accuracy')  
        cv_acc_mean = cv_acc.mean()
        cv_acc_std = cv_acc.std()
        cv_auc=cross_val_score(model, X_train, y_train, cv=splits, scoring='roc_auc_ovr_weighted')        
        cv_auc_mean = cv_auc.mean()
        cv_auc_std = cv_auc.std()
        
        if max_y == 1: #if data is binary
            AUC = roc_auc_score(y_test, model.predict_proba(X_test)[:,1])
        else: #if data is multiclass
            AUC = roc_auc_score(y_test, model.predict_proba(X_test),
                                multi_class='ovr')
            
        confusion_m = confusion_matrix(y_test, y_pred)
        confusion_m = pd.DataFrame(confusion_m)
        class_report = classification_report(y_test, y_pred, output_dict=True)
        class_report = pd.DataFrame(class_report).T
        print("accuracy {}: %.2f%%".format(model_name) % (accuracy * 100.0))
        print("cv_accuracy {}: %.2f%%".format(model_name) % (cv_acc_mean * 100.0))
        print("roc_auc_score {}: %.2f%%".format(model_name) % (AUC * 100.0))
        print("cv_auc {}: %.2f%%".format(model_name) % (cv_auc_mean * 100.0))

        auc_dict[model_name]=AUC.round(4)
        accuracy_dict[model_name]=accuracy.round(4) 
        cv_acc_dict[model_name]=cv_acc_mean.round(4)
        cv_std_dict[model_name]=cv_acc_std.round(4) 
        cv_auc_dict[model_name]=cv_auc_mean.round(4)
        cv_aucstd_dict[model_name]=cv_auc_std.round(4) 
        classification_report_dict[model_name]=class_report
        confusion_matrix_dict[model_name]=confusion_m
        
    df = pd.DataFrame([auc_dict, accuracy_dict, cv_auc_dict, cv_aucstd_dict,
                       cv_acc_dict, cv_std_dict,run_time_dict]).T
    df.rename(columns={df.columns[0]:'AUC', df.columns[1]:'Accuracy',
                       df.columns[2]:'CV_AUC', df.columns[3]:'CV_AUC_std', 
                       df.columns[4]:'CV_Accuracy', df.columns[5]:'CV_Accuracy_std',
                       df.columns[6]:'model_fit_runtime'       
                       }, inplace=True)
    
    return df, classification_report_dict, confusion_matrix_dict

##############################################################################   

def export_multiclass_inherent(auc_roc_df, class_report, 
                               confusion_m, path):    
    
    class_report['LDA'] = class_report.pop('LinearDiscriminantAnalysis')
    class_report['QDA'] = class_report.pop('QuadraticDiscriminantAnalysis')
    confusion_m['LDA'] = confusion_m.pop('LinearDiscriminantAnalysis')
    confusion_m['QDA'] = confusion_m.pop('QuadraticDiscriminantAnalysis')
    
    class_report = {'clr_' + key: value for key, value in class_report.items()}
    confusion_m = {'cm_' + key: value for key, value in confusion_m.items()}

    dict_export={'auc_roc': auc_roc_df}
    dict_export.update(class_report)
    dict_export.update(confusion_m)


    writer = pd.ExcelWriter(path)
    for key in dict_export:
        dict_export[key].to_excel(writer, key)
        
    writer.save()
