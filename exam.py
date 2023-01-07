# exam : whole data
# 0: PATNO, 1: VERIFYDT, 2: ORDCODE, 3:EXAMSEQ, 4: MEDDATE, 5: ORDDATE, 6:SPCDATE
# 7:SPCNO, 8: AGEYEAR, 9: RSLTNUM, 10: UNIT, 11: ETLDTTM
exam = []

import pymysql      # use pip to install pymysql
conn = pymysql.connect(host='203.252.105.181',
                       user='yohansohn',        #userid
                       password='johnsohn12',   #userpassword
                       db='DR_ANS_AJCO',
                       charset='utf8')
cursor = conn.cursor()
sql = '''
            select * from `exam`
            where `PATNO` > 5000000 and `PATNO`< 6000000
            ;
        '''
cursor.execute(sql)
result = cursor.fetchall()      # result is given as tuple
exam = list(result)
for i in range(len(exam)):      # each data's type is tuple so type chasting to list is needed
    exam[i] = list(exam[i])
#print(exam[0])

exam_dic_by_ordcode = {}    #save as dictionary, key == ordcode, value = list of data with same key

for i in exam:
    #print(i)
    if i[2] in exam_dic_by_ordcode.keys():
        exam_dic_by_ordcode[i[2]].append(i)
    else:
        exam_dic_by_ordcode[i[2]] = [i]
keys = list(exam_dic_by_ordcode.keys())
keys.sort()
exam_dic_by_ordcode = {i: exam_dic_by_ordcode[i] for i in keys}

# distinct : data for distinct rsltnum
# only number of types for numeric data
# dicitonary for non-numeric data
# non-numeric data could carry float, negative number
# numeric : ORDCODE for data that has mainly numeric data
# mainly numeric == (len(numeric)/len(non-numeric) > 1)
distinct = {}
numeric = []
pn = []
pen = []
null = []

# function to check if string is numeric value
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
# list of cases for pn, pen
pen_case = ('Positive','Pos','P','W.Pos','Negative','Neg','N','Equivocal','E')
e_case = ('Equivocal','E')
# list of unusual case
null_case = ('B4144004')

for i in exam_dic_by_ordcode.keys():
    distinct_numeric = []
    distinct_non_numeric = {}
    for j in exam_dic_by_ordcode[i]:
        if is_number(j[9]):
            if j[9] not in distinct_numeric:
                distinct_numeric.append(j[9])
        else:
            if j[9] not in distinct_non_numeric.keys():
                distinct_non_numeric[j[9]] = 1
            else:
                distinct_non_numeric[j[9]] += 1
    distinct[i] = []
    distinct[i].append(len(distinct_numeric))
    distinct[i].append(distinct_non_numeric)
'''
    if i in null_case:
        null.append(i)
    else:
        non_numeric = len(distinct_non_numeric.keys())
        try:
            if len(distinct_numeric) / non_numeric > 1:
                numeric.append(i)
        except ZeroDivisionError:
            if non_numeric == 0:
                numeric.append(i)
        if i not in numeric:
            #print(type(distinct_non_numeric.keys()))
            if list(distinct_non_numeric.keys())[0] in pen_case or 'P' in list(distinct_non_numeric.keys())[0] or 'N' in list(distinct_non_numeric.keys())[0] or 'E' in list(distinct_non_numeric.keys())[0]:
                for k in distinct_non_numeric.keys():
                    if k in e_case or 'E' in k:
                        pen.append(i)
                        break
                else:
                    pn.append(i)
            else:
                numeric.append(i)
'''

'''

# exam_dic_numeric : exam data that is numeric
# type casting for RSLTNUM is also done
# non-numeric data is casted into None
exam_dic_numeric = {}
for i in exam_dic_by_ordcode.keys():
    if i in numeric:
        exam_dic_numeric[i] = exam_dic_by_ordcode[i]
        for j in exam_dic_numeric[i]:
            if is_number(j[9]):
                j[9] = float(j[9])
            else:
                j[9] = None

print('satistical data for numeric values')
import numpy as np
for i in exam_dic_numeric.keys():
    rsltnum = []
    for j in exam_dic_numeric[i]:
        if j[9] is not None:
            rsltnum.append(j[9])
    print(i,'count without non-nummeric value :',len(rsltnum))
    if len(rsltnum)!=0:
        print(i, 'mean=',np.mean(rsltnum), ', min=', np.min(rsltnum), ', max=', np.max(rsltnum),
              ', std=', np.std(rsltnum))
        print(i, end=' ')
        for i in range(10, 100, 10):
            print(i, end='')
            print('%=', np.percentile(rsltnum, i), end=', ')
    else:
        print(i, 'no specific value to examine, check other value')
    print()
print()


# converting pen value to integer
# positive : 1
# equivocal : 0
# negative : -1
print('statistical value from ORDCODE that has pen value')
e_case = ('Equivocal','E')
p_case = ('Positive','Pos','P','W.Pos')
n_case = ('Neg','N')

exam_dic_pen = {}
for i in pen:
    num_p = []
    num_e = []
    num_n = []
    num_error = []
    exam_dic_pen[i] = exam_dic_by_ordcode[i]
    for j in exam_dic_pen[i]:
        if j[9] in p_case or 'P' in j[9]:
            j[9] = 1
            num_p.append(1)
        elif j[9] in e_case or 'E' in j[9]:
            j[9] = 0
            num_e.append(0)
        elif j[9] in n_case or 'N' in j[9]:
            j[9] = -1
            num_n.append(-1)
        else:
            num_error.append(j[9])
    total = num_p + num_e + num_n
    print(i,'count :',len(total))
    print(i,'positive :',len(num_p),', equivocal :',len(num_e),', negative :',len(num_n),', error :',len(num_error))
    if len(total)!=0:
        print(i,'mean :',np.mean(total),', std :',np.std(total))
    else:
        print(i,'no specific value to examine, check other value')
    if len(num_error) != 0:
        print(num_error)
print()
# converting pn value to integer
# positive : 1
# negative : 0
# use same case from pen value
print('statistical value from ORDCODE that has pn value')

exam_dic_pn = {}
for i in pn:
    num_p = []
    num_n = []
    num_error = []
    exam_dic_pn[i] = exam_dic_by_ordcode[i]
    for j in exam_dic_pn[i]:
        if j[9] in p_case or 'P' in j[9]:
            j[9] = 1
            num_p.append(1)
        elif j[9] in n_case or 'N' in j[9]:
            j[9] = -1
            num_n.append(0)
        else:
            num_error.append(j[9])
    total = num_p + num_n
    print(i,'count :',len(total))
    print(i,'positive :',len(num_p),', negative :',len(num_n),', error :',len(num_error))
    if len(total)!=0:
        print(i,'mean :',np.mean(total),', std :',np.std(total))
    else:
        print(i,'no specific value to examine, check other value')
    if len(num_error) != 0:
        print(num_error)
print()

final_patient = []
for i in exam_dic_by_ordcode.keys():
    distinct_patient = []
    for j in exam_dic_by_ordcode[i]:
        if j[0] not in distinct_patient:
            distinct_patient.append(j[0])
    fianl_patient.append(len(distinct_patient))

import pandas as pd
import os

file_nm = 'drug_wo_scode.xlsx'
xlxs_dir = os.path.join(file_nm)

df = pd.DataFrame({'ordcode':list(drug_dic_by_ordcode.keys()),
                   'patient':final_patient,
                   'duration':final_duration,
                   'avg_duration':final_avg_dur})

df.to_excel(xlxs_dir, # directory and file name to write
            sheet_name = 'Sheet1', 
            na_rep = 'NaN', 
            float_format = "%.2f", 
            header = True,
            startrow = 1, 
            startcol = 1, 
            #engine = 'xlsxwriter', 
            freeze_panes = (2, 0)
            ) 
