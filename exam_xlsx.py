# exam : whole data
# 0: PATNO, 1: VERIFYDT, 2: ORDCODE, 3:EXAMSEQ, 4: MEDDATE, 5: ORDDATE, 6:SPCDATE
# 7:SPCNO, 8: AGEYEAR, 9: RSLTNUM, 10: UNIT, 11: ETLDTTM
import numpy as np

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

final_patient = []
final_distinct_numeric = []
final_distinct_non_numeric = []
final_distinct_total = []
final_value_type = []
final_unit = []
final_records = []
final_mean = []
final_std = []
final_min = []
final_percentile = [[],[],[],[],[],[],[],[],[]]
final_max = []
final_p = []
final_e = []
final_n =[]
final_other = []

# function to check if string is numeric value
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
# list of cases for pn, pen
pen_case = ('Positive','Pos','P','W.Pos','Negative','Neg','N','Equivocal','E')
p_case = ('Positive','Pos','P','W.Pos')
e_case = ('Equivocal','E')
n_case = ('Neg','N')
# list of unusual case
null_case = ('B4144004')

for i in exam_dic_by_ordcode.keys():
    # count for numeric, non-numeric, unit
    distinct_numeric = []
    distinct_non_numeric = {}
    distinct_unit = []
    distinct_patient = []
    for j in exam_dic_by_ordcode[i]:
        if is_number(j[9]):
            if j[9] not in distinct_numeric:
                distinct_numeric.append(j[9])
        else:
            if j[9] not in distinct_non_numeric.keys():
                distinct_non_numeric[j[9]] = 1
            else:
                distinct_non_numeric[j[9]] += 1
        if j[10] is None:
            distinct_unit.append('NULL')
        elif j[10] not in distinct_unit:
            distinct_unit.append(j[10])
        if j[0] not in distinct_patient:
            distinct_patient.append(j[0])
        
    distinct[i] = []
    distinct[i].append(len(distinct_numeric))
    distinct[i].append(distinct_non_numeric)

    final_patient.append(len(distinct_patient))
    final_distinct_numeric.append(len(distinct_numeric))
    num_non_numeric = 0
    for j in distinct_non_numeric.keys():
        num_non_numeric += distinct_non_numeric[j]
    final_distinct_non_numeric.append(num_non_numeric)
    final_distinct_total.append(len(distinct_numeric) + num_non_numeric)
    if len(distinct_unit) == 1:
        final_unit.append(distinct_unit[0])
    else:
        string = ''
        for k in range(len(distinct_unit)-1):
            string += distinct_unit[k]
            string += '; '
        string += distinct_unit[len(distinct_unit)-1]
        final_unit.append(string)
    final_other.append(distinct_non_numeric)

    # case division
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

# need value type, records, mean, std, min, percentile, max
# None for p, e, n
def numeric_data(ordcode):
    final_value_type.append('numeric')
    for j in exam_dic_by_ordcode[ordcode]:
        if is_number(j[9]):
            j[9] = float(j[9])
        else:
            j[9] = None
    rsltnum = []
    for j in exam_dic_by_ordcode[ordcode]:
        if j[9] is not None:
            rsltnum.append(j[9])
    final_records.append(len(rsltnum))
    if len(rsltnum)!=0:
        final_mean.append(np.mean(rsltnum))
        final_min.append(np.min(rsltnum))
        final_max.append(np.max(rsltnum))
        final_std.append(np.std(rsltnum))
        for i in range(9):
            final_percentile[i].append(np.percentile(rsltnum,(i+1)*10))
    else:
        final_mean.append(None)
        final_min.append(None)
        final_max.append(None)
        final_std.append(None)
        for i in range(9):
            final_percentile[i].append(None)

    final_p.append(None)
    final_e.append(None)
    final_n.append(None)

# need value type, records, mean, std, p, e, n
# None for min, percentile, max
def pen_data(ordcode):
    final_value_type.append('PEN')
    num_p = []
    num_e = []
    num_n = []
    num_error = []
    for j in exam_dic_by_ordcode[ordcode]:
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
    final_records.append(len(total))
    final_p.append(len(num_p))
    final_e.append(len(num_e))
    final_n.append(len(num_n))
    if len(total)!=0:
        final_mean.append(np.mean(total))
        final_std.append(np.std(total))
    else:
        final_mean.append(None)
        final_std.append(None)

    final_min.append(None)
    final_max.append(None)
    for i in range(9):
        final_percentile[i].append(None)

# need value type, records, mean, std, p, n
# None for min, percentile, max, e
def pn_data(ordcode):
    final_value_type.append('PN')
    num_p = []
    num_n = []
    num_error = []
    for j in exam_dic_by_ordcode[ordcode]:
        if j[9] in p_case or 'P' in j[9]:
            j[9] = 1
            num_p.append(1)
        elif j[9] in n_case or 'N' in j[9]:
            j[9] = -1
            num_n.append(-1)
        else:
            num_error.append(j[9])
    total = num_p + num_n
    final_records.append(len(total))
    final_p.append(len(num_p))
    final_n.append(len(num_n))
    if len(total)!=0:
        final_mean.append(np.mean(total))
        final_std.append(np.std(total))
    else:
        final_mean.append(None)
        final_std.append(None)

    final_e.append(None)
    final_min.append(None)
    final_max.append(None)
    for i in range(9):
        final_percentile[i].append(None)

# need value type, records
# None for rest
def null_data(ordcode):
    final_value_type.append('NULL')
    final_records.append(0)
    final_mean.append(None)
    final_std.append(None)
    final_min.append(None)
    final_max.append(None)
    for i in range(9):
        final_percentile[i].append(None)
    final_p.append(None)
    final_e.append(None)
    final_n.append(None)

for i in exam_dic_by_ordcode.keys():
    if i in numeric:
        numeric_data(i)
    elif i in pn:
        pn_data(i)
    elif i in pen:
        pen_data(i)
    elif i in null:
        null_data(i)
    else:
        print('processing error',i)
'''
print(len(list(exam_dic_by_ordcode.keys())))
print(len(final_patient))
print(len(final_distinct_numeric))
print(len(final_distinct_non_numeric))
print(len(final_distinct_total))
print(len(final_value_type))
print(len(final_unit))
print(len(final_records))
print(len(final_mean))
print(len(final_std))
print(len(final_min))
print(len(final_percentile[0]))
print(len(final_max))
print(len(final_p))
print(len(final_e))
print(len(final_n))
print(len(final_other))

'''
import pandas as pd
import os

file_nm = 'exam.xlsx'
xlxs_dir = os.path.join(file_nm)

df = pd.DataFrame({'ordcode':list(exam_dic_by_ordcode.keys()),
                   'patient':final_patient,
                   'distinct_numeric':final_distinct_numeric,
                   'distinct_non_numeric':final_distinct_non_numeric,
                   'distinct_total':final_distinct_total,
                   'value_type':final_value_type,
                   'unit':final_unit,
                   'records':final_records,
                   'mean':final_mean,
                   'std':final_std,
                   'min':final_min,
                   '10%':final_percentile[0],
                   '20%':final_percentile[1],
                   '30%':final_percentile[2],
                   '40%':final_percentile[3],
                   '50%':final_percentile[4],
                   '60%':final_percentile[5],
                   '70%':final_percentile[6],
                   '80%':final_percentile[7],
                   '90%':final_percentile[8],
                   'max':final_max,
                   'P':final_p,
                   'E':final_e,
                   'N':final_n,
                   'other_value':final_other
                   })

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
