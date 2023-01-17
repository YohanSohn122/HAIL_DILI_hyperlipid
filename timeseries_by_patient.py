# PATNO, HEIGHT, WEIGHT - nur | SEX- common_pat | ADMDT(입원날짜), DSCHDT(퇴원날짜) -adm_history |
import pandas
import pymysql  # use pip to install pymysql

print('program started')
conn = pymysql.connect(host='203.252.105.181',
                       user='yohansohn',  # userid
                       password='johnsohn12',  # userpassword
                       db='DR_ANS_AJCO',
                       charset='utf8')
cursor = conn.cursor()

# 0: SCODE
# only scode that has more than 100 patients are prescripted and has DCYN as N
# remove socde where product name contains '원외', '자가', '임상'
sql = '''SELECT SCODE
FROM common_drug_master
JOIN drug ON drug.ORDCODE = common_drug_master.ORDCODE
WHERE PATNO like '5%' AND DCYN ='N'
AND common_drug_master.TODATE LIKE '29991231'
AND common_drug_master.`PRODENNM` not like '%원외%'
AND common_drug_master.`PRODENNM` not like '%자가%'
AND common_drug_master.`PRODENNM` not like '%임상%'
GROUP BY SCODE
HAVING COUNT(DISTINCT PATNO) >= 100 AND SCODE is not null;'''
cursor.execute(sql)
result = cursor.fetchall()
drug_raw = list(result)
drug_scode = []
for i in drug_raw:
    drug_scode.append(i[0])

# avgerage data about weight and height : need to be modified to common average by gender and age
sql = '''
select avg(weight), avg(height) from nur
where patno>5000000 and patno<6000000;'''
cursor.execute(sql)
avg_data = cursor.fetchall()


# 0: PATNO, 1: HEIGHT, 2: WEIGHT
sql = '''select patno, meddate, height, weight from body_measure
where patno>5000000 and patno<6000000;'''
cursor.execute(sql)
result = cursor.fetchall()
additional_patient_data = {}
for i in result:
    if i[0] in additional_patient_data.keys():
        additional_patient_data[i[0]].append(i)
    else:
        additional_patient_data[i[0]] = [i]

# 0: PATNO, 1: MEDDATE, 2: HEIGHT, 3: WEIGHT, 4: SEX, 5: AGE_YEAR, 6: AGE_MONTH
sql = '''#PATNO, HEIGHT, WEIGHT, SEX, AGE_YEAR(나이(연)), AGE_MONTH(나이(개월))
SELECT nur.PATNO, nur.meddate, nur.HEIGHT, nur.WEIGHT, common_pat.SEX, nur.AGE_YEAR, nur.AGE_MONTH
FROM nur JOIN common_pat ON nur.PATNO = common_pat.PATNO
WHERE nur.PATNO >5000000;
'''
cursor.execute(sql)
result = cursor.fetchall()
patient_data_raw = list(result)

patient_data = {}
for i in patient_data_raw:
    if i[0] in patient_data.keys():
        patient_data[i[0]].append(i)
    else:
        patient_data[i[0]] = [i]
# # admission data of each patient(not used)
# # 0: PATNO, 1: ADMDT, 2: DSCHDT, 3: DATEDIFF
# sql = '''SELECT nur.PATNO, adm_history.ADMDT, adm_history.DSCHDT, DATEDIFF(adm_history.DSCHDT, adm_history.ADMDT)
# FROM adm_history JOIN common_pat ON adm_history.PATNO = common_pat.PATNO
# 		JOIN nur ON adm_history.PATNO = nur.PATNO
# WHERE adm_history.PATNO>5000000 AND (nur.`PATNO`,nur.`SEQ`) IN (SELECT PATNO,MAX(SEQ) as SEQ
# 				FROM nur GROUP BY PATNO);'''
# cursor.execute(sql)
# result = cursor.fetchall()
# patient_admin_data_raw = list(result)
# patient_admin_data = {}
# for i in patient_admin_data_raw:
#     if i[0] in patient_admin_data.keys():
#         patient_admin_data[i[0]].append(i)
#     else:
#         patient_admin_data[i[0]] = [i]

# 0: PATNO, 1: ordcode, 2: orddate, 3: rsltnum
sql = '''select patno, ordcode, orddate, rsltnum from `exam`
where `PATNO`>5000000 and `PATNO`<6000000;'''
cursor.execute(sql)
result = cursor.fetchall()
patient_exam_raw = list(result)
patient_exam = {}
for i in patient_exam_raw:
    if i[0] in patient_exam.keys():
        patient_exam[i[0]].append(i)
    else:
        patient_exam[i[0]] = [i]

# 0: patno, 1: ordcode, 2: orddate, 3: patfg, 4: packqty, 5: cnt, 6: day, 7: dcyn, 8: mkfg 9: scode
sql = '''select
    patno, drug.ordcode, orddate, patfg, packqty, cnt, day, dcyn, mkfg, scode
from
    `drug` join common_drug_master cdm on drug.ORDCODE = cdm.ORDCODE
where
    SCODE IS NOT NULL AND dcyn = 'N' AND `PATNO` >5000000 and `PATNO`<6000000;'''
cursor.execute(sql)
result = cursor.fetchall()
patient_drug_raw = list(result)
patient_drug = {}
for i in patient_drug_raw:
    if i[0] in patient_drug.keys():
        patient_drug[i[0]].append(i)
    else:
        patient_drug[i[0]] = [i]

# patient data by date
# key = patno
# item = { key: date, item = [nur, exam, drug] }
# nur, exam, drug : list
patient_data_by_date = {}
for patno in patient_data.keys():
    data_by_date = {}
    for nur_data in patient_data[patno]:
        if nur_data[1] in data_by_date.keys():
            data_by_date[nur_data[1]][0].append(nur_data)
        else:
            data_by_date[nur_data[1]] = [[nur_data], [], []]
    for exam_data in patient_exam[patno]:
        if exam_data[2] in data_by_date.keys():
            data_by_date[exam_data[2]][1].append(exam_data)
        else:
            data_by_date[exam_data[2]] = [[], [exam_data], []]
    for drug_data in patient_drug[patno]:
        if drug_data[2] in data_by_date.keys():
            data_by_date[drug_data[2]][2].append(drug_data)
        else:
            data_by_date[drug_data[2]] = [[], [], [drug_data]]

    keys = list(data_by_date.keys())
    keys.sort()
    data_by_date = {i: data_by_date[i] for i in keys}

    patient_data_by_date[patno] = data_by_date
# print(patient_data_by_date[list(patient_data_by_date.keys())[0]])

print('exam and drug data classified by patno')

# exam_ord : whole ordcode used for exam data
# exam_for_column : whole ordcode used only for column of timeseries data
# exam_pn_ord : ordcode that has PN for value type
# exam_pen_ord : ordcode that has PEN for value type
# exam_num_ord : ordcdoe that has numeric vlaue for value type
# grouping_needed : each group of ordcode that needs to be grouped to one(first ordcode as representitive ordcode)
# pen_case : whole case of values for PEN value type
# p_case: whole case of values for P value type
# e_case: whole case of values for E value type
# n_case: whole case of values for N value type
exam_ord = ('B1060001', 'B1540001A', 'B1540001B', 'B2570001', 'B2580001', 'B2602001', 'B2710001', 'C2210001',
            'C3720001', 'C3730001', 'C3750001', 'C3750001A', 'C4802001', 'C4802002', 'C4802051', 'C4803001',
            'C4812052', 'C4872001', 'C4872002', 'CZ492001')
exam_for_column = ('B1060001', 'B1540001A', 'B1540001B', 'B2570001', 'B2580001', 'B2602001', 'B2710001',
                   'C2210001', 'C3720001', 'C3730001', 'C3750001', 'C3750001A', 'C4802001(PN)',
                   'C4802001(value)', 'C4812052(PN)', 'C4812052(value)', 'C4872001(PN)', 'C4872001(value)')
exam_pn_ord = ('C4802001', 'C4812052', 'C4872001')
exam_pen_ord = ()
exam_num_ord = ('B1060001', 'B1540001A', 'B1540001B', 'B2570001', 'B2580001', 'B2602001', 'B2710001',
                'C2210001', 'C3720001', 'C3730001', 'C3750001', 'C3750001A')
grouping_needed = (('C4802001', 'C4802002', 'C4802051', 'C4803001'), ('C4872001', 'C4872002', 'CZ492001'))
pen_case = ('Positive', 'Pos', 'P', 'W.Pos', 'Negative', 'Neg', 'N', 'Equivocal', 'E')
p_case = ('Positive', 'Pos', 'P', 'W.Pos')
e_case = ('Equivocal', 'E')
n_case = ('Neg', 'N')

# key: PATNO
# item =[ 0: timesereis data patient body measure, 1: timeseries data exam data, 2: timeseries data drug data ]
# body measure = [ age_year, age_month, height, weight, bmi ]
# exam data = key : date
#             item = 0: B1060001, 1: B1540001A, 2: B1540001B, 3: B2570001, 4: B2580001
#             5: B2602001, 6: B2710001, 7: C2210001, 8: C3720001, 9: C3730001
#             10: C3750001, 11: C3750001A ... -> specified in ordcode list
# drug data = same structure with exam data with item number ordered by scode
import re
final_data = {}
# multiple_tests = {}
for patno in patient_data_by_date.keys():
    final_data[patno] = [{}, {}, {}]
    for date in patient_data_by_date[patno].keys():
        final_data[patno][0][date] = []
        final_data[patno][1][date] = {}
        final_data[patno][2][date] = {}

# patient body measure datw for each day
# need to be done after checking data
#         for body_data in patient_data_by_date[patno][date][0]:
#             temp = []
#             temp.append()
#             final_data[patno][0][date].append(list(patient_data_by_date[patno][date][0][2]))
        for ordcode in exam_num_ord:
            final_data[patno][1][date][ordcode] = []
        for ordcode in exam_pn_ord:
            final_data[patno][1][date][ordcode] = [[], []]
        for ordcode in exam_pen_ord:
            final_data[patno][1][date][ordcode] = [[], []]
        for exam_data in patient_data_by_date[patno][date][1]:
            if exam_data[1] in exam_num_ord:
                final_data[patno][1][date][exam_data[1]].append(exam_data[3])
            elif exam_data[1] in exam_pn_ord:
                if exam_data[3] in p_case or 'P' in exam_data[3]:
                    final_data[patno][1][date][exam_data[1]][0].append(1)
                elif exam_data[3] in n_case or 'N' in exam_data[3]:
                    final_data[patno][1][date][exam_data[1]][0].append(0)
                if len(re.findall("\d+\.\d+",exam_data[3])) != 0:
                    final_data[patno][1][date][exam_data[1]][1].append(re.findall("\d+\.\d+",exam_data[3])[0])
            elif exam_data[1] in grouping_needed[0]:
                if exam_data[3] in p_case or 'P' in exam_data[3]:
                    final_data[patno][1][date][grouping_needed[0][0]][0].append(1)
                elif exam_data[3] in n_case or 'N' in exam_data[3]:
                    final_data[patno][1][date][grouping_needed[0][0]][0].append(0)
                if len(re.findall("\d+\.\d+", exam_data[3])) != 0:
                    final_data[patno][1][date][grouping_needed[0][0]][1].append(re.findall("\d+\.\d+", exam_data[3])[0])
            elif exam_data[1] in grouping_needed[1]:
                if exam_data[3] in p_case or 'P' in exam_data[3]:
                    final_data[patno][1][date][grouping_needed[1][0]][0][0].append(1)
                elif exam_data[3] in n_case or 'N' in exam_data[3]:
                    final_data[patno][1][date][grouping_needed[1][0]][0].append(0)
                if len(re.findall("\d+\.\d+", exam_data[3])) != 0:
                    final_data[patno][1][date][grouping_needed[1][0]][1].append(re.findall("\d+\.\d+", exam_data[3])[0])
            else:
                continue
            # if len(final_data[patno][0][date][each_exam_data[1]]) > 1 and (each_exam_data[1] in exam_pn_ord or each_exam_data[1] in exam_pen_ord):
            #     print(patno, date, each_exam_data[1], final_data[patno][0][date][each_exam_data[1]])

        for scode in drug_scode:
            final_data[patno][2][date][scode] = 0
        for each_drug_data in patient_data_by_date[patno][date][2]:
            if each_drug_data[9] in drug_scode:
                dur = each_drug_data[4] * each_drug_data[5] * each_drug_data[6]
                if each_drug_data[8] in ['N', 'P']:
                    final_data[patno][2][date][each_drug_data[9]] += dur
                else:
                    if dur > 0:
                        final_data[patno][2][date][each_drug_data[9]] -= dur
                    else:
                        final_data[patno][2][date][each_drug_data[9]] += dur

# code to check mutliple identical exam data for same patient in same day
#        for i in exam_ord:
#            if len(final_data[patno][0][date][i]) > 1:
#                if i in multiple_tests.keys():
#                    multiple_tests[i].append([patno, len(final_data[patno][0][date][i])])
#                else:
#                    multiple_tests[i] = [[patno, len(final_data[patno][0][date][i])]]

print(final_data[list(final_data.keys())[4]])
print('final data making process finished')

import pandas as pd
import numpy as np

xlsx_dir = 'timeseries.xlsx'
csv_dir = 'timeseries.csv'

# export data about mutliple identical exam data for same patient in same day
# temp_dir = 'statistical.csv'
# final_ord = []
# final_patno = []
# final_count = []
# for i in multiple_tests.keys():
#    final_ord.append(i)
#    for j in multiple_tests[i]:
#         final_patno.append(j[0])
#         final_count.append(j[1])
#     for k in range(1,len(multiple_tests[i])):
#         final_ord.append(None)
# export_data = {'ordcode':final_ord,
#                'patno':final_patno,
#                'count':final_count}
# df = pd.DataFrame(export_data)
# df.to_csv(temp_dir)

final_patno = []
final_gender = []
final_age_year = []
final_age_month = []
final_weight = []
final_height = []
final_bmi = []
final_admission = []
final_discharge = []
final_admission_date = []
final_date = []
final_exam = []
for i in range(len(exam_num_ord)+len(exam_pn_ord)+len(exam_pen_ord)):
    final_exam.append([])

final_drug = []
for i in range(len(drug_scode)):
    final_drug.append([])


def null_input_check(patno, value, value_type):
    if value is None or value == '':
        print(patno, 'doesn\'t have,', value_type, 'data')
    return value


# missing_WH_patno_wh = []
# missing_WH_patno_w = []
# missing_WH_patno_h = []
# write all data needed to export
for patno in patient_data.keys():
    date_length = len(final_data[patno][0].keys())

    final_patno.append(patno)
    final_gender.append(null_input_check(patno, patient_data[patno][0][3], 'gender'))

    for date in final_data[patno][0].keys():
        final_date.append(date)
        # height, weight, age need to be done
        # final_height.append(null_input_check(patno, [patno][0][date][2]), 'height')
        for k in range(len(exam_ord)):
            final_exam[k].append(final_data[patno][0][date][exam_ord[k]])
        for m in range(len(drug_scode)):
            if final_data[patno][1][date][drug_scode[m]] >= 0:
                final_drug[m].append(final_data[patno][1][date][drug_scode[m]])
            else:
                final_drug[m].append('ERROR')

    # final_age_year.append(null_input_check(patno, patient_data[patno][0][4], 'age year'))
    # final_age_month.append(null_input_check(patno, patient_data[patno][0][5], 'age month'))
    # if patient_data[patno][0][1] is not None and patient_data[patno][0][2] is not None:
    #     final_weight.append(null_input_check(patno, patient_data[patno][0][2], 'weight'))
    #     final_height.append(null_input_check(patno, patient_data[patno][0][1], 'height'))
    #     final_bmi.append(final_weight[-1] / np.power(final_height[-1] / 100, 2))
    # elif patient_data[patno][0][1] is not None:
    #     final_height.append(patient_data[patno][0][1])
    #     if patno in additional_patient_data.keys():
    #         final_weight.append(additional_patient_data[patno][1])
    #     else:
    #         final_weight.append(avg_data[0][0])
    #         # missing_WH_patno_w.append(patno)
    #     final_bmi.append(final_weight[-1] / np.power(final_height[-1] / 100, 2))
    # elif patient_data[patno][0][2] is not None:
    #     final_weight.append(patient_data[patno][0][2])
    #     if patno in additional_patient_data.keys():
    #         final_height.append(additional_patient_data[patno][0])
    #     else:
    #         final_height.append(avg_data[0][1])
    #         # missing_WH_patno_h.append(patno)
    #     final_bmi.append(final_weight[-1] / np.power(final_height[-1] / 100, 2))
    # else:
    #     if patno in additional_patient_data.keys():
    #         final_weight.append(additional_patient_data[patno][1])
    #         final_height.append(additional_patient_data[patno][0])
    #     else:
    #         final_weight.append(avg_data[0][0])
    #         final_height.append(avg_data[0][1])
    #         # missing_WH_patno_wh.append(patno)
    #     final_bmi.append(final_weight[-1] / np.power(final_height[-1] / 100, 2))
    # for i in range(date_length - 1):
    #     final_patno.append(None)
    #     final_weight.append(None)
    #     final_height.append(None)
    #     final_bmi.append(None)
    #     final_gender.append(None)
    #     final_age_year.append(None)
    #     final_age_month.append(None)
    #
    # for j in patient_admin_data[patno]:
    #     final_admission.append(j[1])
    #     final_discharge.append(j[2])
    #     final_admission_date.append(j[3])
    # for l in range(date_length - len(patient_admin_data[patno])):
    #     final_admission.append(None)
    #     final_discharge.append(None)
    #     final_admission_date.append(None)
    #
    #

print('making export data process finished')

export_data = {'PATNO': final_patno,
               'GENDER': final_gender,
               'AGE_YEAR': final_age_year,
               'AGE_MONTH': final_age_month,
               'HEIGHT': final_height,
               'WEIGHT': final_weight,
               'BMI': final_bmi,
               'ADMDT': final_admission,
               'DSCHDT': final_discharge,
               'DATEDIFF': final_admission_date,
               'DATE': final_date}
for i in range(len(exam_ord)):
    export_data[exam_ord[i]] = final_exam[i]
for i in range(len(drug_scode)):
    export_data[drug_scode[i]] = final_drug[i]

# print('weight', missing_WH_patno_w)
# print('height', missing_WH_patno_h)
# print('both', missing_WH_patno_wh)
df = pd.DataFrame(export_data)
print(df.shape)

df.to_csv(csv_dir)
# df.to_xlsx(csv_dir,)

print('code finished')
