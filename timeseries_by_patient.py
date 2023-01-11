#PATNO, HEIGHT, WEIGHT - nur | SEX- common_pat | ADMDT(입원날짜), DSCHDT(퇴원날짜) -adm_history |
import pymysql      # use pip to install pymysql
print('program started')
conn = pymysql.connect(host='203.252.105.181',
                       user='yohansohn',        #userid
                       password='johnsohn12',   #userpassword
                       db='DR_ANS_AJCO',
                       charset='utf8')
cursor = conn.cursor()

# 0: PATNO, 1: HEIGHT, 2: WEIGHT, 3: WEX, 4: AGE_YEAR, 5: AGE_MONTH
sql = '''#PATNO, HEIGHT, WEIGHT, SEX, AGE_YEAR(나이(연)), AGE_MONTH(나이(개월))
SELECT nur.PATNO, nur.HEIGHT, nur.WEIGHT, common_pat.SEX, nur.AGE_YEAR, nur.AGE_MONTH
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
for i in patient_data.keys():
    if len(patient_data[i]) != 1:
        valid = []
        j = len(patient_data[i])-1
        while j>=0:
            valid = patient_data[i][j]
            if valid[1] is None or valid[2] is None:
                j -= 1
            else:
                break
        if j==-1:
            patient_data[i] = [patient_data[i][-1]]
        else:
            patient_data[i] = [valid]
    else:
        patient_data[i] = [patient_data[i][0]]

# 0: ORDCODE, 1: SCODE, 2: IRGDNAME
sql='''SELECT drug.ORDCODE, common_drug_master.SCODE, common_drug_master.IRGDNAME FROM drug JOIN `common_drug_master` ON drug.ORDCODE = `common_drug_master`.ORDCODE
WHERE drug.PATNO>5000000 AND common_drug_master.SCODE is not null 
GROUP BY drug.ORDCODE ORDER BY common_drug_master.SCODE ASC;'''
cursor.execute(sql)
result = cursor.fetchall()
scode = list(result)

# 0: PATNO, 1: ADMDT, 2: DSCHDT, 3: DATEDIFF
sql = '''SELECT nur.PATNO, adm_history.ADMDT, adm_history.DSCHDT, DATEDIFF(adm_history.DSCHDT, adm_history.ADMDT)
FROM adm_history JOIN common_pat ON adm_history.PATNO = common_pat.PATNO 
		JOIN nur ON adm_history.PATNO = nur.PATNO
WHERE adm_history.PATNO>5000000 AND (nur.`PATNO`,nur.`SEQ`) IN (SELECT PATNO,MAX(SEQ) as SEQ 
				FROM nur GROUP BY PATNO);'''
cursor.execute(sql)
result = cursor.fetchall()
patient_admin_data_raw = list(result)
patient_admin_data = {}
for i in patient_admin_data_raw:
    if i[0] in patient_admin_data.keys():
        patient_admin_data[i[0]].append(i)
    else:
        patient_admin_data[i[0]] = [i]

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

# 0: patno, 1: ordcode, 2: orddate, 3: patfg, 4: packqty, 5: cnt, 6: day, 7: dcyn, 8: mkfg
sql = '''select patno, ordcode, orddate, patfg, packqty, cnt, day, dcyn, mkfg from `drug`
where `PATNO`>5000000 and `PATNO`<6000000;'''
cursor.execute(sql)
result = cursor.fetchall()
patient_drug_raw = list(result)
patient_drug = {}
for i in patient_drug_raw:
    if i[0] in patient_drug.keys():
        patient_drug[i[0]].append(i)
    else:
        patient_drug[i[0]] = [i]

# patient data
# item = [ nur data, { key: date, item = [exam data, drug data] } ]
for patno in patient_data.keys():
    data_by_date = {}
    for data in patient_exam[patno]:
        if data[2] in data_by_date.keys():
            data_by_date[data[2]][0].append(data)
        else:
            data_by_date[data[2]] = [[data], []]
    for data in patient_drug[patno]:
        if data[2] in data_by_date.keys():
            data_by_date[data[2]][1].append(data)
        else:
            data_by_date[data[2]] = [[], [data]]

    keys = list(data_by_date.keys())
    keys.sort()
    data_by_date = {i: data_by_date[i] for i in keys}

    patient_data[patno].append(data_by_date)
#print(patient_data[list(patient_data.keys())[0]])
print('exam and drug data classified by patno')

# key: PATNO, item =[ 0: nur data, 1: admission data, 2: timeseries data exam data, 3: timeseries data drug data ]
# exam data = key : date
#             item = 0: B1060001, 1: B1540001A, 2: B1540001B, 3: B2570001, 4: B2580001
#             5: B2602001, 6: B2710001, 7: C2210001, 8: C3720001, 9: C3730001
#             10: C3750001, 11: C3750001A
# drug data = same structure with exam data with item number ordered by scode
exam_ord = ['B1060001','B1540001A','B1540001B','B2570001','B2580001','B2602001',
            'B2710001','C2210001','C3720001','C3730001','C3750001','C3750001A']
def find_ord(ord):
    for i in range(12):
        if exam_ord[i] == ord:
            return i
    return -1
def find_scode(inp):
    for i in range(len(scode)):
        if scode[i][0] == inp:
            return i
    return -1

final_data = {}
for patno in patient_data.keys():
    final_data[patno] = [patient_data[patno][0], patient_admin_data[patno], {}, {}]
    for date in patient_data[patno][1].keys():
        final_data[patno][2][date] = [None,None,None,None,None,None,None,None,None,None,None,None]
        for k in patient_data[patno][1][date][0]:   # k : # 0: PATNO, 1: ordcode, 2: orddate, 3: rsltnum
            index = find_ord(k[1])
            if index != -1:
                final_data[patno][2][date][index] = k[3]

        final_data[patno][3][date] = []
        for l in range(len(scode)):
            final_data[patno][3][date].append(0)
        for l in patient_data[patno][1][date][1]:
            index = find_scode(l[1])
            if index != -1:
                final_data[patno][3][date][index] += 1

#temp = final_data[list(final_data.keys())[0]]
#print(temp[1])
#print(len(temp))
#print(len(temp[2].keys()))
#print(len(temp[3].keys()))
#print(len(temp[2][list(temp[2].keys())[0]]))
#print(len(temp[3][list(temp[3].keys())[0]]))

print('final data making process finished')

import pandas as pd
import openpyxl

xlsx_dir = 'timeseries.xlsx'
csv_dir = 'timeseries.csv'

final_patno = []
final_weight = []
final_height = []
final_gender = []
final_age_year = []
final_age_month = []
final_admission = []
final_discharge = []
final_admission_date = []
final_date = []
final_exam = []
for i in range(12):
    final_exam.append([])

final_drug = []
for i in range(len(scode)):
    final_drug.append([])

# write all data needed to export
for patno in final_data.keys():
    date_length = len(final_data[patno][2].keys())

    final_patno.append(final_data[patno][0][0])
    final_weight.append(final_data[patno][0][2])
    final_height.append(final_data[patno][0][1])
    final_gender.append(final_data[patno][0][3])
    final_age_year.append(final_data[patno][0][4])
    final_age_month.append(final_data[patno][0][5])
    for i in range(date_length-1):
        final_patno.append(None)
        final_weight.append(None)
        final_height.append(None)
        final_gender.append(None)
        final_age_year.append(None)
        final_age_month.append(None)

    for a in range(len(final_data[patno][1])):
        final_admission.append(final_data[patno][1][a][1])
        final_discharge.append(final_data[patno][1][a][2])
        final_admission_date.append(final_data[patno][1][a][3])
    for b in range(date_length-len(final_data[patno][1])):
        final_admission.append(None)
        final_discharge.append(None)
        final_admission_date.append(None)

    for c in final_data[patno][2].keys():   # c : date
        final_date.append(c)
        for j in range(12):
            final_exam[j].append(final_data[patno][2][c][j])

        for k in range(len(scode)):
            final_drug[k].append(final_data[patno][3][c][k])

print('making export data process finished')

export_data = {'PATNO':final_patno,
               'HEIGHT':final_height,
               'WEIGHT':final_weight,
               'GENDER':final_gender,
               'AGE_YEAR':final_age_year,
               'AGE_MONTH':final_age_month,
               'ADMDT':final_admission,
               'DSCHDT':final_discharge,
               'DATEDIFF':final_admission_date,
               'DATE':final_date}
for i in range(12):
    export_data[exam_ord[i]] = final_exam[i]

for i in range(len(scode)):
    export_data[scode[i][1]] = final_drug[i]

df = pd.DataFrame(export_data)
print(df.shape)

df.to_csv(csv_dir, index_label=export_data.keys())
'''
df.to_excel(xlxs_dir, # directory and file name to write
            sheet_name = 'Sheet1',
            na_rep = '',
            float_format = "%.2f",
            header = True,
            startrow = 0,
            startcol = 0,
            #engine = 'xlsxwriter',
            freeze_panes = (2, 0)
            )'''
print('code finished')
