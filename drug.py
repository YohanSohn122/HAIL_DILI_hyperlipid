# drug with SCODE
# common : ORDCODE, ORDNAME, SCODE, IRGDNAME from common_drug_master
# drug : whole data
# 0: PATNO, 1: ORDCODE, 2: SCODE, 3: OQTY, 4: PACKQTY, 5: CNT, 6: SDATE(ORDDATE)
# 7: DAY, 8: EDATE(ORDDATE+DAY), 9: 'PACKQTY'*'CNT'*'DAY', 10: DCYN, 11: MKFG
import pymysql      # use pip to install pymysql
print('program started')
conn = pymysql.connect(host='',
                       user='',        #userid
                       password='',   #userpassword
                       db='',
                       charset='utf8')
cursor = conn.cursor()
sql = '''select `ORDCODE`,`ORDNAME`,`SCODE`,`IRGDNAME`  from `common_drug_master`;'''
cursor.execute(sql)
result = cursor.fetchall()
common = list(result)

sql = '''SELECT `PATNO`, `drug`.`ORDCODE`, `common_drug_master`.`SCODE`,`OQTY`, `PACKQTY`, `CNT`, `ORDDATE` AS `SDATE(ORDDATE)`, `DAY`, DATE_FORMAT(DATE_ADD(STR_TO_DATE(`ORDDATE`, '%Y%m%d'), INTERVAL `DAY` DAY), '%Y%m%d') AS `EDATE(ORDDATE + DAY)`,`PACKQTY` * `CNT` * `DAY`, `DCYN`, `MKFG`
FROM `drug` INNER JOIN `common_drug_master` ON `drug`.`ORDCODE`=`common_drug_master`.`ORDCODE`
WHERE `SCODE` IS NOT NULL AND `PATNO` > 5000000 AND SIGN(`PACKQTY`) <> -1 AND SIGN(`CNT`) <> -1 AND SIGN(`DAY`) <> -1 AND `DCYN` = 'N';'''
cursor.execute(sql)
result = cursor.fetchall()      # result is given as tuple
drug = list(result)
for i in range(len(drug)):      # each data's type is tuple so type chasting to list is needed
    drug[i] = list(drug[i])

data_by_patno_ordcode = {}
for i in drug:    
    key_string = str(i[0]) + str(i[1])
    if key_string in data_by_patno_ordcode.keys():
        data_by_patno_ordcode[key_string].append(i)
    else:
        data_by_patno_ordcode[key_string] = [i]
print('all data read')
'''
print('Data that has other value for MKFG than NPBCDR')
def remove_data(key):
    patno = key[:7]
    ordcode = key[7:]
    for i in drug:
        if i[0]==patno and i[1]==ordcode:
            

for i in data_by_patno_ordcode.keys():
    np = 0
    dcbr = 0
    for j in data_by_patno_ordcode[i]:
        if j[11] in 'NP':
            np += j[9]
        elif j[11] in 'DCBR':
            dcbr += j[9]
        else:
            print(j)
    if np < dcbr:
        remove_data(i)
'''
# drug_dic_by_scode : data grouped by scode
drug_dic_by_scode = {}

for i in drug:
    #print(i)
    if i[2] in drug_dic_by_scode.keys():
        drug_dic_by_scode[i[2]].append(i)
    else:
        drug_dic_by_scode[i[2]] = [i]
keys = list(drug_dic_by_scode.keys())
keys.sort()
drug_dic_by_scode = {i: drug_dic_by_scode[i] for i in keys}
print('all data by scode classified')
# final_ordcode : list of number of ORDCODE for a SCODE(used to make dataframe)
# final_patient : list of number of PATNO for a SCODE(used to make dataframe)
# final_duration : list of Duration(used to make dataframe)
# final_avg_dur : list of AVG_Duration(used to make dataframe)
final_ordcode = []
final_patient = []
final_duration_np = []
final_duration_dcbr = []
final_avg_dur_np = []
final_avg_dur_dcbr = []
for i in drug_dic_by_scode.keys():
    distinct_ord = []
    distinct_patient = []
    np = 0
    dcbr = 0
    for j in drug_dic_by_scode[i]:
        if j[1] not in distinct_ord:
            distinct_ord.append(j[1])
        if j[0] not in distinct_patient:
            distinct_patient.append(j[0])
        if j[11] in 'NP':
            np += j[9]
        elif j[11] in 'DCBR':
            dcbr += j[9]
    final_ordcode.append(len(distinct_ord))
    final_patient.append(len(distinct_patient))
    final_duration_np.append(np)
    final_duration_dcbr.append(dcbr)
    final_avg_dur_np.append(np/len(distinct_patient))
    final_avg_dur_dcbr.append(dcbr/len(distinct_patient))
print('final data making process done')
import pandas as pd
import os
import openpyxl

file_nm = 'drug.xlsx'
xlxs_dir = os.path.join(file_nm)

df = pd.DataFrame({'scode':list(drug_dic_by_scode.keys()),
                   'ordcode':final_ordcode,
                   'patient':final_patient,
                   'duration_np':final_duration_np,
                   'duration_dcbr':final_duration_dcbr,
                   'avg_duration_np':final_avg_dur_np,
                   'avg_duration_dcbr':final_duration_dcbr})

df.to_excel(xlxs_dir, # directory and file name to write
            sheet_name = 'Sheet1', 
            na_rep = 'NaN', 
            float_format = "%.2f", 
            header = True,
            startrow = 0,
            startcol = 0,
            #engine = 'xlsxwriter', 
            freeze_panes = (2, 0)
            ) 
print('code finished')