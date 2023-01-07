# drug with SCODE
# drug : whole data
# 0: PATNO, 1: ORDCODE, 2: SCODE, 3: OQTY, 4: PACKQTY, 5: CNT, 6: SDATE(ORDDATE)
# 7: DAY, 8: EDATE(ORDDATE+DAY), 9: 'PACKQTY'*'CNT'*'DAY', 10: DCYN, 11: MKFG
import pymysql      # use pip to install pymysql
conn = pymysql.connect(host='203.252.105.181',
                       user='yohansohn',        #userid
                       password='johnsohn12',   #userpassword
                       db='DR_ANS_AJCO',
                       charset='utf8')
cursor = conn.cursor()
sql = '''
            SELECT `PATNO`, `drug`.`ORDCODE`, `common_drug_master`.`SCODE`,`OQTY`, `PACKQTY`, `CNT`, `ORDDATE` AS `SDATE(ORDDATE)`, `DAY`, DATE_FORMAT(DATE_ADD(STR_TO_DATE(`ORDDATE`, '%Y%m%d'), INTERVAL `DAY` DAY), '%Y%m%d') AS `EDATE(ORDDATE + DAY)`,`PACKQTY` * `CNT` * `DAY`, `DCYN`, `MKFG`
FROM `drug` INNER JOIN `common_drug_master` ON `drug`.`ORDCODE`=`common_drug_master`.`ORDCODE`
WHERE `SCODE` IS NOT NULL AND `PATNO` > 5000000 AND SIGN(`PACKQTY`) <> -1 AND SIGN(`CNT`) <> -1 AND SIGN(`DAY`) <> -1 AND `DCYN` = 'N' AND ((`MKFG` = 'N') OR (`MKFG` = 'P'));
        '''
cursor.execute(sql)
result = cursor.fetchall()      # result is given as tuple
drug = list(result)
for i in range(len(drug)):      # each data's type is tuple so type chasting to list is needed
    drug[i] = list(drug[i])

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

# final_ordcode : list of number of ORDCODE for a SCODE(used to make dataframe)
# final_patient : list of number of PATNO for a SCODE(used to make dataframe)
# final_duration : list of Duration(used to make dataframe)
# final_avg_dur : list of AVG_Duration(used to make dataframe)
final_ordcode = []
final_patient = []
final_duration = []
final_avg_dur = []
for i in drug_dic_by_scode.keys():
    distinct_ord = []
    distinct_patient = []
    duration = 0
    for j in drug_dic_by_scode[i]:
        if j[1] not in distinct_ord:
            distinct_ord.append(j[1])
        if j[0] not in distinct_patient:
            distinct_patient.append(j[0])
        duration += j[9]
    final_ordcode.append(len(distinct_ord))
    final_patient.append(len(distinct_patient))
    final_duration.append(duration)
    final_avg_dur.append(duration/len(distinct_patient))
#    print(i,'# of different ORDCODE :',len(distinct_ord),
#          ', # of patients :',len(distinct_patient),
#          ', duration :',duration,
#          ', duration/patients :',duration/len(distinct_patient))

import pandas as pd
import os

file_nm = 'drug.xlsx'
xlxs_dir = os.path.join(file_nm)

df = pd.DataFrame({'scode':list(drug_dic_by_scode.keys()),
                   'ordcode':final_ordcode,
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
