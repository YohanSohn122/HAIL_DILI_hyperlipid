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

# 0: ordcode, 1: scode, 2: irgdname
sql = '''SELECT drug.ORDCODE, common_drug_master.SCODE, common_drug_master.IRGDNAME FROM drug JOIN `common_drug_master` ON drug.ORDCODE = `common_drug_master`.ORDCODE
WHERE drug.PATNO>5000000 AND common_drug_master.SCODE is not null 
GROUP BY drug.ORDCODE ORDER BY common_drug_master.SCODE ASC;'''
cursor.execute(sql)
result = cursor.fetchall()
common = list(result)
irgd_by_scode = {}
for i in common:
    if i[1] in irgd_by_scode.keys():
        irgd_by_scode[i[1]].append(i[2])
    else:
        irgd_by_scode[i[1]] = [i[2]]

sql = '''SELECT SCODE
FROM common_drug_master
JOIN drug ON drug.ORDCODE = common_drug_master.ORDCODE
WHERE PATNO like '5%' AND DCYN ='N'
AND common_drug_master.`PRODENNM` not like '%원외%'
AND common_drug_master.`PRODENNM` not like '%자가%'
AND common_drug_master.`PRODENNM` not like '%임상%'
GROUP BY SCODE
HAVING COUNT(DISTINCT PATNO) >= 100 AND SCODE is not null;'''
cursor.execute(sql)
result = cursor.fetchall()
valid_scode = []
for i in result:
    valid_scode.append(i[0])

# 0: patno, 1: ordcode, 2: scode, 3: OQTY, 4: PACKQTY, 5: CNT, 6: ORDDATE, 7: DAY, 8: EDATE
# 9: PACKQTY*CNT*DAY, 10: DCYN, 11: MKFG
sql = '''SELECT `PATNO`, `drug`.`ORDCODE`, `common_drug_master`.`SCODE`,`OQTY`, `PACKQTY`, `CNT`, `ORDDATE` AS `SDATE(ORDDATE)`, `DAY`, DATE_FORMAT(DATE_ADD(STR_TO_DATE(`ORDDATE`, '%Y%m%d'), INTERVAL `DAY` DAY), '%Y%m%d') AS `EDATE(ORDDATE + DAY)`,`PACKQTY` * `CNT` * `DAY`, `DCYN`, `MKFG`
FROM `drug` INNER JOIN `common_drug_master` ON `drug`.`ORDCODE`=`common_drug_master`.`ORDCODE`
WHERE `SCODE` IS NOT NULL AND `PATNO` > 5000000 and `DCYN` = 'N';'''
cursor.execute(sql)
drug_raw = cursor.fetchall()      # result is given as tuple

# key: patno
# item: { key: date, item: { key: scode, item: [data] } }
drug_by_patient = {}
total_count = 0
total_dur = 0
for data in drug_raw:
    if data[2] in valid_scode:
        total_count += 1
        if data[11] in ['N', 'P']:
            total_dur += data[9]
        else:
            if data[9] > 0:
                total_dur -= data[9]
            else:
                total_dur += data[9]
        if data[0] in drug_by_patient.keys():
            if data[6] in drug_by_patient[data[0]].keys():
                if data[2] in drug_by_patient[data[0]][data[6]].keys():
                    drug_by_patient[data[0]][data[6]][data[2]].append(data)
                else:
                    drug_by_patient[data[0]][data[6]][data[2]] = [data]
            else:
                drug_by_patient[data[0]][data[6]] = {data[2]: [data]}
        else:
            drug_by_patient[data[0]] = {data[6]: {data[2]: [data]}}
# print(drug_by_patient[list(drug_by_patient.keys())[0]])

# key: scode
# item: [ count_np, count_final_valid, count_final_valid_with_zero, duration_np, duration_final_valid ]
final_by_scode = {}
for scode in valid_scode:
    final_by_scode[scode] = [0, 0, 0, 0, 0]
for patno in drug_by_patient.keys():
    for date in drug_by_patient[patno].keys():
        for scode in drug_by_patient[patno][date].keys():
            dur = 0
            for data in drug_by_patient[patno][date][scode]:
                if data[11] in ['N', 'P']:
                    final_by_scode[scode][0] += 1
                    final_by_scode[scode][3] += data[9]
                    dur += data[9]
                else:
                    if data[9] > 0:
                        dur -= data[9]
                    else:
                        dur += data[9]
            if dur >= 0:
                final_by_scode[scode][2] += 1
                if dur > 0:
                    final_by_scode[scode][1] += 1
                    final_by_scode[scode][4] += dur

keys = list(final_by_scode.keys())
keys.sort()
final_by_scode = {i:final_by_scode[i] for i in keys}

print('final data making process done')

import pandas as pd
import openpyxl

csv_dir = 'drug_only_valid.csv'

final_scode = list(final_by_scode.keys())
final_irgd = []
final_np_count = []
final_valid_count = []
final_valid_zero_count = []
final_np_dur = []
final_valid_dur = []
total_np_count = 0
total_final_count = 0
total_final_count_with_zero = 0
total_np_dur = 0
total_final_dur = 0

for scode in final_by_scode.keys():
    final_irgd.append(irgd_by_scode[scode])
    final_np_count.append(final_by_scode[scode][0])
    total_np_count += final_by_scode[scode][0]
    final_valid_count.append(final_by_scode[scode][1])
    total_final_count += final_by_scode[scode][1]
    final_valid_zero_count.append(final_by_scode[scode][2])
    total_final_count_with_zero += final_by_scode[scode][2]
    final_np_dur.append(final_by_scode[scode][3])
    total_np_dur += final_by_scode[scode][3]
    final_valid_dur.append(final_by_scode[scode][4])
    total_final_dur += final_by_scode[scode][4]

df = pd.DataFrame({'scode': final_scode,
                   'IRGDNAME': final_irgd,
                   'count(only NP)': final_np_count,
                   'count(with DCBR without zero duration)': final_valid_count,
                   'count(with DCBR with zero duration)': final_valid_zero_count,
                   'duration(only NP)': final_np_dur,
                   'duration(with DCBR)': final_valid_dur
                   })

df.to_csv(csv_dir)

print('total count(DCYN = N)', total_count, 'total duration', total_dur)
print('total NP', total_np_count, 'total final', total_final_count, 'total final with zero', total_final_count_with_zero)
print('total NP dur', total_np_dur, 'total final dur', total_final_dur)
print('code finished')