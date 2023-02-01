import pandas as pd
import pymysql  # use pip to install pymysql
import time
import re
import numpy as np
from enum import Enum

class Mkfg(Enum):
    PRESCRIBED = 1
    RETRIEVED = -1


class PN(Enum):
    POSITIVE = 1
    NEGATIVE = 0


class PEN(Enum):
    POSITIVE = 1
    EQUIVOCAL = 0
    NEGATIVE = -1


class ValueType(Enum):
    NUMERIC = 0
    PN = 1
    PEN = 2


# exam_ord : whole ordcode used for exam data
# exam_for_column : whole ordcode used only for column of timeseries data
# exam_pn_ord : ordcode that has PN for value type
# exam_pen_ord : ordcode that has PEN for value type
# exam_num_ord : ordcode that has numeric value for value type
# grouping_needed : each group of ordcode that needs to be grouped to one(first ordcode as representative ordcode)
# pen_case : whole case of values for PEN value type
# p_case: whole case of values for P value type
# e_case: whole case of values for E value type
# n_case: whole case of values for N value type
exam_pn_ord = ('C4802001', 'C4812052', 'C4872001')
exam_pen_ord = ()
exam_num_ord = ('B1060001', 'B1540001A', 'B1540001B', 'B2570001', 'B2580001', 'B2602001', 'B2710001',
                'C2210001', 'C3720001', 'C3730001', 'C3750001', 'C3750001A')
grouping_needed = (('C4802001', 'C4802002', 'C4802051', 'C4803001'), ('C4872001', 'C4872002', 'CZ492001'))
pen_case = ('Positive', 'Pos', 'P', 'W.Pos', 'Negative', 'Neg', 'N', 'Equivocal', 'E')
p_case = ('Positive', 'Pos', 'P', 'W.Pos')
e_case = ('Equivocal', 'E')
n_case = ('Neg', 'N')
exam_ord = ('B1060001', 'B1540001A', 'B1540001B', 'B2570001', 'B2580001', 'B2602001', 'B2710001', 'C2210001',
            'C3720001', 'C3730001', 'C3750001', 'C3750001A', 'C4802001', 'C4802002', 'C4802051', 'C4803001',
            'C4812052', 'C4872001', 'C4872002', 'CZ492001')
exam_for_column = ('B1060001', 'B1540001A', 'B1540001B', 'B2570001', 'B2580001', 'B2602001', 'B2710001',
                   'C2210001', 'C3720001', 'C3730001', 'C3750001', 'C3750001A', 'C4802001(PN)',
                   'C4802001(value)', 'C4812052(PN)', 'C4812052(value)', 'C4872001(PN)', 'C4872001(value)')


def pn(rsltnum):
    if rsltnum in p_case or 'P' in rsltnum:
        return PN.POSITIVE
    if rsltnum in n_case or 'N' in rsltnum:
        return PN.NEGATIVE
    return None


def pen(rsltnum):
    if rsltnum in p_case or 'P' in rsltnum:
        return PEN.POSITIVE
    if rsltnum in n_case or 'N' in rsltnum:
        return PEN.NEGATIVE
    if rsltnum in e_case or 'E' in rsltnum:
        return PEN.EQUIVOCAL
    return None


class Exam:
    def __init__(self, ordcode, meddate, rsltnum):
        for group in grouping_needed:
            if ordcode in group:
                self.ordcode = group[0]
            else:
                self.ordcode = ordcode
        meddate = time.strptime(meddate, '%Y%m%d')
        if self.ord_value() == ValueType.NUMERIC:
            self.rsltnum = {meddate: [rsltnum, [rsltnum]]}
        elif self.ord_value() == ValueType.PN:
            if pn(rsltnum) == PN.POSITIVE:
                self.rsltnum = {meddate: [PN.POSITIVE, [re.findall('\d+\.+\d', rsltnum)[0]]]}
            elif pn(rsltnum) == PN.NEGATIVE:
                self.rsltnum = {meddate: [PN.NEGATIVE, [re.findall('\d+\.+\d', rsltnum)[0]]]}
            else:
                print('error : no pn value', self.ordcode, meddate, self.rsltnum)
        elif self.ord_value() == ValueType.PEN:
            if pen(rsltnum) == PEN.POSITIVE:
                self.rsltnum = {meddate: [PEN.POSITIVE, [re.findall('\d+\.+\d', rsltnum)[0]]]}
            elif pen(rsltnum) == PEN.NEGATIVE:
                self.rsltnum = {meddate: [PEN.NEGATIVE, [re.findall('\d+\.+\d', rsltnum)[0]]]}
            elif pen(rsltnum) == PEN.EQUIVOCAL:
                self.rsltnum = {meddate: [PEN.EQUIVOCAL, [re.findall('\d+\.+\d', rsltnum)[0]]]}
            else:
                print('error : no pen value', self.ordcode, meddate, self.rsltnum)
        else:
            print('error : no value type', self.ordcode, meddate, self.rsltnum)

    def ord_value(self):
        if self.ordcode in exam_num_ord:
            return ValueType.NUMERIC
        if self.ordcode in exam_pn_ord:
            return ValueType.PN
        if self.ordcode in exam_pen_ord:
            return ValueType.PEN
        return None

    def add_rsltnum(self, meddate, rsltnum):
        meddate = time.strptime(meddate, '%Y%m%d')
        if meddate in self.rsltnum.keys():
            if self.ord_value() == ValueType.NUMERIC:
                self.rsltnum[meddate][1].append(rsltnum)
                self.rsltnum[meddate][0] = np.mean(self.rsltnum[meddate][1])
            elif self.ord_value() == ValueType.PN:
                if pn(rsltnum) == PN.POSITIVE:
                    if self.rsltnum[meddate][0] != PN.POSITIVE:
                        print('different PN value', self.ordcode, meddate)
                        return None
                    self.rsltnum[meddate][1].append(re.findall('\d+\.+\d', rsltnum)[0])
                elif pn(rsltnum) == PN.NEGATIVE:
                    if self.rsltnum[meddate][0] != PN.NEGATIVE:
                        print('different PN value', self.ordcode, meddate)
                        return None
                    self.rsltnum[meddate][1].append(re.findall('\d+\.+\d', rsltnum)[0])
                else:
                    print('error', self.ordcode, meddate, rsltnum)
            elif self.ord_value() == ValueType.PEN:
                if pen(rsltnum) == PEN.POSITIVE:
                    if self.rsltnum[meddate][0] != PEN.POSITIVE:
                        print('different PEN value', self.ordcode, meddate)
                        return None
                    self.rsltnum[meddate][1].append(re.findall('\d+\.+\d', rsltnum)[0])
                elif pen(rsltnum) == PEN.NEGATIVE:
                    if self.rsltnum[meddate][0] != PEN.NEGATIVE:
                        print('different PEN value', self.ordcode, meddate)
                        return None
                    self.rsltnum[meddate][1].append(re.findall('\d+\.+\d', rsltnum)[0])
                elif pen(rsltnum) == PEN.EQUIVOCAL:
                    if self.rsltnum[meddate][0] != PEN.EQUIVOCAL:
                        print('different PEN value', self.ordcode, meddate)
                        return None
                    self.rsltnum[meddate][1].append(re.findall('\d+\.+\d', rsltnum)[0])
                else:
                    print('error : no PEN value', self.ordcode, meddate, rsltnum)
            else:
                print('error : no value type', self.ordcode, meddate, rsltnum)
        else:
            if self.ord_value() == ValueType.NUMERIC:
                self.rsltnum[meddate] = [rsltnum, [rsltnum]]
            elif self.ord_value() == ValueType.PN:
                if pn(rsltnum) == PN.POSITIVE:
                    self.rsltnum[meddate] = [PN.POSITIVE, [re.findall('\d+\.+\d', rsltnum)[0]]]
                elif pn(rsltnum) == PN.NEGATIVE:
                    self.rsltnum[meddate] = [PN.NEGATIVE, [re.findall('\d+\.+\d', rsltnum)[0]]]
                else:
                    print('error : no pn value', self.ordcode, meddate, self.rsltnum)
            elif self.ord_value() == ValueType.PEN:
                if pen(rsltnum) == PEN.POSITIVE:
                    self.rsltnum[meddate] = [PEN.POSITIVE, [re.findall('\d+\.+\d', rsltnum)[0]]]
                elif pen(rsltnum) == PEN.NEGATIVE:
                    self.rsltnum[meddate] = [PEN.NEGATIVE, [re.findall('\d+\.+\d', rsltnum)[0]]]
                elif pen(rsltnum) == PEN.EQUIVOCAL:
                    self.rsltnum[meddate] = [PEN.EQUIVOCAL, [re.findall('\d+\.+\d', rsltnum)[0]]]
                else:
                    print('error : no pen value', self.ordcode, meddate, self.rsltnum)
            else:
                print('error : no value type', self.ordcode, meddate, self.rsltnum)


class Drug:
    def __init__(self, ordcode, scode):
        self.ordcode = ordcode
        self.scode = scode
        self.duration = {}
        self.result = {}

    def add_drug_record(self, meddate, ordseqno, packqty, cnt, day, mkfg):
        meddate = time.strptime(meddate, '%Y%m%d')
        try:
            if mkfg in ['D', 'B', 'C']:
                return None
            if mkfg in ['N', 'P']:
                mkfg = Mkfg.PRESCRIBED
            else:
                mkfg = Mkfg.RETRIEVED
            info = [int(ordseqno), abs(float(packqty)*int(cnt)*int(day)), float(packqty), int(cnt), int(day), mkfg]
            if meddate not in self.duration.keys():
                self.duration[meddate] = [info]
            else:
                self.duration[meddate].append(info)
        except TypeError:
            print('error in type casting drug info', self.ordcode, meddate, ordseqno, packqty, cnt, day)

    def process_record_by_date(self):
        keys = list(self.duration.keys())
        keys = sorted(keys, reverse=True)
        self.duration = {i:self.duration[i] for i in keys}
        for date in self.duration.keys():
            temp = []
            for record in self.duration[date]:
                if record[5] == Mkfg.PRESCRIBED:
                    temp.append(record)
                else:
                    for i in temp:
                        if i[1] == record[1]:
                            temp.remove(i)
                            break
                    else:
                        temp.append(record)
            for i in range(len(temp)-1, 0, -1):
                if temp[i][5] == Mkfg.PRESCRIBED:
                    if temp[i-1][5] == Mkfg.PRESCRIBED:
                        temp[i-1][1] += temp[i][1]
                        temp[i-1][4] += temp[i][4]
                    else:
                        continue
                else:
                    temp[i-1][1] -= temp[i][1]
            self.duration[date] = temp
        keys = list(self.duration.keys())
        keys = sorted(keys)
        self.duration = {i: self.duration[i] for i in keys}

    def make_result(self):
        current_meddate = list(self.duration.keys())[0]
        rslt_total = 0
        rslt_day = 7
        for date in self.duration.keys():
            if abs(date - current_meddate) <= rslt_day:
                for record in self.duration[date]:
                    rslt_total += record[5] * record[1]
                    rslt_day += record[4]
            else:
                self.result[current_meddate] = [rslt_total, rslt_day-7]
                rslt_total = 0
                rslt_day = 7
                current_meddate = date
                for record in self.duration[date]:
                    rslt_total += record[5] * record[1]
                    rslt_day += record[4]

class Physical:
    def __init__(self, meddate, birthyear, height, weight):
        meddate = time.strptime(meddate, '%Y%m%d')
        self.birthyear = time.strptime(birthyear, '%Y%m%d').tm_year
        self.age = {meddate: abs(meddate.tm_year - self.birthyear)}
        self.height = {meddate: height}
        self.weight = {meddate: weight}
        self.bmi = {meddate: weight/pow(height/100, 2)}

    def add_physical(self, meddate, height, weight):
        meddate = time.strptime(meddate, '%Y%m%d')
        self.age[meddate] = abs(meddate.tm_year - self.birthyear)
        self.height[meddate] = height
        self.weight[meddate] = weight


class Patient:
    def __init__(self, patno, gender, birthyear):
        self.patno = patno
        self.gender = gender
        self.birthyear = time.strptime(birthyear, '%Y%m%d')
        self.physical = None
        self.exam = {}
        self.drug = {}

    def init_physical(self, meddate, height, weight):
        self.physical = Physical(meddate, self.birthyear, height, weight)

    def new_physical(self, meddate, height, weight):
        self.physical.add_physical(meddate, height, weight)

    def new_exam(self, ordcode, meddate, rsltnum):
        if ordcode not in self.exam.keys():
            self.exam[ordcode] = Exam(ordcode, meddate, rsltnum)
        else:
            self.exam[ordcode].add_rsltnum(meddate, rsltnum)

    def new_drug(self, ordcode, meddate, day, packqty, cnt, mkfg, scode):
        if ordcode not in self.drug.keys():
            self.drug[ordcode] = Drug(ordcode, meddate, day, packqty, cnt, scode)
        else:
            self.drug[ordcode].cal_duration(meddate, day, packqty, cnt, mkfg)

#
# print('program started')
# conn = pymysql.connect(host='203.252.105.181',
#                        user='yohansohn',  # user id
#                        password='johnsohn12',  # user password
#                        db='DR_ANS_AJCO',
#                        charset='utf8')
# cursor = conn.cursor()
#
# # 0: SCODE
# # maybe need ordcode instead of scode
# # only scode that has more than 100 patients are prescript and has DCYN as N
# # remove scode where product name contains '원외', '자가', '임상'
# sql = '''SELECT SCODE
# FROM common_drug_master
# JOIN drug ON drug.ORDCODE = common_drug_master.ORDCODE
# WHERE PATNO like '5%' AND DCYN ='N'
# AND common_drug_master.`PRODENNM` not like '%원외%'
# AND common_drug_master.`PRODENNM` not like '%자가%'
# AND common_drug_master.`PRODENNM` not like '%임상%'
# GROUP BY SCODE
# HAVING COUNT(DISTINCT PATNO) >= 100 AND SCODE is not null;'''
# cursor.execute(sql)
# result = cursor.fetchall()
# drug_raw = list(result)
# drug_scode = []
# for i in drug_raw:
#     drug_scode.append(i[0])
#
# # 0: PATNO, 1: HEIGHT, 2: WEIGHT    # old data
# # 0:patno 1:gender 2:birthyear      # new data
# sql = '''select patno, meddate, height, weight from body_measure
# where patno>5000000 and patno<6000000;'''
# cursor.execute(sql)
# result = cursor.fetchall()
# patient_data = {}
# for data in result:
#     patient_data[data[0]] = Patient(data[0], data[1], data[2])
#
# # 0:patno 1:meddate 2:height 3:weight
# sql = ''''''
# cursor.execute(sql)
# result = cursor.fetchall()
# for data in result:
#     if patient_data[data[0]].physical is None:
#         patient_data[data[0]].init_physical(data[1], data[2], data[3])
#     else:
#         patient_data[data[0]].new_physical(data[1], data[2], data[3])
#
# # 0: PATNO, 1: ordcode, 2: orddate, 3: rsltnum  # old data
# # 0:patno 1:ordcode 2:orddate 3:rsltnum  # new data
# sql = '''select patno, ordcode, orddate, rsltnum from `exam`
# where `PATNO`>5000000 and `PATNO`<6000000;'''
# cursor.execute(sql)
# result = cursor.fetchall()
# for data in result:
#     patient_data[data[0]].new_exam(data[1], data[2], data[3])
#
# # 0: patno, 1: ordcode, 2: orddate, 3: patfg, 4: packqty, 5: cnt, 6: day, 7: dcyn, 8: mkfg 9: scode # old data
# # 0:patno 1:ordcode 2:meddate 3:day 4:packqty 5:cnt 6:mkfg 7:scode # new data
# sql = '''select
#     patno, drug.ordcode, orddate, patfg, packqty, cnt, day, dcyn, mkfg, scode
# from
#     `drug` join common_drug_master cdm on drug.ORDCODE = cdm.ORDCODE
# where
#     SCODE IS NOT NULL AND dcyn = 'N' AND `PATNO` >5000000 and `PATNO`<6000000;'''
# cursor.execute(sql)
# result = cursor.fetchall()
# for data in result:
#     patient_data[data[0]].new_drug(data[1], data[2], data[3], data[4], data[5], data[6], data[7])
#
# print('exam and drug data classified by patno')
#
# export_patno = []
# export_gender = []
# export_meddate = []
# export_age = []
# export_height = []
# export_weight = []
# export_bmi = []
# export_exam = {}
# for ordcode in exam_for_column:
#     export_exam[ordcode] = []
# export_drug = {}
# for scode in drug_scode:
#     export_drug[scode] = []
#
# for i in patient_data.keys():
#     export_patno.append(i)
#     export_gender.append(patient_data[i].gender)
#     date = []
#     for exam in patient_data[i].exam:
#         for meddate in exam.rsltnum.keys():
#             if meddate not in date:
#                 date.append(meddate)
#     for drug in patient_data[i].drug:
#         for meddate in drug.duration.keys():
#             if meddate not in date:
#                 date.append(meddate)
#     date.sort()
#     for meddate in date:
#         export_meddate.append(meddate)
#         for ordcode in exam_for_column:
#             if meddate in patient_data[i].exam[ordcode].rsltnum.keys():
#                 export_exam[ordcode].append()
