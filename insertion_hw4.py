import os
import sys
import csv
import psycopg2 as sql


def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def filter_data(fields, row={}):

#todel = ['Column_Order','Description','Unit']

    #    new_fields = [field for field in fields if not field in todel]
    #  new_data = {key: val for key, val in row.items() if not key in todel}
    new_fields = [field for field in fields]
    new_data = {key: val for key, val in row.items()}

    return new_fields, new_data


def make_datatypes(fields, rows=[]):

    mappings = {'MSN': 'VARCHAR(10)',
                'YYYYMM': 'INT',
                'Value': 'TEXT',
                'Column_Order' : 'VARCHAR(2)',
                'Description':'VARCHAR(100)',
                'Unit':'VARCHAR(100)'}

    types = [mappings.get(field, 'TEXT') for field in fields]

    return types

def make_datatypes_per(fields, rows=[]):

    mappings = {'HOUSEID' : 'VARCHAR(8)', 'PERSONID': 'VARCHAR(2)', 'VARSTRAT' : 'INT', 'WTPERFIN' : 'FLOAT', 'SFWGT' : 'FLOAT', 'HH_HISP' : 'VARCHAR(2)', 'HH_RACE' : 'VARCHAR(2)','DRVRCNT' : 'INT', 'HHFAMINC' : 'VARCHAR(2)', 'HHSIZE' : 'INT', 'HHVEHCNT' : 'INT', 'NUMADLT' : 'INT', 'WRKCOUNT' : 'INT', 'FLAG100' : 'VARCHAR(2)', 'LIF_CYC' : 'VARCHAR(2)', 'CNTTDTR' : 'INT', 'BORNINUS' : 'VARCHAR(2)', 'CARRODE' : 'INT', 'CDIVMSAR' : 'VARCHAR(2)','CENSUS_D' : 'VARCHAR(2)','CENSUS_R' : 'VARCHAR(2)','CONDNIGH' : 'VARCHAR(2)', 'CONDPUB' : 'VARCHAR(2)', 'CONDRIDE' : 'VARCHAR(2)', 'CONDRIVE' : 'VARCHAR(2)', 'CONDSPEC' : 'VARCHAR(2)', 'CONDTAX' : 'VARCHAR(2)', 'CONDTRAV' : 'VARCHAR(2)', 'DELIVER' : 'INT', 'DIARY' : 'VARCHAR(2)', 'DISTTOSC' : 'VARCHAR(2)', 'DRIVER' : 'VARCHAR(2)', 'DTACDT' : 'VARCHAR(2)', 'DTCONJ' : 'VARCHAR(2)', 'DTCOST' : 'VARCHAR(2)', 'DTRAGE' : 'VARCHAR(2)', 'DTRAN' : 'VARCHAR(2)', 'DTWALK' : 'VARCHAR(2)', 'EDUC' : 'VARCHAR(2)', 'EVERDROV' : 'VARCHAR(2)', 'FLEXTIME' : 'VARCHAR(2)', 'FMSCSIZE' : 'INT', 'FRSTHM' : 'VARCHAR(2)', 'FXDWKPL' : 'VARCHAR(2)', 'GCDWORK' : 'FLOAT', 'GRADE' : 'VARCHAR(2)', 'GT1JBLWK' : 'VARCHAR(2)', 'HHRESP' : 'VARCHAR(2)', 'HHSTATE' : 'VARCHAR(2)', 'HHSTFIPS' : 'VARCHAR(2)','ISSUE' : 'VARCHAR(2)', 'OCCAT' : 'VARCHAR(2)', 'LSTTRDAY' : 'INT','MCUSED' : 'INT', 'MEDCOND' : 'VARCHAR(2)', 'MEDCOND6' : 'VARCHAR(2)', 'MOROFTEN' : 'VARCHAR(2)', 'MSACAT' : 'VARCHAR(2)', 'MSASIZE' : 'VARCHAR(2)', 'NBIKETRP' : 'INT', 'NWALKTR' : 'INT', 'OUTCNTRY' : 'VARCHAR(2)', 'OUTOFTWN' : 'VARCHAR(2)', 'PAYPROF' : 'VARCHAR(2)', 'PRMACT' : 'VARCHAR(2)', 'PROXY' : 'VARCHAR(2)', 'PTUSED' : 'INT', 'PURCHASE' : 'INT', 'R_AGE' : 'INT', 'R_RELAT' : 'VARCHAR(2)', 'R_SEX' : 'VARCHAR(2)', 'RAIL' : 'VARCHAR(2)', 'SAMEPLC' : 'VARCHAR(2)', 'SCHCARE' : 'VARCHAR(2)', 'SCHCRIM' : 'VARCHAR(2)', 'SCHDIST' : 'VARCHAR(2)', 'SCHSPD' : 'VARCHAR(2)', 'SCHTRAF' : 'VARCHAR(2)', 'SCHTRN1' : 'VARCHAR(2)', 'SCHTRN2' : 'VARCHAR(2)', 'SCHTYP' : 'VARCHAR(2)', 'SCHWTHR' : 'VARCHAR(2)', 'SELF_EMP' : 'VARCHAR(2)', 'TIMETOSC' : 'INT', 'TIMETOWK' : 'INT', 'TOSCSIZE' : 'INT', 'TRAVDAY' : 'VARCHAR(2)', 'URBAN' : 'VARCHAR(2)', 'URBANSIZE' : 'VARCHAR(2)', 'URBRUR' : 'VARCHAR(2)', 'USEINTST' : 'VARCHAR(2)', 'USEPUBTR': 'VARCHAR(2)', 'WEBUSE' : 'VARCHAR(2)', 'WKFMHMXX' : 'INT', 'WKFTPT' : 'VARCHAR(2)', 'WKRMHM' : 'VARCHAR(2)', 'WKSTFIPS' : 'VARCHAR(2)', 'WORKER' : 'VARCHAR(2)', 'WRKTIME' : 'VARCHAR(8)', 'WRKTRANS' : 'VARCHAR(2)', 'YEARMILE' : 'INT', 'YRMLCAP' : 'VARCHAR(2)', 'YRTOUS' : 'INT','DISTTOWK' : 'FLOAT', 'TDAYDATE' : 'VARCHAR(8)', 'HOMEOWN' : 'VARCHAR(2)', 'HOMETYPE' : 'VARCHAR(2)', 'HBHUR' : 'VARCHAR(2)','HTRESDN' : 'VARCHAR(5)', 'HTHTNRNT' : 'VARCHAR(2)', 'HTPPOPDN' : 'VARCHAR(5)', 'HTEEMPDN' : 'VARCHAR(4)', 'HBRESDN' : 'VARCHAR(5)', 'HBHTNRNT' : 'VARCHAR(2)', 'HBPPOPDN' : 'VARCHAR(5)', 'HH_CBSA' : 'VARCHAR(5)', 'HHC_MSA' : 'VARCHAR(4)'}

    types = [mappings.get(field, 'TEXT') for field in fields]
    return types

def make_datatypes_day(fields, rows=[]):

    mappings = {'HOUSEID' : 'VARCHAR(8)', 'PERSONID': 'VARCHAR(2)', 'FRSTHM' : 'VARCHAR(2)', 'OUTOFTWN' : 'VARCHAR(2)', 'ONTD_P1' : 'VARCHAR(2)','ONTD_P2' : 'VARCHAR(2)', 'ONTD_P3' : 'VARCHAR(2)','ONTD_P4' : 'VARCHAR(2)','ONTD_P5' : 'VARCHAR(2)','ONTD_P6' : 'VARCHAR(2)','ONTD_P7' : 'VARCHAR(2)','ONTD_P8' : 'VARCHAR(2)', 'ONTD_P9' : 'VARCHAR(2)','ONTD_P10' : 'VARCHAR(2)','ONTD_P11' : 'VARCHAR(2)', 'ONTD_P12' : 'VARCHAR(2)','ONTD_P13' : 'VARCHAR(2)','ONTD_P14' : 'VARCHAR(2)','ONTD_P15' : 'VARCHAR(2)', 'TDCASEID' : 'VARCHAR(12)', 'HH_HISP' : 'VARCHAR(2)', 'HH_RACE' : 'VARCHAR(2)','DRIVER' : 'VARCHAR(2)', 'R_SEX' : 'VARCHAR(2)', 'WORKER' : 'VARCHAR(2)', 'DRVRCNT' : 'INT', 'HHFAMINC' : 'VARCHAR(2)', 'HHSIZE' : 'INT', 'HHVEHCNT' : 'INT', 'NUMADLT' : 'INT', 'FLAG100' : 'VARCHAR(2)', 'LIF_CYC' : 'VARCHAR(2)', 'TRIPPURP' : 'VARCHAR(8)','AWAYHOME' : 'VARCHAR(2)', 'CDIVMSAR' : 'VARCHAR(2)','CENSUS_D' : 'VARCHAR(2)','CENSUS_R' : 'VARCHAR(2)','DROP_PRK' : 'VARCHAR(2)', 'DRVR_FLG' : 'VARCHAR(2)', 'EDUC' : 'VARCHAR(2)', 'ENDTIME' : 'VARCHAR(4)','HH_ONTD' : 'INT', 'HHMEMDRV' : 'VARCHAR(2)', 'HHRESP' : 'VARCHAR(2)', 'HHSTATE' : 'VARCHAR(2)', 'HHSTFIPS' : 'VARCHAR(2)','INTSTATE' : 'VARCHAR(2)', 'MSACAT' : 'VARCHAR(2)', 'MSASIZE' : 'VARCHAR(2)', 'NONHHCNT' : 'INT', 'NUMONTRP' : 'INT','PAYTOLL' : 'VARCHAR(2)','PRMACT' : 'VARCHAR(2)','PROXY' : 'VARCHAR(2)','PSGR_FLG' : 'VARCHAR(2)','R_AGE' : 'INT','RAIL' : 'VARCHAR(2)', 'STRTTIME' : 'VARCHAR(4)', 'TRACC1' : 'VARCHAR(2)','TRACC2' : 'VARCHAR(2)','TRACC3' : 'VARCHAR(2)','TRACC4' : 'VARCHAR(2)','TRACC5' : 'VARCHAR(2)','TRACCTM' : 'INT', 'TRAVDAY' : 'VARCHAR(2)', 'TREGR1' : 'VARCHAR(2)','TREGR2' : 'VARCHAR(2)','TREGR3' : 'VARCHAR(2)','TREGR4' : 'VARCHAR(2)','TREGR5' : 'VARCHAR(2)','TREGRTM' : 'INT', 'TRPACCMP' : 'INT','TRPHHACC' : 'INT', 'TRPHHVEH' : 'VARCHAR(2)','TRPTRANS' : 'VARCHAR(2)','TRVL_MIN' : 'INT','TRVLCMIN' : 'INT', 'TRWAITTM' : 'INT', 'URBAN' : 'VARCHAR(2)', 'URBANSIZE' : 'VARCHAR(2)','URBRUR' : 'VARCHAR(2)', 'USEINTST' : 'VARCHAR(2)', 'USEPUBTR' : 'VARCHAR(2)', 'VEHID' : 'VARCHAR(2)', 'WHODROVE' : 'VARCHAR(2)','WHYFROM' : 'VARCHAR(2)','WHYTO' : 'VARCHAR(2)','WHYTRP1S' : 'VARCHAR(2)','WRKCOUNT' : 'INT',	'DWELTIME' : 'INT', 'WHYTRP90' : 'VARCHAR(2)','TDTRPNUM' : 'VARCHAR(12)','TDWKND' : 'VARCHAR(2)', 'TDAYDATE' : 'VARCHAR(8)','TRPMILES' : 'FLOAT', 'WTTRDFIN' : 'FLOAT', 'VMT_MILE' : 'FLOAT', 'PUBTRANS' : 'VARCHAR(2)', 'HOMEOWN' : 'VARCHAR(2)','HOMETYPE' : 'VARCHAR(2)','HBHUR' : 'VARCHAR(2)','HTRESDN' : 'VARCHAR(5)','HTHTNRNT' : 'VARCHAR(2)','HTPPOPDN' : 'VARCHAR(5)','HTEEMPDN' : 'VARCHAR(4)', 'HBRESDN' : 'VARCHAR(5)', 'HBHTNRNT' : 'VARCHAR(2)','HBPPOPDN' : 'VARCHAR(5)', 'GASPRICE' : 'FLOAT','VEHTYPE' : 'VARCHAR(3)','HH_CBSA' : 'VARCHAR(5)','HHC_MSA' : 'VARCHAR(4)'}

    types = [mappings.get(field, 'TEXT') for field in fields]
    return types

def make_datatypes_hhv(fields, rows=[]):

    mappings = {'HOUSEID' : 'VARCHAR(8)', 'VARSTRAT' : 'INT','WTHHFIN' : 'FLOAT', 'DRVRCNT' : 'INT', 'CDIVMSAR' : 'VARCHAR(2)', 'CENSUS_D' : 'VARCHAR(2)', 'CENSUS_R' : 'VARCHAR(2)', 'HH_HISP' : 'VARCHAR(2)', 'HH_RACE' : 'VARCHAR(2)', 'HHFAMINC' : 'VARCHAR(2)', 'HHRELATD' : 'VARCHAR(2)', 'HHRESP' : 'VARCHAR(2)', 'HHSIZE' : 'INT', 'HHSTATE' : 'VARCHAR(2)', 'HHSTFIPS' : 'VARCHAR(2)', 'HHVEHCNT' : 'INT', 'HOMEOWN' : 'VARCHAR(2)', 'HOMETYPE' : 'VARCHAR(2)', 'MSACAT' : 'VARCHAR(2)', 'MSASIZE' : 'VARCHAR(2)', 'NUMADLT' : 'INT', 'RAIL' : 'VARCHAR(2)', 'RESP_CNT' : 'INT', 'SCRESP' : 'VARCHAR(2)', 'TRAVDAY' : 'VARCHAR(2)', 'URBAN' : 'VARCHAR(2)', 'URBANSIZE' : 'VARCHAR(2)', 'URBRUR' : 'VARCHAR(2)', 'WRKCOUNT' : 'INT', 'TDAYDATE' : 'VARCHAR(8)', 'FLAG100' : 'VARCHAR(2)', 'LIF_CYC' : 'VARCHAR(2)', 'CNTTDHH' : 'VARCHAR(2)', 'HBHUR' : 'VARCHAR(2)', 'HTRESDN' : 'VARCHAR(5)', 'HTHTNRNT' : 'VARCHAR(2)', 'HTPPOPDN' : 'VARCHAR(5)', 'HTEEMPDN' : 'VARCHAR(4)', 'HBRESDN' : 'VARCHAR(5)','HBHTNRNT' : 'VARCHAR(2)', 'HBPPOPDN' : 'VARCHAR(5)', 'HH_CBSA' : 'VARCHAR(5)', 'HHC_MSA' : 'VARCHAR(4)'}

    types = [mappings.get(field, 'TEXT') for field in fields]
    return types

def make_datatypes_veh(fields, rows=[]):

    mappings = {'HOUSEID' : 'VARCHAR(8)', 'WTHHFIN' : 'FLOAT', 'VEHID' : 'VARCHAR(2)', 'DRVRCNT' : 'INT', 'HHFAMINC' : 'VARCHAR(2)', 'HHSIZE' : 'INT', 'HHVEHCNT' : 'INT', 'NUMADLT' : 'INT', 'FLAG100' : 'VARCHAR(2)', 'CDIVMSAR' : 'VARCHAR(2)', 'CENSUS_D' : 'VARCHAR(2)', 'CENSUS_R' : 'VARCHAR(2)', 'HHSTATE' : 'VARCHAR(2)', 'HHSTFIPS' : 'VARCHAR(2)', 'HYBRID' : 'VARCHAR(2)', 'MAKECODE' : 'VARCHAR(2)', 'MODLCODE' : 'VARCHAR(3)', 'MSACAT' : 'VARCHAR(2)', 'MSASIZE' : 'VARCHAR(2)', 'OD_READ' : 'INT', 'RAIL' : 'VARCHAR(2)', 'TRAVDAY' : 'VARCHAR(2)', 'URBAN' : 'VARCHAR(2)', 'URBANSIZE' : 'VARCHAR(2)', 'URBRUR' : 'VARCHAR(2)', 'VEHCOMM' : 'VARCHAR(2)', 'VEHOWNMO' : 'float', 'VEHYEAR': 'VARCHAR(4)', 'WHOMAIN' : 'VARCHAR(2)', 'WRKCOUNT' : 'INT', 'TDAYDATE' : 'VARCHAR(8)' , 'VEHAGE' : 'INT', 'PERSONID' : 'VARCHAR(2)', 'HH_HISP' : 'VARCHAR(2)','HH_RACE' : 'VARCHAR(2)', 'HOMEOWN' : 'VARCHAR(2)', 'HOMETYPE ' : 'VARCHAR(2)', 'LIF_CYC' : 'VARCHAR(2)', 'ANNMILES' : 'FLOAT', 'HBHUR' : 'VARCHAR(2)', 'HTRESDN' : 'VARCHAR(5)', 'HTHTNRNT' : 'VARCHAR(2)', 'HTPPOPDN' : 'VARCHAR(5)', 'HTEEMPDN' : 'VARCHAR(4)', 'HBRESDN' : 'VARCHAR(5)', 'HBHTNRNT' : 'VARCHAR(2)', 'HBPPOPDN' : 'VARCHAR(5)', 'BEST_FLG' : 'VARCHAR(2)', 'BESTMILE' : 'float', 'BEST_EDT' : 'VARCHAR(2)', 'BEST_OUT' : 'VARCHAR(2)', 'FUELTYPE' : 'INT', 'GSYRGAL' : 'INT', 'GSCOST' : 'float', 'GSTOTCST' : 'INT', 'EPATMPG' : 'float', 'EPATMPGF' : 'VARCHAR(2)', 'EIADMPG' : 'float', 'VEHTYPE' : 'VARCHAR(3)', 'HH_CBSA' : 'VARCHAR(5)', 'HHC_MSA' : 'VARCHAR(4)'}

    types = [mappings.get(field, 'TEXT') for field in fields]
    return types

def create_table(curr, table, fields, types):

    names = ", ".join(f + " " + t for f, t in zip(fields, types))
    command = 'CREATE TABLE IF NOT EXISTS ' + table + '(' + names + ')'

    curr.execute(command)



#csv_filename = raw_input('insert csv filename: ')
csv_filename = 'EIA_MkWh_2015.csv'
if not os.path.exists(csv_filename):
    sys.exit("'{0}' does not exist".format(csv_filename))

table = csv_filename.rsplit('.', 1)[0]
fp = open(csv_filename, 'r')
reader = csv.DictReader(fp)
fields = reader.fieldnames


fields, v = filter_data(fields)
types = make_datatypes(fields)
fp.close()

#conn = sql.connect(database="postgres", host="localhost")
conn = sql.connect(database="postgres", host="/home/" + os.environ['USER'] + "/postgres", port="5432" )
curr = conn.cursor()

create_table(curr, table, fields, types)
conn.commit()

fp = open(csv_filename, 'r')
names = ",".join(fields)
command = r'''COPY {0}({1}) FROM STDIN DELIMITER ',' CSV HEADER;'''.format(table, names)
curr.copy_expert(command, fp)
conn.commit()

#conn.close()
fp.close()

#EIA_CO2_Electricity_2015.csv
csv_filename = 'EIA_CO2_Electricity_2015.csv'
if not os.path.exists(csv_filename):
    sys.exit("'{0}' does not exist".format(csv_filename))

table = csv_filename.rsplit('.', 1)[0]
fp = open(csv_filename, 'r')
reader = csv.DictReader(fp)
fields = reader.fieldnames


fields, v = filter_data(fields)
types = make_datatypes(fields)
fp.close()

create_table(curr, table, fields, types)
conn.commit()

fp = open(csv_filename, 'r')
names = ",".join(fields)
command = r'''COPY {0}({1}) FROM STDIN DELIMITER ',' CSV HEADER;'''.format(table, names)
curr.copy_expert(command, fp)
conn.commit()

#conn.close()
fp.close()

#EIA_CO2_Transportation_2015.csv
csv_filename = 'EIA_CO2_Transportation_2015.csv'
if not os.path.exists(csv_filename):
    sys.exit("'{0}' does not exist".format(csv_filename))

table = csv_filename.rsplit('.', 1)[0]
fp = open(csv_filename, 'r')
reader = csv.DictReader(fp)
fields = reader.fieldnames


fields, v = filter_data(fields)
types = make_datatypes(fields)
fp.close()

create_table(curr, table, fields, types)
conn.commit()

fp = open(csv_filename, 'r')
names = ",".join(fields)
command = r'''COPY {0}({1}) FROM STDIN DELIMITER ',' CSV HEADER;'''.format(table, names)
curr.copy_expert(command, fp)
conn.commit()

#conn.close()
fp.close()

#FILE_PATH4 = 'PERV2PUB.CSV'
csv_filename = 'PERV2PUB.CSV'
if not os.path.exists(csv_filename):
    sys.exit("'{0}' does not exist".format(csv_filename))

table = csv_filename.rsplit('.', 1)[0]
fp = open(csv_filename, 'r')
reader = csv.DictReader(fp)
fields = reader.fieldnames


fields, v = filter_data(fields)
types = make_datatypes_per(fields)
fp.close()

create_table(curr, table, fields, types)
conn.commit()

fp = open(csv_filename, 'r')
names = ",".join(fields)
command = r'''COPY {0}({1}) FROM STDIN DELIMITER ',' CSV HEADER;'''.format(table, names)
curr.copy_expert(command, fp)
conn.commit()

#conn.close()
fp.close()

#DAYV2PUB.CSV
csv_filename = 'DAYV2PUB.CSV'
if not os.path.exists(csv_filename):
    sys.exit("'{0}' does not exist".format(csv_filename))

table = csv_filename.rsplit('.', 1)[0]
fp = open(csv_filename, 'r')
reader = csv.DictReader(fp)
fields = reader.fieldnames


fields, v = filter_data(fields)
types = make_datatypes_day(fields)
fp.close()

create_table(curr, table, fields, types)
conn.commit()

fp = open(csv_filename, 'r')
names = ",".join(fields)
command = r'''COPY {0}({1}) FROM STDIN DELIMITER ',' CSV HEADER;'''.format(table, names)
curr.copy_expert(command, fp)
conn.commit()

#conn.close()
fp.close()

#VEHV2PUB.CSV
csv_filename = 'VEHV2PUB.CSV'
if not os.path.exists(csv_filename):
    sys.exit("'{0}' does not exist".format(csv_filename))

table = csv_filename.rsplit('.', 1)[0]
fp = open(csv_filename, 'r')
reader = csv.DictReader(fp)
fields = reader.fieldnames


fields, v = filter_data(fields)
types = make_datatypes_veh(fields)
fp.close()

create_table(curr, table, fields, types)
conn.commit()

fp = open(csv_filename, 'r')
names = ",".join(fields)
command = r'''COPY {0}({1}) FROM STDIN DELIMITER ',' CSV HEADER;'''.format(table, names)
curr.copy_expert(command, fp)
conn.commit()

#conn.close()
fp.close()

#HHV2PUB.CSV
csv_filename = 'HHV2PUB.CSV'
if not os.path.exists(csv_filename):
    sys.exit("'{0}' does not exist".format(csv_filename))

table = csv_filename.rsplit('.', 1)[0]
fp = open(csv_filename, 'r')
reader = csv.DictReader(fp)
fields = reader.fieldnames


fields, v = filter_data(fields)
types = make_datatypes_hhv(fields)
fp.close()

create_table(curr, table, fields, types)
conn.commit()

fp = open(csv_filename, 'r')
names = ",".join(fields)
command = r'''COPY {0}({1}) FROM STDIN DELIMITER ',' CSV HEADER;'''.format(table, names)
curr.copy_expert(command, fp)
conn.commit()

conn.close()
fp.close()
