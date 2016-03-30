'''Problem #5 Extra Credit query SQL'''

import psycopg2
import os
import math
import sys


try:
    #conn = psycopg2.connect(database="postgres", host="localhost")
    conn = sql.connect(database="postgres", host="/home/" + os.environ['USER'] + "/postgres", port="5432" )
    cur = conn.cursor()
except psycopg2.DatabaseError as e:
    print ('Error %s' % e)
    sys.exit(1)
except Exception as e:
    print ('Something Unexpected Went On %s' % e)

################################################################################
#################################<Problem5A>####################################
################################################################################

def Problem5a():

    sql = " SELECT TDAYDATE, TRPMILES, EPATMPG, HOUSEID \
            INTO R5a \
            FROM DAYV2PUB NATURAL JOIN (SELECT HOUSEID, VEHID, EPATMPG FROM VEHV2PUB) AS T1\
            WHERE TRPMILES > 0 AND CAST(VEHID AS INT) >= 1"
    cur.execute(sql)
    conn.commit()

    for i in [80,107,208,270]:
        print("Miles: '%d'" % i)
        for j in range(200803, 200905, 1):
            if not (j > 200812 and j < 200901):
                sql = "SELECT HybridCO2 * AVG AS Hybrid, CombustCO2 * AVG AS Combust, (CombustCO2 - HybridCO2)/CombustCO2 * 100 AS CO2CHG \
                            FROM (SELECT TDAYDATE,\
                            SUM(CASE \
                            WHEN TRPMILES > " + str(i) + " \
                            THEN (TRPMILES/EPATMPG) * 0.26661 \
                            ELSE ((CAST(EIA_CO2_ELECTRICITY_2015.VALUE AS FLOAT) / CAST(EIA_MKWH_2015.VALUE AS FLOAT))*(TRPMILES/(EPATMPG * 0.090634441))*30) \
                            END) AS HybridCO2, SUM(TRPMILES/EPATMPG * 0.26661) AS CombustCO2, (117538000 / COUNT(DISTINCT HOUSEID)) AS AVG   \
                            FROM R5a, EIA_MKWH_2015, EIA_CO2_ELECTRICITY_2015 \
                            WHERE CAST(TDAYDATE AS INT) = '%d'\
                            AND EIA_CO2_ELECTRICITY_2015.MSN = 'TXEIEUS' AND EIA_CO2_ELECTRICITY_2015.YYYYMM = '%d'  \
                            AND EIA_MKWH_2015.MSN='ELETPUS' AND EIA_MKWH_2015.YYYYMM = '%d'\
                            GROUP BY TDAYDATE) emco" % (j,j,j)
                cur.execute(sql)
                print ("%d, Hybrid, Conventional, Percent Change" % (j))
                print(cur.fetchall())
        print('*****************************************************************')


################################################################################
#################################<Problem5B>####################################
################################################################################
def Problem5b():

    sql = " SELECT TDAYDATE, TRPMILES, EPATMPG, HOUSEID \
            INTO R5b \
            FROM DAYV2PUB NATURAL JOIN (SELECT HOUSEID, VEHID, EPATMPG FROM VEHV2PUB) AS T1\
            WHERE TRPMILES > 0 AND CAST(VEHID AS INT) >= 1"
    cur.execute(sql)
    conn.commit()

    sql = "SELECT MSN, YYYYMM, VALUE\
            INTO R5b1\
            FROM (SELECT MSN, YYYYMM, VALUE\
            FROM EIA_MKWH_2015\
            WHERE MSN = 'NGETPUS'\
            UNION\
            SELECT MSN, YYYYMM, VALUE\
            FROM EIA_MKWH_2015\
            WHERE MSN = 'WYETPUS'\
            UNION\
            SELECT MSN, YYYYMM, VALUE\
            FROM EIA_MKWH_2015\
            WHERE MSN = 'NUETPUSAND')T1"

    cur.execute(sql)
    conn.commit()

    for i in [80,107,208,270]:
        print("Miles: '%d'" % i)
        for j in range(200803, 200905, 1):
            if not (j > 200812 and j < 200901):
                sql = "SELECT HybridCO2 * AVG AS Hybrid, CombustCO2 * AVG AS Combust, (CombustCO2 - HybridCO2)/CombustCO2 * 100 AS CO2CHG \
                            FROM (SELECT TDAYDATE,\
                            SUM(CASE \
                            WHEN TRPMILES > " + str(i) + " \
                            THEN (TRPMILES/EPATMPG) * 0.26661 \
                            ELSE ((CAST(EIA_CO2_ELECTRICITY_2015.VALUE AS FLOAT) / CAST(R3.VALUE AS FLOAT))*(TRPMILES/(EPATMPG * 0.090634441))*30) \
                            END) AS HybridCO2, SUM(TRPMILES/EPATMPG * 0.26661) AS CombustCO2, (117538000 / COUNT(DISTINCT HOUSEID)) AS AVG   \
                            FROM R5b1, R5b, EIA_CO2_ELECTRICITY_2015 \
                            WHERE CAST(TDAYDATE AS INT) = '%d'\
                            AND EIA_CO2_ELECTRICITY_2015.MSN = 'NNEIEUS' AND EIA_CO2_ELECTRICITY_2015.YYYYMM = '%d'  \
                            AND R3.YYYYMM = '%d'\
                            GROUP BY TDAYDATE) emco" % (j,j,j)
                cur.execute(sql)
                print ("%d, Hybrid, Conventional, Percent Change" % (j))
                print(cur.fetchall())
        print('*****************************************************************')

print ('***********************************************************************')
Problem5a()
print ('***********************************************************************')
Problem5b()
