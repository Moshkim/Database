'''Problem #3 query SQL'''

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
#################################<Problem3A>####################################
################################################################################
def Problem3a():

    sql = "SELECT HOUSEID, PERSONID, SUM(TRPMILES) AS miles \
           INTO traveler \
           FROM DAYV2PUB \
           WHERE TRPMILES > -1\
           GROUP BY houseid, personid"
    cur.execute(sql)
    conn.commit()

    sql = "SELECT COUNT(*) FROM traveler"
    cur.execute(sql)
    total = int((cur.fetchone())[0])
    print ("Total number of people: %d" % total)

    for i in range (5, 105, 5):
        sql = "SELECT COUNT(miles) \
               FROM traveler \
               WHERE miles < "+ str(i) +" "
        cur.execute(sql)
        count = float((cur.fetchone())[0])
        percentage = ((count / total) * 100)
        print ("Percentage of people who travel less than %d miles: %.2f%%" % (i, percentage))


################################################################################
#################################<Problem3B>####################################
################################################################################


def Problem3b():
    #arrary=[]
    for i in range (5, 105, 5):
        sql = "SELECT AVG(EPATMPG) \
                FROM VEHV2PUB INNER JOIN DAYV2PUB \
                ON VEHV2PUB.HOUSEID = DAYV2PUB.HOUSEID \
                WHERE CAST(VEHV2PUB.VEHID AS INT) >= 1 \
                AND DAYV2PUB.TRPMILES < "+ str(i) +""
        cur.execute(sql)
        fuel_avg = float((cur.fetchone())[0])
        #array.append(fuel_avg)
        print ("Average fuel less than %d miles: %.2f" % (i, fuel_avg))
    #print(array)


################################################################################
#################################<Problem3C>####################################
################################################################################

def Problem3c():

        sql = " SELECT TDAYDATE, TRPMILES, EPATMPG, HOUSEID \
                INTO T1 \
                FROM DAYV2PUB NATURAL JOIN VEHV2PUB\
                WHERE TRPMILES > 0 AND CAST(VEHID AS INT) >= 1"
        cur.execute(sql)
        conn.commit()

        for i in range(200803, 200905, 1):
            if not (i > 200812 and i < 200901):
                sql = "SELECT CO2_emission * SCALE /((CAST (VALUE AS FLOAT)))\
                        FROM (SELECT TDAYDATE, SUM((TRPMILES / EPATMPG) * 0.26661)as CO2_emission, 117538000 / COUNT(DISTINCT HOUSEID) AS SCALE\
                        FROM T1\
                        WHERE CAST(TDAYDATE AS INT) = '%d'\
                        GROUP BY TDAYDATE) T2, EIA_CO2_Transportation_2015\
                        WHERE MSN = 'TEACEUS' AND CAST(YYYYMM AS INT) = '%d'" % (i,i)
                cur.execute(sql)
                result = float((cur.fetchone())[0])
                print ("Percent of CO2 emission for %d: %.2f%%" % (i, result * 1/10000))


def Problem3d():

    sql = " SELECT TDAYDATE, TRPMILES, EPATMPG, HOUSEID \
            INTO R1 \
            FROM DAYV2PUB NATURAL JOIN (SELECT HOUSEID, VEHID, EPATMPG FROM VEHV2PUB) AS T1\
            WHERE TRPMILES > 0 AND CAST(VEHID AS INT) >= 1"
    cur.execute(sql)
    conn.commit()

    for i in range(20, 80, 20):
        print("Miles: '%d'" % i)
        for j in range(200803, 200905, 1):
            if not (j > 200812 and j < 200901):
                sql = "SELECT HybridCO2 * AVG AS Hybrid, CombustCO2 * AVG AS Combust, (CombustCO2 - HybridCO2)/CombustCO2 * 100 AS CO2CHG \
                            FROM (SELECT TDAYDATE,\
                            SUM(CASE \
                            WHEN TRPMILES > " + str(i) + " \
                            THEN (((TRPMILES - " + str(i) +")/EPATMPG) * 0.26661 + (CAST(EIA_CO2_ELECTRICITY_2015.VALUE AS FLOAT) / CAST(EIA_MKWH_2015.VALUE AS FLOAT)) * (" + str(i) +"/(EPATMPG * 0.090634441))* 30) \
                            ELSE ((CAST(EIA_CO2_ELECTRICITY_2015.VALUE AS FLOAT) / CAST(EIA_MKWH_2015.VALUE AS FLOAT))*(TRPMILES/(EPATMPG * 0.090634441))*30) \
                            END) AS HybridCO2, SUM(TRPMILES/EPATMPG * 0.26661) AS CombustCO2, (117538000 / COUNT(DISTINCT HOUSEID)) AS AVG   \
                            FROM R1, EIA_MKWH_2015, EIA_CO2_ELECTRICITY_2015 \
                            WHERE CAST(TDAYDATE AS INT) = '%d'\
                            AND EIA_CO2_ELECTRICITY_2015.MSN = 'TXEIEUS' AND EIA_CO2_ELECTRICITY_2015.YYYYMM = '%d'  \
                            AND EIA_MKWH_2015.MSN='ELETPUS' AND EIA_MKWH_2015.YYYYMM = '%d'\
                            GROUP BY TDAYDATE) emco" % (j,j,j)
                cur.execute(sql)
                print ("%d, Hybrid, Conventional, Percent Change" % (j))
                print(cur.fetchall())
        print('*****************************************************************')



print('PART3')
#Problem3a()
print ('***********************************************************************')
#Problem3b()
print ('***********************************************************************')
#Problem3c()
print ('***********************************************************************')
#Problem3d()
print ('***********************************************************************')
