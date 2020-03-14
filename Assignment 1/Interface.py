#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS ' + ratingstablename + '(userid INT,v VARCHAR, movieid INT,x VARCHAR, rating FLOAT, y  VARCHAR, Timestamp BIGINT)')
    with open(ratingsfilepath, 'r') as file:
        cur.copy_from(file, ratingstablename, sep=':')
    # dropping extra columns as ':' seperator was used.
    cur.execute('ALTER TABLE ' + ratingstablename + ' DROP v, DROP x, DROP y, DROP Timestamp' )
    openconnection.commit()
    file.close()
    pass


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    if numberofpartitions>0:
        interval = (5.0-0.0)/numberofpartitions
        for i in range(numberofpartitions):
            name_of_table = 'range_part' + str(i)
            cur.execute('CREATE TABLE {0} ({1} INT, {2} INT, {3} FLOAT);'.format(name_of_table, 'userid', 'movieid', 'rating'))
            lb = i*interval
            ub = lb + interval
            if lb == 0.0:
                cur.execute('INSERT INTO {0} ({1}, {2}, {3}) SELECT * FROM {4} WHERE {3}>={5} and {3}<={6};'.format(name_of_table,'userid', 'movieid', 'rating', ratingstablename, lb, ub))
            else:
                cur.execute('INSERT INTO {0} ({1}, {2}, {3}) SELECT * FROM {4} WHERE {3}>{5} and {3}<={6};'.format(name_of_table,'userid','movieid','rating',ratingstablename,lb, ub))
        openconnection.commit()
    else:
        print('Invalid number of partitions')
        openconnection.commit()
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    for i in range(numberofpartitions):
        name_of_table = 'rrobin_part' + str(i)
        cur.execute('CREATE TABLE {0} ({1} INT, {2} INT, {3} FLOAT);'.format(name_of_table, 'userid', 'movieid','rating'))
        disk = i%numberofpartitions
        # row_number starts from 1 and i starts from 0, therefore either i should be incremented by 1
        # while calculating disk or row should be deduced by 1 while calculating mod
        cur.execute("INSERT INTO {0} SELECT {1}, {2}, {3} FROM (SELECT ROW_NUMBER() OVER() AS row, * FROM {4})"\
                    "AS obj WHERE MOD(row-1, {5}) = {6};".format(name_of_table, 'userid', 'movieid', 'rating', ratingstablename, numberofpartitions, disk))
    openconnection.commit()
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    #First insert into 'ratings' table for completeness.
    cur.execute('INSERT INTO {0} ({1}, {2}, {3}) VALUES ({4}, {5}, {6});'.format(ratingstablename, 'userid', 'movieid', 'rating', userid, itemid, rating))
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'rrobin_part%'")
    partitions = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM rrobin_part0')
    present_rows = cur.fetchone()[0]
    for i in range(1,partitions):
        nameofpartition = 'rrobin_part' + str(i)
        cur.execute('SELECT COUNT(*) FROM {0};'.format(nameofpartition))
        next_rows = cur.fetchone()[0]
        if present_rows>next_rows:
            cur.execute('INSERT INTO {0} ({1}, {2}, {3}) VALUES ({4}, {5}, {6});'.format(nameofpartition, 'userid', 'movieid', 'rating', userid, itemid, rating))
            break
        elif (i==(partitions-1)) and present_rows==next_rows:
            cur.execute('INSERT INTO rrobin_part0 ({0}, {1}, {2}) VALUES ({3}, {4}, {5});'.format('userid', 'movieid', 'rating', userid, itemid, rating))
            break
        else:
            present_rows=next_rows
            continue
    openconnection.commit()
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    #First insert into ratings table to satisfy completeness.
    cur.execute('INSERT INTO {0} ({1}, {2}, {3}) VALUES ({4}, {5}, {6});'.format(ratingstablename, 'userid', 'movieid', 'rating', userid, itemid, rating))
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE 'range_part%'")
    partitions = cur.fetchone()[0]
    if partitions>0:
        interval = (5.0-0.0)/partitions
        for i in range(partitions):
            lb = i*interval
            ub = lb + interval
            nameofpartition = 'range_part' + str(i)
            if rating>=lb and rating<=ub and lb==0.0:
                cur.execute('INSERT INTO {0} ({1}, {2}, {3}) VALUES({4}, {5}, {6});'.format(nameofpartition, 'userid','movieid','rating',userid, itemid, rating))
                print('0th partition')
            elif(rating>lb and rating<=ub):
                cur.execute('INSERT INTO {0} ({1}, {2}, {3}) VALUES({4}, {5}, {6});'.format(nameofpartition, 'userid','movieid', 'rating', userid, itemid, rating))
            else:
                continue
        openconnection.commit()
    else:
        print('Invalid number of partitions')
        openconnection.commit()
    pass


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    # con.close()
    #ratingstablename = 'ratings'
    #ratingsfilepath= "/home/user/Downloads/ml-10M100K/ratings.dat"
    #testingfilepath="/home/user/Downloads/test_data.dat"
    #loadratings(ratingstablename, ratingsfilepath, con)
    #loadratings(ratingstablename,testingfilepath,con)
    #rangepartition(ratingstablename,3, con)
    #roundrobinpartition(ratingstablename,3, con)
    #rangeinsert(ratingstablename, 1, 462, 2.0, con)
    #roundrobininsert(ratingstablename, 3, 8, 2.5, con)
# if __name__ == '__main__':
#     create_db('postgres')

