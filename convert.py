# requirements
# -pyodbc
# -mysqlclient

import re
import os
import argparse
import pyodbc
import MySQLdb

# settings
SAVE_PATH = 'e:/tof_data/'
DES_ID_RE = re.compile('(.*?)_DES_ID(.*)')
CDS_ID_RE = re.compile('(.*?)_CDS_ID')
TECDOC_CONNECTION_STRING = 'DSN=tecdoc; UID=tecdoc; PWD=tcd_error_0'
MYSQL_CONNECTION_PARAMS = {'host': 'localhost',
                           'user': 'root',
                           'passwd': 'root',
                           'db': 'tecdoc',
                           }


def get_tecdoc_connection():

    try:
        conn = pyodbc.connect(TECDOC_CONNECTION_STRING)
    except Exception as e:
        print("Unable to connect to Tecdoc database: {}".format(e))
        return False

    return conn


def get_mysql_connection():

    try:
        conn = MySQLdb.connect(host=MYSQL_CONNECTION_PARAMS["host"], user=MYSQL_CONNECTION_PARAMS["user"],
                               passwd=MYSQL_CONNECTION_PARAMS["passwd"], db=MYSQL_CONNECTION_PARAMS["db"], local_infile=True)
    except Exception as e:
        print("Unable to connect to MySQL database: {}".format(e))
        return False

    return conn


def exec_tecdoc_query(query='', params=None):

    conn = get_tecdoc_connection()
    if not conn:
        return False

    cur = conn.cursor()
    if params is not None:
        cur.execute(query, params)
    else:
        cur.execute(query)

    return cur


def exec_mysql_query(query='', params=None, commit=False):

    conn = get_mysql_connection()
    if not conn:
        return False

    cur = conn.cursor()
    if params is not None:
        cur.execute(query, params)
    else:
        cur.execute(query)

    if commit:
        conn.commit()

    return cur


def create_schema():
    DDL_texts = []
    conn = get_tecdoc_connection()
    cur = conn.cursor()
    tables = [table.table_name for table in cur.tables(table='TOF_%')]
    for table in tables:
        if 'TOF_GRA_DATA_' in table:
            continue
        DDL = 'DROP TABLE IF EXISTS {};'.format(table.lower())
        DDL_texts.append(DDL)
        columns = cur.columns(table)
        columns_defs = []
        for column in columns:
            if 'char' in column.type_name:
                columns_defs.append('{} VARCHAR({}) '.format(column.column_name.lower(), column.column_size))
            elif column.type_name == 'bits':
                columns_defs.append('{} integer DEFAULT NULL'.format(column.column_name.lower()))
            elif column.type_name == 'datetime':
                columns_defs.append('{} DATETIME DEFAULT NULL'.format(column.column_name.lower()))
            else:
                columns_defs.append('{} {}({}) DEFAULT NULL'.format(column.column_name.lower(), column.type_name, column.column_size))
                if '_DES_ID' in column.column_name:
                    groups = re.search(DES_ID_RE, column.column_name)
                    columns_defs.append('{}_des_text{} VARCHAR(1200) '.format(groups.group(1).lower(), groups.group(2).lower()))
                if '_CDS_ID' in column.column_name:
                    groups = re.search(CDS_ID_RE, column.column_name)
                    columns_defs.append('{}_cds_text VARCHAR(1200) '.format(groups.group(1).lower()))

        DDL = 'CREATE TABLE {} ({}) ENGINE=InnoDB DEFAULT CHARSET=utf8;'.format(table.lower(), ', '.join(columns_defs))
        DDL_texts.append(DDL)

    for DDL_text in DDL_texts:
        exec_mysql_query(DDL_text)


def transfer_data(filter_table=None, limit=0, begin=''):
    conn = get_tecdoc_connection()
    cur = conn.cursor()
    if filter_table:
        tables = [table.table_name for table in cur.tables(table=filter_table)]
    else:
        tables = [table.table_name for table in cur.tables(table='TOF_%')]
    for table in tables:
        if 'TOF_GRA_DATA_' in table:
            continue
        if begin and begin != table:
            continue
        else:
            begin = ''
        print('Exporting {}...'.format(table))
        fields = []
        joins = []
        nulls = []
        columns = cur.columns(table)
        for column in columns:
            if column.type_name == 'bits':
                fields.append('{name} subrange(221 cast integer) AS {name}'.format(name=column.column_name.lower()))
            else:
                fields.append('{name}'.format(name=column.column_name.lower()))
                if '_DES_ID' in column.column_name:
                    groups = re.search(DES_ID_RE, column.column_name)
                    fields.append('{pre}_des_tex_{post}.tex_text {pre}_des_text{post}'.
                                  format(pre=groups.group(1).lower(), post=groups.group(2).lower()))
                    joins.append('LEFT OUTER JOIN tof_designations {pre}_des_{post} \
                                 ON  {pre}_des_{post}.des_id = {column} AND {pre}_des_{post}.des_lng_id = 16'.
                                 format(pre=groups.group(1).lower(), post=groups.group(2).lower(), column=column.column_name.lower()))
                    joins.append('LEFT OUTER JOIN tof_des_texts {pre}_DES_TEX_{post} \
                                 ON {pre}_DES_TEX_{post}.tex_id = {pre}_des_{post}.des_tex_id'.
                                 format(pre=groups.group(1).lower(), post=groups.group(2).lower(), column=column.column_name.lower()))
                if '_CDS_ID' in column.column_name:
                    groups = re.search(CDS_ID_RE, column.column_name)
                    fields.append('{pre}_cds_tex.tex_text {pre}_cds_text'.format(pre=groups.group(1).lower()))
                    joins.append('LEFT OUTER JOIN tof_country_designations {pre}_cds \
                                 ON {pre}_cds.cds_id = {column} AND {pre}_cds.cds_lng_id = 16 AND {pre}_cds.cds_ctm Subrange(221 cast integer) = 1'.
                                 format(pre=groups.group(1).lower(), column=column.column_name.lower()))
                    joins.append('LEFT OUTER JOIN tof_des_texts {pre}_cds_tex ON {pre}_cds_tex.tex_id = {pre}_cds.cds_tex_id'.
                                 format(pre=groups.group(1).lower()))
            if 'int' in column.type_name or 'num' in column.type_name:
                nulls.append('0')
                if '_DES_ID' in column.column_name or '_CDS_ID' in column.column_name:
                    nulls.append('')
            else:
                nulls.append('')

        limit_str = ''
        if limit > 0:
            limit_str = 'FIRST({})'.format(limit)

        SQL = 'SELECT {} FROM {} {} {}'.format(', '.join(fields), table, ' '.join(joins), limit_str)
        try:
            cur = exec_tecdoc_query(SQL)
        except Exception as e:
            print('Query error {} \n {}'.format(e, SQL))
            raise Exception('Terminated after error!')

        with open(os.path.join(SAVE_PATH, table), 'w') as f:
            while True:
                rows = cur.fetchmany(5000)
                if not rows:
                    break
                for row in rows:
                    i = 0
                    fields = []
                    for col in row:
                        if col is None:
                            fields.append(str(nulls[i]))
                        else:
                            fields.append(str(col))
                        i += 1
                    fields_str = '(|)'.join(fields)
                    f.write('{}\n'.format(fields_str))

        path = os.path.join(SAVE_PATH, table)

        SQL = """
              load data local infile '{}' into table {} char set 'cp1251'
              fields terminated by '(|)';
              """.format(path, table.lower())

        try:
            exec_mysql_query(SQL, commit=True)
        except Exception as e:
            print('Loading error {} \n {}'.format(e, SQL))
            raise Exception('Terminated after error!')

        os.remove(path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Export schema and data from tecdoc to mysql')
    parser.add_argument('-s', '--schema', help='convert schema', action='store_true')
    parser.add_argument('-d', '--data', help='convert data', action='store_true')
    parser.add_argument('-t', '--table', help='table')
    parser.add_argument('-l', '--limit', type=int, help='table', default=0)
    parser.add_argument('-b', '--begin', type=str, help='begin table', default='')
    args = parser.parse_args()
    if args.schema:
        create_schema()
    elif args.data:
        transfer_data(args.table, args.limit, args.begin)
    else:
        print('Add -s or -d argument')


