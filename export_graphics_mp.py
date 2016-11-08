# requirements
# -pyodbc
# -pillow

import os
import argparse
import cStringIO as StringIO
import multiprocessing
import pyodbc
from PIL import Image

# settings
SAVE_PATH = 'e:/tof_data/gra/'
TECDOC_CONNECTION_STRING = 'DSN=tecdoc; UID=tecdoc; PWD=tcd_error_0'
DIRPATH_BY_TABLES = True  # if False then by 7 start chars of filename


def get_connection():

    try:
        conn = pyodbc.connect(TECDOC_CONNECTION_STRING)
    except Exception as e:
        print("Unable to connect to the database: {}".format(e))
        return False

    return conn


def exec_query(query='', params=None):

    conn = get_connection()
    if not conn:
        return False

    cur = conn.cursor()
    if params is not None:
        cur.execute(query, params)
    else:
        cur.execute(query)

    return cur


def save_image(row):
    gra_id, data, table_number, gra_grd_id = row
    if DIRPATH_BY_TABLES:
        dir_path = os.path.join(SAVE_PATH, str(table_number))
        file_path = os.path.join(dir_path, '{}.gif'.format(gra_id))
    else:
        dir_path = os.path.join(SAVE_PATH, gra_id[0:6])
        file_path = os.path.join(dir_path, '{}.gif'.format(gra_grd_id))
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)    
    if os.path.exists(file_path):
        return
    img = Image.open(StringIO.StringIO(data))
    img.save(file_path)




def export_table(table_number):

    rows = exec_query(
        """
        select gra_id, grd_graphic, {table_number} as table_number, gra_grd_id from tof_gra_data_{table_number}
        left join tof_graphics on grd_id = gra_grd_id
        left join tof_link_gra_art on gra_id = lga_gra_id
        WHERE gra_tab_nr = {table_number}
        """
        .format(table_number=table_number), None).fetchall()

    pool = multiprocessing.Pool()
    pool.map(save_image, rows)
    pool.close()
    pool.join()


def export(start_table=0, end_table=0):

    rows = exec_query(
        """
        select distinct gra_tab_nr from tof_graphics order by gra_tab_nr
        """).fetchall()

    if not rows:
        print('0 rows')
        return

    for row in rows:
        if (row[0] is None or row[0] < start_table) or (end_table > 0 and row[0] > end_table):
            continue
        print('Exporting tof_gra_data_{} ...'.format(row[0]))
        export_table(row[0])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Export graphic from tecdoc')
    parser.add_argument('-s', '--start', type=int, help='start table', dest='start_table', default=0)
    parser.add_argument('-e', '--end', type=int, help='end table', dest='end_table', default=0)
    args = parser.parse_args()
    export(args.start_table, args.end_table)
