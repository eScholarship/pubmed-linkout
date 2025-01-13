# LinkOut submission documentation
# https://www.ncbi.nlm.nih.gov/books/NBK3812/

from dotenv import dotenv_values
import pymysql
import submit_new_eschol_pubmed_items

submission_threshold = 10


# =========================
# Get Connections
def get_eschol_db_connection(env):
    mysql_conn = pymysql.connect(
        host=env['ESCHOL_DB_SERVER_PROD'],
        user=env['ESCHOL_DB_USER_PROD'],
        password=env['ESCHOL_DB_PASSWORD_PROD'],
        database=env['ESCHOL_DB_DATABASE_PROD'],
        cursorclass=pymysql.cursors.DictCursor)

    return mysql_conn


def get_logging_db_connection(env):
    mysql_conn = pymysql.connect(
        host=env['LOGGING_DB_SERVER'],
        user=env['LOGGING_DB_USER'],
        password=env['LOGGING_DB_PASSWORD'],
        database=env['LOGGING_DB_DATABASE'],
        cursorclass=pymysql.cursors.DictCursor)

    return mysql_conn


# =========================
def main():
    env = dotenv_values(".env")

    # Get the pubs we've already submitted
    submitted_ids = get_previous_pubmed_submissions(env)

    # Get newly-added eSchol pubmed items;
    # Add them to the logging db
    # Check the total number of enqueued items
    new_eschol_pubmed_items = get_new_pmid_pubs(env, submitted_ids)
    if new_eschol_pubmed_items:
        total_enqueued = add_new_items_to_logging_db(env, new_eschol_pubmed_items)
    else:
        print("No new pmid publications in eScholarship. Exiting.")
        exit(1)

    print(f"Including the new items, {total_enqueued} total items are enqueued for submission.")
    if total_enqueued >= submission_threshold:
        print(f"Total enqueued items over the threshold ({submission_threshold}): Moving to submission step.\n")
        submit_new_eschol_pubmed_items.main()
    else:
        print(f"Total enqueued items under the submission threshold ({submission_threshold}): Exiting.")
        exit(1)


# =========================
def get_previous_pubmed_submissions(env):
    mysql_conn = get_logging_db_connection(env)

    # Get the Item IDs already submitted
    with mysql_conn.cursor() as cursor:
        print("Connected to the logging DB. Collecting previously-submitted IDs.")
        cursor.execute("SELECT eschol_id FROM linkout_items")
        submitted_pubs = cursor.fetchall()
    mysql_conn.close()

    submitted_ids = [i['eschol_id'] for i in submitted_pubs]
    return submitted_ids


def get_new_pmid_pubs(env, submitted_ids):

    # connect to the mySql db
    mysql_conn = get_eschol_db_connection(env)
    with mysql_conn.cursor() as cursor:
        print("Connected to eSchol DB.")

        print("Creating temp table with submitted IDs.")
        temp_table_create = "CREATE TEMPORARY TABLE linkout_ids (id varchar(16))"
        temp_table_insert = "INSERT INTO linkout_ids (id) VALUES (%s)"
        cursor.execute(temp_table_create)
        cursor.executemany(temp_table_insert, submitted_ids)
        mysql_conn.commit()

        print("Querying for new pubmed items")
        get_new_eschol_pubmed_items = """
            select
                i.id,
                local_id.value as 'pmid'
            from
                items i,
                JSON_TABLE(
                    attrs,
                    "$.local_ids[*]"
                    COLUMNS(type varchar(255) PATH "$.type",
                            value varchar(255) PATH "$.id")
                ) as local_id
            where
                i.id not in (select l.id from linkout_ids l)
                and local_id.type = 'pmid'
                and (local_id.value not REGEXP('[^0-9]') 
                    and local_id.value != '')
            order by i.added;"""
        cursor.execute(get_new_eschol_pubmed_items)
        new_eschol_pubmed_items = cursor.fetchall()
        mysql_conn.close()

    return new_eschol_pubmed_items


def add_new_items_to_logging_db(env, new_eschol_pubmed_items):
    mysql_conn = get_logging_db_connection(env)

    # Get the Item IDs already submitted
    print(f"Adding {len(new_eschol_pubmed_items)} new items to the pmid logging db.")
    with mysql_conn.cursor() as cursor:
        linkout_insert_sql = "INSERT INTO linkout_items (eschol_id, pmid) VALUES (%(id)s, %(pmid)s)"
        cursor.executemany(linkout_insert_sql, new_eschol_pubmed_items)
        mysql_conn.commit()

        print(f"Checking new total enqueued items.")
        cursor.execute("""SELECT count(eschol_id) as total_enqueued
                FROM linkout_items WHERE submitted IS NULL""")
        total_enqueued = cursor.fetchone()['total_enqueued']
        mysql_conn.close()

    return total_enqueued


# =========================
if __name__ == '__main__':
    main()
