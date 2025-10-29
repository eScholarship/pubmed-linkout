# LinkOut submission documentation
# https://www.ncbi.nlm.nih.gov/books/NBK3812/

import boto3
import pymysql
import pyodbc
import submit_new_pubmed_items

submission_threshold = 250
session = boto3.Session()


# =========================
# Get Connections

def get_ssm_parameters(folder, names):
    print("Connect to SSM for parameters")
    ssm_client = session.client(service_name='ssm', region_name='us-west-2')

    param_names = [f"{folder}{name}" for name in names]
    response = ssm_client.get_parameters(Names=param_names, WithDecryption=True)

    param_values = {
        (param['Name'].split('/')[-1]): param['Value']
        for param in response['Parameters']}

    return param_values


def get_logging_db_connection(creds):
    return pymysql.connect(
        host=creds['server'],
        user=creds['user'],
        password=creds['password'],
        database=creds['pubmed-linkout-db'],
        cursorclass=pymysql.cursors.DictCursor)


def get_elements_report_db_connection(creds):
    mssql_conn = pyodbc.connect(
        driver=creds['driver'],
        server=(creds['server'] + ',1433'),
        database=creds['database'],
        uid=creds['user'],
        pwd=creds['password'],
        trustservercertificate='yes')
    mssql_conn.autocommit = True  # Required when queries use TRANSACTION
    return mssql_conn


# =========================
def main():
    elements_db_creds = get_ssm_parameters(
        folder="/pub-oapi-tools/elements-reporting-db/prod/",
        names=['server', 'database', 'user', 'password', 'driver'])

    tools_rds_creds = get_ssm_parameters(
        folder="/pub-oapi-tools/tools-rds/prod/",
        names=['server', 'pubmed-linkout-db', 'user', 'password'])

    # Get the pubs we've already submitted - returns a list of eschol_ids.
    submitted_ids = get_previous_pubmed_submissions(tools_rds_creds)

    # Get newly-added eSchol pubmed items;
    # Add them to the logging db
    # Check the total number of enqueued items
    new_pubmed_items = get_new_pmid_pubs(elements_db_creds, submitted_ids)
    if new_pubmed_items:
        total_enqueued = add_new_items_to_logging_db(tools_rds_creds, new_pubmed_items)
    else:
        print("No new pmid publications in eScholarship. Exiting.")
        exit(1)

    print(f"Including the new items, {total_enqueued} total items are enqueued for submission.")
    if total_enqueued >= submission_threshold:
        print(f"Total enqueued items over the threshold ({submission_threshold}): Moving to submission step.\n")
        submit_new_pubmed_items.main()
    else:
        print(f"Total enqueued items under the submission threshold ({submission_threshold}): Exiting.")
        exit(1)


# =========================
def get_previous_pubmed_submissions(tools_rds):
    mysql_conn = get_logging_db_connection(tools_rds)

    # Get the Item IDs already submitted
    with mysql_conn.cursor() as cursor:
        print("Connected to the logging DB. Collecting previously-submitted IDs.")
        cursor.execute("SELECT eschol_id FROM linkout_items")
        submitted_pubs = cursor.fetchall()
    mysql_conn.close()

    submitted_ids = [i['eschol_id'] for i in submitted_pubs]
    return submitted_ids


def get_new_pmid_pubs(elements_reporting_db, submitted_ids):

    # connect to the mySql db
    mssql_conn = get_elements_report_db_connection(elements_reporting_db)
    with mssql_conn.cursor() as cursor:
        print("Connected to Elements Reporting DB.")

        print("Creating temp table with submitted IDs.")
        cursor.execute("CREATE TABLE #linkout_ids (id varchar(16) COLLATE Latin1_General_CI_AS)")
        temp_table_insert = "INSERT INTO #linkout_ids (id) VALUES (?)"
        cursor.fast_executemany = True  # enables bulk inserting in executemany
        submitted_ids = [[s] for s in submitted_ids]  # Required format for executemany
        cursor.executemany(temp_table_insert, submitted_ids)
        mssql_conn.commit()

        print("Querying Elements Reporting DB for new pubmed items")
        get_new_eschol_pubmed_items = """
            SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
            BEGIN TRANSACTION;
            select
                p.id as ucpms_id,
                epr.[data source proprietary ID] as eschol_id,
                ppr.[data source proprietary ID] as pubmed_id
            from
                publication p
                    join [publication record] epr
                        on p.id = epr.[publication id]
                        and epr.[data source] = 'escholarship'
                    join [publication record file] prf
                        on epr.id = prf.[Publication Record ID]
                        and prf.[index] = 0
                    join [Publication Record] ppr
                        on p.id = ppr.[publication id]
                        and ppr.[data source] = 'pubmed'
            where
                epr.[data source proprietary ID]
                    not in (select li.id from #linkout_ids li)
            order by
                ppr.[Created When];
            COMMIT TRANSACTION;"""
        cursor.execute(get_new_eschol_pubmed_items)

        # pyodbc doesn't return dicts automatically, we have to make them ourselves
        columns = [column[0] for column in cursor.description]
        new_eschol_pubmed_items = [dict(zip(columns, row)) for row in cursor.fetchall()]

    mssql_conn.close()

    return new_eschol_pubmed_items


def add_new_items_to_logging_db(tools_rds, new_eschol_pubmed_items):
    mysql_conn = get_logging_db_connection(tools_rds)

    # Get the Item IDs already submitted
    print(f"Adding {len(new_eschol_pubmed_items)} new items to the pmid logging db.")
    with mysql_conn.cursor() as cursor:
        linkout_insert_sql = """
            INSERT INTO linkout_items (ucpms_id, eschol_id, pubmed_id)
            VALUES (%(ucpms_id)s, %(eschol_id)s, %(pubmed_id)s)"""
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
