# LinkOut submission documentation
# https://www.ncbi.nlm.nih.gov/books/NBK3812/

from pub_oapi_tools_common import aws_lambda
from pub_oapi_tools_common import ucpms_db
from pub_oapi_toosl_common import pub_oapi_tools_db
import submit_new_pubmed_items

submission_threshold = 250


# =========================
creds = {
    'elements_db': {
        'folder': 'pub-oapi-tools/elements-reporting-db',
        'env': 'prod'},
    'linkout_db': {
        'folder': 'pub-oapi-tools/tools-rds',
        'env': 'prod',
        'names': ['server', 'pubmed-linkout-db', 'user', 'password']}
}

creds = aws_lambda.get_parameters(creds)
creds['linkout_db']['database'] = creds['linkout_db']['pubmed-linkout-db']


# =========================
def main():

    # Get the pubs we've already submitted - returns a list of eschol_ids.
    submitted_ids = get_previous_pubmed_submissions(creds['linkout_db'])

    # Get newly-added eSchol pubmed items
    # Add them to the logging db
    # Check the total number of enqueued items
    new_pubmed_items = get_new_pmid_pubs(creds['elements_db'], submitted_ids)
    if new_pubmed_items:
        total_enqueued = add_new_items_to_logging_db(creds['linkout_db'], new_pubmed_items)
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
    mysql_conn = pub_oapi_tools_db.get_connection(tools_rds)

    # Get the Item IDs already submitted
    with mysql_conn.cursor() as cursor:
        print("Connected to the logging DB. Collecting previously-submitted IDs.")
        cursor.execute("SELECT eschol_id FROM linkout_items")
        submitted_pubs = cursor.fetchall()
    mysql_conn.close()

    submitted_ids = [i['eschol_id'] for i in submitted_pubs]
    return submitted_ids


# Connects to Elements DB, create temp table w/ linkout IDs, get new pubs
def get_new_pmid_pubs(elements_reporting_db, submitted_ids):

    mssql_conn = ucpms_db.get_connection(elements_reporting_db)
    with mssql_conn.cursor() as cursor:
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
    mysql_conn = pub_oapi_tools_db.get_connection(tools_rds)

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
