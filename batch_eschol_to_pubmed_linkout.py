# LinkOut submission documentation
# https://www.ncbi.nlm.nih.gov/books/NBK3812/

from dotenv import dotenv_values
import datetime
import pymysql
import os
import xml.etree.ElementTree as ET
from ftplib import FTP
from time import sleep

# Batching vars
page_size = 15000
resource_filename_no_extension = "eschol_linkout_resource"


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

    # Config, logging, and output dir setup.
    env = dotenv_values(".env")
    run_time = datetime.datetime.now().replace(microsecond=0).isoformat()
    run_time = run_time.replace(':', "-")
    log_file = f"output/{run_time}-submission-log.csv"
    output_dir = f"output/{run_time}-pubmed-linkout-files"
    os.mkdir(f"./{output_dir}")

    # Gets the pubs we've already sent
    submitted_ids = get_previous_pubmed_submissions(env)

    # Get pubs w/ pmids in eScholarship
    eschol_pmid_pubs = get_eschol_pmid_pubs(env, submitted_ids)
    eschol_pmid_pubs_pages = list(chunk_into_n(eschol_pmid_pubs, page_size))

    # TK check against what we already have

    # Creates the xml
    print("Processing paginated files:")
    resource_xml_files = []
    for page_number, eschol_page in enumerate(eschol_pmid_pubs_pages):
        resource_xml_file = create_resource_xml(eschol_page, output_dir, page_number, run_time)
        resource_xml_files.append(resource_xml_file)

    # Upload resource to PMID FTP
    sleep(10)
    upload_xml_files_to_ftp(resource_xml_files, env)

    # Update logging db
    update_logging_db(env, eschol_pmid_pubs)


# =========================
# Returns a list of lists, of size n or fewer.
def chunk_into_n(full_list, n):
    for i in range(0, len(full_list), n):
        yield full_list[i:i + n]


# =========================
def get_previous_pubmed_submissions(env):
    mysql_conn = get_logging_db_connection(env)

    # Query for items w/ PMIDs
    logging_sql = "SELECT item_id FROM linkout_items"

    with mysql_conn.cursor() as cursor:
        print("Connected to the logging DB. Collecting previously-submitted IDs.")
        cursor.execute(logging_sql)
        submitted_pubs = cursor.fetchall()
    mysql_conn.close()

    submitted_ids = [i['item_id'] for i in submitted_pubs]
    print(submitted_ids)

    return submitted_ids


# =========================
def get_eschol_pmid_pubs(env, submitted_ids):

    # connect to the mySql db
    mysql_conn = get_eschol_db_connection(env)

    exclude_ids_sql = ',\n'.join([f"'{i}'" for i in submitted_ids]) if len(submitted_ids) > 0 else ''

    # Query for items w/ PMIDs
    eschol_sql = f"""
        select id as `eschol_id`, json_t.*
        from
            items i,
            JSON_TABLE(
                attrs,
                "$.local_ids[*]"
                COLUMNS(local_id_type varchar(255) PATH "$.type",
                        local_id_value varchar(255) PATH "$.id")
            ) as json_t
        where
            json_t.local_id_type in ('pmid')
            and (json_t.local_id_value not REGEXP('[^0-9]') 
                and json_t.local_id_value != '')
        order by i.added;
        """

# Goes above "order by i.added": and i.id not in ({exclude_ids_sql})

    with mysql_conn.cursor() as cursor:
        print("Connected to eSchol MySQL DB. Querying for items with PubMed IDs.")
        cursor.execute(eschol_sql)
        eschol_pmid_pubs = cursor.fetchall()
    mysql_conn.close()

    return eschol_pmid_pubs


# =========================
def create_resource_xml(items, output_dir, file_number, run_time):
    link_set = ET.Element("LinkSet")

    for item in items:
        link = ET.SubElement(link_set, "Link")
        ET.SubElement(link, "LinkId").text = item['eschol_id']
        ET.SubElement(link, "ProviderId").text = "7383"
        # ET.SubElement(link, "IconURL").text = "https://escholarship.org/images/pubmed_linkback.png"
        ET.SubElement(link, "IconUrl").text = "&icon.url;"

        # Link > ObjectSelector
        object_selector = ET.SubElement(link, "ObjectSelector")
        ET.SubElement(object_selector, "Database").text = "PubMed"

        # Link > ObjectSelector > ObjectList
        object_list = ET.SubElement(object_selector, "ObjectList")
        ET.SubElement(object_list, "ObjId").text = item['local_id_value']

        # Link > ObjectURL
        object_url = ET.SubElement(link, "ObjectUrl")
        # ET.SubElement(object_url, "Rule").text = f"https://escholarship.org/uc/item/{item['eschol_id']}"
        ET.SubElement(object_url, "Base").text = '&base.url;'
        ET.SubElement(object_url, "Rule").text = item['eschol_id']
        ET.SubElement(object_url, "UrlName").text = "Full text from University of California eScholarship"
        ET.SubElement(object_url, "Attribute").text = "full-text PDF"

    # Output XML file
    filename_date = run_time.split('T')[0]
    output_filename = f'{output_dir}/{filename_date}_{resource_filename_no_extension}_{file_number}.xml'
    with open(output_filename, 'w') as f:
        print(output_filename)

        # Add the header manually before the XML body
        doctype_header = '<?xml version="1.0" ?>\n' \
                         '<!DOCTYPE LinkSet PUBLIC "-//NLM//DTD LinkOut 1.0//EN" ' \
                         '"https://www.ncbi.nlm.nih.gov/projects/linkout/doc/LinkOut.dtd" ' \
                         '[<!ENTITY icon.url "https://escholarship.org/images/pubmed_linkback.png"> ' \
                         '<!ENTITY base.url "https://escholarship.org/uc/item/" > ]>\n'
        f.write(doctype_header)

        # Element tree: Convert to string, replace & html escaping
        ET.indent(link_set, space="\t", level=0)
        xml_string = ET.tostring(link_set, encoding='unicode')
        xml_string = xml_string.replace('&amp;', '&')
        f.write(xml_string)

    # Return the output filename
    return output_filename


# =========================
# https://docs.python.org/3/library/ftplib.html#ftplib.FTP.storbinary
def upload_xml_files_to_ftp(files, env):
    print("Connecting to PubMed Linkout FTP.")
    ftp = FTP(env['LINKOUT_FTP_URL'],
              env['LINKOUT_FTP_USER'],
              env['LINKOUT_FTP_PASSWORD'])
    # should return a 230 successful login

    ftp.cwd(env['LINKOUT_FTP_DIR'])
    # should return a 250 successful dir change

    print("Transferring files:")
    for file_name in files:
        xml_filename = file_name.split('/')[-1]
        print(xml_filename)
        with open(file_name, 'rb') as file:
            ftp.storbinary(f'STOR {xml_filename}', file)

    ftp.quit()


def update_logging_db(env, eschol_pmid_pubs):
    # Connect to logging DB
    mysql_conn = get_logging_db_connection(env)
    mysql_conn.autocommit(True)

    # Query for items w/ PMIDs
    with mysql_conn.cursor() as cursor:
        print("Connected to the logging DB, adding newly-submitted eSchol IDs")
        linkout_insert_sql = "INSERT INTO linkout_items (item_id, pmid) VALUES (%(eschol_id)s, %(local_id_value)s)"
        cursor.executemany(linkout_insert_sql, eschol_pmid_pubs)
        mysql_conn.commit()

    mysql_conn.close()


# =========================
if __name__ == '__main__':
    main()
