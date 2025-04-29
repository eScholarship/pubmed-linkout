from dotenv import dotenv_values
import datetime
import pymysql
import xml.etree.ElementTree as ET
from ftplib import FTP
import subprocess

# Batching vars
page_size = 20000
# batch_input_file = "input/ucpms-eschol-pubmed-batch-input.csv"


# =========================
def get_logging_db_connection(env):
    mysql_conn = pymysql.connect(
        host=env['LOGGING_DB_SERVER'],
        user=env['LOGGING_DB_USER'],
        password=env['LOGGING_DB_PASSWORD'],
        database=env['LOGGING_DB_DATABASE'],
        cursorclass=pymysql.cursors.DictCursor)

    return mysql_conn


# Returns a list of lists, of size n or fewer.
def chunk_into_n(full_list, n):
    for i in range(0, len(full_list), n):
        yield full_list[i:i + n]


def main():
    env = dotenv_values(".env")

    # Runtime string for dirs, filenames, logging DB
    run_time = datetime.datetime.now()
    run_time = run_time.replace(microsecond=0).isoformat()
    run_time = run_time.replace(':', "-")
    run_date = run_time.split('T')[0]

    output_dir = "output"

    # Get the new items enqueued for submission
    all_items = get_all_items(env)
    all_item_count = len(all_items)
    print(f"Full item count: {all_item_count}")

    # Create the XML files
    submission_file_stub = f"{run_date}_eschol_linkout_resource"
    submission_files_with_path = create_submission_files(
        all_items, output_dir, submission_file_stub)

    # Send to PubMed FTP
    upload_submission_files_to_ftp(
        env, output_dir, submission_files_with_path)

    # Update the logging DB
    update_logging_db(env, submission_file_stub)

    print("Program complete. Exiting.")


def get_all_items(env):
    mysql_conn = get_logging_db_connection(env)

    print("Connected to logging DB. Getting all items for resubmission.")
    with mysql_conn.cursor() as cursor:
        cursor.execute("SELECT eschol_id, pubmed_id FROM linkout_items")
        new_items = cursor.fetchall()
    mysql_conn.close()

    return new_items


def create_submission_files(all_items, output_dir, submission_file_stub):

    eschol_pmid_pubs_pages = list(chunk_into_n(all_items, page_size))
    submission_files_with_path = []

    print(f"{len(eschol_pmid_pubs_pages)} pages for batch upload.")
    for page_number, eschol_page in enumerate(eschol_pmid_pubs_pages):

        # Create the XML from new_items dict
        xml_data = create_xml_data(eschol_page)

        # Export XML --> string to output file
        file_number = str(page_number).zfill(5)
        submission_file_with_path = f'{output_dir}/{submission_file_stub}_{file_number}.xml'
        with open(submission_file_with_path, 'w') as f:
            print(f"Exporting: {submission_file_with_path}")

            # Add the header manually before the XML body
            doctype_header = '<?xml version="1.0" ?>\n' \
                             '<!DOCTYPE LinkSet PUBLIC "-//NLM//DTD LinkOut 1.0//EN" ' \
                             '"https://www.ncbi.nlm.nih.gov/projects/linkout/doc/LinkOut.dtd" ' \
                             '[<!ENTITY icon.url "https://escholarship.org/images/pubmed_linkback.png"> ' \
                             '<!ENTITY base.url "https://escholarship.org/uc/item/" > ]>\n'
            f.write(doctype_header)

            # Element tree: Convert to string, replace & html escaping
            ET.indent(xml_data, space="\t", level=0)
            xml_string = ET.tostring(xml_data, encoding='unicode')
            xml_string = xml_string.replace('&amp;', '&')
            f.write(xml_string)

        submission_files_with_path.append(submission_file_with_path)

    # Return the output filename
    return submission_files_with_path


def create_xml_data(new_items):
    link_set = ET.Element("LinkSet")

    for item in new_items:
        link = ET.SubElement(link_set, "Link")
        ET.SubElement(link, "LinkId").text = item['eschol_id'][2:]
        ET.SubElement(link, "ProviderId").text = "7383"
        # ET.SubElement(link, "IconURL").text = "https://escholarship.org/images/pubmed_linkback.png"
        ET.SubElement(link, "IconUrl").text = "&icon.url;"

        # Link > ObjectSelector
        object_selector = ET.SubElement(link, "ObjectSelector")
        ET.SubElement(object_selector, "Database").text = "PubMed"

        # Link > ObjectSelector > ObjectList
        object_list = ET.SubElement(object_selector, "ObjectList")
        ET.SubElement(object_list, "ObjId").text = str(item['pubmed_id'])

        # Link > ObjectURL
        object_url = ET.SubElement(link, "ObjectUrl")
        # ET.SubElement(object_url, "Rule").text = f"https://escholarship.org/uc/item/{item['eschol_id']}"
        ET.SubElement(object_url, "Base").text = '&base.url;'
        ET.SubElement(object_url, "Rule").text = item['eschol_id']
        ET.SubElement(object_url, "UrlName").text = "Full text from University of California eScholarship"
        ET.SubElement(object_url, "Attribute").text = "full-text PDF"

    return link_set


def upload_submission_files_to_ftp(env, output_dir, submission_file_with_path):
    # https://docs.python.org/3/library/ftplib.html#ftplib.FTP.storbinary

    print("Connecting to PubMed Linkout FTP.")
    ftp = FTP(env['LINKOUT_FTP_URL'],
              env['LINKOUT_FTP_USER'],
              env['LINKOUT_FTP_PASSWORD'])  # should return 230 successful login

    ftp.cwd(env['LINKOUT_FTP_DIR'])  # should return 250 successful dir change

    for submission_file_with_path in submission_file_with_path:
        submission_file = submission_file_with_path.split(f"{output_dir}/")[1]

        print(f"Transferring: {submission_file}")
        with open(submission_file_with_path, 'rb') as file:
            ftp.storbinary(f'STOR {submission_file}', file)

    ftp.quit()


def update_logging_db(env, submission_file_stub):
    mysql_conn = get_logging_db_connection(env)
    submission_file = f"{submission_file_stub}_*"

    print("Connected to logging DB. Updating submitted items.")
    with mysql_conn.cursor() as cursor:
        cursor.execute(f"""
            UPDATE linkout_items
            SET
                submitted = now(),
                pubmed_filename = '{submission_file}';
            """)
        mysql_conn.commit()

    mysql_conn.close()



# =========================
if __name__ == '__main__':
    main()
