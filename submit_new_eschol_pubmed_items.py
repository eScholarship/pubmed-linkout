from dotenv import dotenv_values
import datetime
import pymysql
import os
import xml.etree.ElementTree as ET
from ftplib import FTP
import subprocess


# =========================
def get_logging_db_connection(env):
    mysql_conn = pymysql.connect(
        host=env['LOGGING_DB_SERVER'],
        user=env['LOGGING_DB_USER'],
        password=env['LOGGING_DB_PASSWORD'],
        database=env['LOGGING_DB_DATABASE'],
        cursorclass=pymysql.cursors.DictCursor)

    return mysql_conn


def main():
    env = dotenv_values(".env")

    # Runtime string for dirs, filenames, logging DB
    run_time = datetime.datetime.now()
    run_time = run_time.replace(microsecond=0).isoformat()
    run_time = run_time.replace(':', "-")
    run_date = run_time.split('T')[0]

    output_dir = f"output/{run_time}-pubmed-linkout-files"
    os.mkdir(f"./{output_dir}")
    resource_filename = "eschol_linkout_resource.xml"

    # Get the new items enqueued for submission
    new_items = get_new_items_for_submission(env)

    # Create the XML file
    submission_file_with_path = create_submission_file(new_items, run_date, output_dir, resource_filename)
    submission_filename = submission_file_with_path.split('/')[-1]

    # Send to PubMed FTP
    upload_submission_file_to_ftp(env, submission_file_with_path, submission_filename)

    # Update the logging DB
    update_logging_db(env, submission_filename)

    # Email stakeholders
    send_notification_email(env, submission_filename)

    print("Program complete. Exiting.")


def get_new_items_for_submission(env):
    mysql_conn = get_logging_db_connection(env)

    print("Connected to logging DB. Getting new items for submission.")
    with mysql_conn.cursor() as cursor:
        cursor.execute("""SELECT item_id, pmid FROM linkout_items
            WHERE submitted IS NULL""")
        new_items = cursor.fetchall()
    mysql_conn.close()

    return new_items


def create_submission_file(new_items, run_date, output_dir, resource_filename):

    # Create the XML from new_items dict
    xml_data = create_xml_data(new_items)

    # Export XML-->string to output file
    output_filename = f'{output_dir}/{run_date}_{resource_filename}'
    with open(output_filename, 'w') as f:
        print(f"Exporting: {output_filename}")

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

    # Return the output filename
    return output_filename


def create_xml_data(new_items):
    link_set = ET.Element("LinkSet")

    for item in new_items:
        link = ET.SubElement(link_set, "Link")
        ET.SubElement(link, "LinkId").text = item['item_id']
        ET.SubElement(link, "ProviderId").text = "7383"
        # ET.SubElement(link, "IconURL").text = "https://escholarship.org/images/pubmed_linkback.png"
        ET.SubElement(link, "IconUrl").text = "&icon.url;"

        # Link > ObjectSelector
        object_selector = ET.SubElement(link, "ObjectSelector")
        ET.SubElement(object_selector, "Database").text = "PubMed"

        # Link > ObjectSelector > ObjectList
        object_list = ET.SubElement(object_selector, "ObjectList")
        ET.SubElement(object_list, "ObjId").text = item['pmid']

        # Link > ObjectURL
        object_url = ET.SubElement(link, "ObjectUrl")
        # ET.SubElement(object_url, "Rule").text = f"https://escholarship.org/uc/item/{item['eschol_id']}"
        ET.SubElement(object_url, "Base").text = '&base.url;'
        ET.SubElement(object_url, "Rule").text = item['item_id']
        ET.SubElement(object_url, "UrlName").text = "Full text from University of California eScholarship"
        ET.SubElement(object_url, "Attribute").text = "full-text PDF"

    return link_set


def upload_submission_file_to_ftp(env, submission_file_with_path, submission_filename):
    # https://docs.python.org/3/library/ftplib.html#ftplib.FTP.storbinary

    print("Connecting to PubMed Linkout FTP.")
    ftp = FTP(env['LINKOUT_FTP_URL'],
              env['LINKOUT_FTP_USER'],
              env['LINKOUT_FTP_PASSWORD'])
    # should return a 230 successful login

    ftp.cwd(env['LINKOUT_FTP_DIR'])
    # should return a 250 successful dir change

    print(f"Transferring: {submission_filename}")
    with open(submission_file_with_path, 'rb') as file:
        ftp.storbinary(f'STOR {submission_filename}', file)

    ftp.quit()


def update_logging_db(env, submission_filename):
    mysql_conn = get_logging_db_connection(env)

    print("Connected to logging DB. Updating submitted items.")
    with mysql_conn.cursor() as cursor:
        cursor.execute(f"""
            UPDATE linkout_items
            SET
                submitted = now(),
                pubmed_filename = '{submission_filename}'
            WHERE pubmed_filename IS NULL""")
        mysql_conn.commit()

    mysql_conn.close()


def send_notification_email(env, submission_filename):
    # Set up the mail process with attachment and email recipients
    subprocess_setup = ['mail',
                        '-s', 'New UC eScholarship .xml file added to linkout FTP']
    # subprocess_setup += [env['DEVIN'], env['ALAINNA'], env['PUBMED_CONTACT']]
    subprocess_setup += [env['DEVIN'], env['ALAINNA']]

    input_byte_string = b'''Salutations,
    
An .xml file containing new publications for LinkOut has been added to our "holdings" folder on the FTP:

''' + submission_filename.encode('UTF8') + b'''.

Please email us at the CC'd address if you have any questions.

Thank you!'''

    # Run the subprocess
    subprocess.run(subprocess_setup, input=input_byte_string, capture_output=True)


# =========================
if __name__ == '__main__':
    main()