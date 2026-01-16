# LinkOut submission documentation
# https://www.ncbi.nlm.nih.gov/books/NBK3812/

from pub_oapi_tools_common import aws_lambda
from pub_oapi_tools_common import pub_oapi_tools_db

import datetime
import xml.etree.ElementTree as ET
from ftplib import FTP
import subprocess


# =========================
creds = {
    'linkout_db': {
        'folder': 'pub-oapi-tools/tools-rds',
        'env': 'prod',
        'names': ['server', 'pubmed-linkout-db', 'user', 'password']},
    'pubmed_ftp': {
        'folder': 'pub-oapi-tools/pubmed-linkout-ftp'},
    'emails': {
        'folder': 'pub-oapi-tools/emails',
        'names': ['devin', 'oapolicy-help']
    }
}

creds = aws_lambda.get_parameters(creds)
creds['linkout_db']['database'] = creds['linkout_db']['pubmed-linkout-db']


# =========================
def main():

    # Runtime string for dirs, filenames, logging DB
    run_time = datetime.datetime.now()
    run_time = run_time.replace(microsecond=0).isoformat()
    run_time = run_time.replace(':', "-")
    run_date = run_time.split('T')[0]

    output_dir = "output"
    submission_file = f"{run_date}_eschol_linkout_resource.xml"

    # Get the new items enqueued for submission
    new_items = get_new_items_for_submission(creds['linkout_db'])
    new_item_count = len(new_items)

    # Create the XML file
    submission_file_with_path = create_submission_file(new_items, output_dir, submission_file)

    # Send to PubMed FTP
    upload_submission_file_to_ftp(creds['pubmed_ftp'], submission_file_with_path, submission_file)

    # Update the logging DB
    update_logging_db(creds['linkout_db'], submission_file)

    # Email stakeholders
    send_notification_email(creds['emails'], submission_file, new_item_count)

    print("Program complete. Exiting.")


def get_new_items_for_submission(linkout_db):
    mysql_conn = pub_oapi_tools_db.getconnection(linkout_db)

    print("Connected to logging DB. Getting new items for submission.")
    with mysql_conn.cursor() as cursor:
        cursor.execute("""SELECT eschol_id, pubmed_id FROM linkout_items
            WHERE submitted IS NULL""")
        new_items = cursor.fetchall()
    mysql_conn.close()

    return new_items


def create_submission_file(new_items, output_dir, submission_file):

    # Create the XML from new_items dict
    xml_data = create_xml_data(new_items)

    # Export XML --> string to output file
    submission_file_with_path = f'{output_dir}/{submission_file}'
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

    # Return the output filename
    return submission_file_with_path


def create_xml_data(new_items):
    link_set = ET.Element("LinkSet")

    for item in new_items:
        link = ET.SubElement(link_set, "Link")
        ET.SubElement(link, "LinkId").text = item['eschol_id']
        ET.SubElement(link, "ProviderId").text = "7383"
        ET.SubElement(link, "IconUrl").text = "&icon.url;"

        # Link > ObjectSelector
        object_selector = ET.SubElement(link, "ObjectSelector")
        ET.SubElement(object_selector, "Database").text = "PubMed"

        # Link > ObjectSelector > ObjectList
        object_list = ET.SubElement(object_selector, "ObjectList")
        ET.SubElement(object_list, "ObjId").text = str(item['pubmed_id'])

        # Link > ObjectURL
        object_url = ET.SubElement(link, "ObjectUrl")
        ET.SubElement(object_url, "Base").text = '&base.url;'
        ET.SubElement(object_url, "Rule").text = item['eschol_id'][2:]
        ET.SubElement(object_url, "UrlName").text = "Full text from University of California eScholarship"
        ET.SubElement(object_url, "Attribute").text = "full-text PDF"

    return link_set


def upload_submission_file_to_ftp(ftp_creds, submission_file_with_path, submission_file):
    # https://docs.python.org/3/library/ftplib.html#ftplib.FTP.storbinary

    print("Connecting to PubMed Linkout FTP.")
    ftp = FTP(
        ftp_creds['url'],
        ftp_creds['user'],
        ftp_creds['password'])  # should return 230 successful login
    ftp.cwd(ftp_creds['dir'])  # should return 250 successful dir change

    print(f"Transferring: {submission_file}")
    with open(submission_file_with_path, 'rb') as file:
        ftp.storbinary(f'STOR {submission_file}', file)

    ftp.quit()


def update_logging_db(linkout_db, submission_file):
    mysql_conn = pub_oapi_tools_db.get_connection(linkout_db)

    print("Connected to logging DB. Updating submitted items.")
    with mysql_conn.cursor() as cursor:
        cursor.execute(f"""
            UPDATE linkout_items
            SET
                submitted = now(),
                pubmed_filename = '{submission_file}'
            WHERE pubmed_filename IS NULL""")
        mysql_conn.commit()

    mysql_conn.close()


# Set up the mail process with attachment and email recipients
def send_notification_email(emails, submission_file, new_item_count):
    subprocess_setup = ['mail', '-s', 'New UC eScholarship .xml file added to linkout FTP']
    subprocess_setup += [emails['devin'], emails['oapolicy-help']]
    input_byte_string = get_email_body_text(submission_file, new_item_count)

    # Run the subprocess
    subprocess.run(subprocess_setup, input=input_byte_string, capture_output=True)


# Split off into its own function bc of the weird formatting
def get_email_body_text(submission_file, new_item_count):
    return(b'''
Hello Pubmed,

An .xml file containing new publications for LinkOut has been added to our "holdings" folder on the FTP:

''' + submission_file.encode('UTF8') + b''' (''' + str(new_item_count).encode('UTF8') + b''' new publication links).

Thank you!
- CDL


Future-proofing Note:
This automated message is sent from the pubmed-linkout tool: https://github.com/eScholarship/pubmed-linkout ''')


# =========================
# Runs the program if the bit is 1, otherwise flip the bit and exit.
if __name__ == '__main__':
    with open("biweekly_bit.txt", 'r') as f:
        biweekly_bit = f.read().strip()

    if biweekly_bit == '1':
        biweekly_bit = 0
        main()
    else:
        biweekly_bit = 1

    with open("biweekly_bit.txt", 'w') as f:
        f.write(str(biweekly_bit))
