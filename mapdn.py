#!/usr/bin/python
# 
# Author: Aaron Maharry
# Last modified: 2016-03-03
#
# This script interfaces with a MySQL database to add, delete, or view mappings
# between a user's distinguished name and OSC account. This script is totaly 
# trusting; user authentication will be handled by the web app frontend.

import argparse
import MySQLdb
import sys
import pika
import os
import re

# Define constants for the database
DATABASE_NAME = "mapdn"
DATABASE_USERNAME = "admindn"
DATABASE_PASSWORD_FILE = "~amaharry/mapdn/admindn"
DATABASE_HOST = "xio29.ten.osc.edu"
MAX_DN_LENGTH = 255
VALID_DN_CHARS = "^[A-Za-z0-9:/,.@\(\)\-+='_ ]+$"

# Define constants for the AMQP Exchange. Uncomment whem functionality is added
# EXCHANGE_HOST = "localhost"
# EXCHANGE_NAME = "federated_ids"

def main():

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Modify the DN mapping \
                                                    database")
    parser.add_argument("-a", "--add", action="store_true", help="add the \
                            distinguished name to the database")
    parser.add_argument("-d", "--delete", action="store_true", help="delete the\
                            distinguished name from the database")
    parser.add_argument("-e", "--export", action="store_true", help="export the\
                            contents of the database")
    parser.add_argument("--dn", help="The distinguished name")
    parser.add_argument("--user", help="The authenticated OSC username")
    args = parser.parse_args()

    # Check that conflicting operations weren't provided
    if args.add and args.delete:
        print("ERROR: Must choose either --add or --delete.")
        sys.exit(1)

    # Check that a DN and username were provided for an add or delete operation
    if args.add or args.delete:
        if args.dn == None or args.user == None:
            print("ERROR: Must provide --dn and --user options for adding and "
                    "deleting entries in the database")
            sys.exit(1)
        if len(args.dn) > MAX_DN_LENGTH:
            print("ERROR: Distinguished Name of length %d is longer than " \
                    "maximum length %d" %(len(args.dn), MAX_DN_LENGTH))
            sys.exit(1)
        if not re.match(VALID_DN_CHARS,args.dn):
            print ("ERROR:  Invalid character in Distinguished Name")
            sys.exit(1)

    # Check that at least one operation (add, delete, or export) was chosen
    if not args.export and not args.add and not args.delete:
        print("ERROR: Must specify add, delete, or export operation.")
        sys.exit(1)

    # Read the database password from the file.
    try:
        password_file = open(os.path.expanduser(DATABASE_PASSWORD_FILE), "ro")
        password = password_file.readline().rstrip("\n")
        password_file.close()
    except IOError as e:
        print("ERROR: Failed to read database password. %s" %e)
        sys.exit(1)

    # Connect to the database
    conn = MySQLdb.connect(db=DATABASE_NAME, user=DATABASE_USERNAME, \
                            passwd=password, host=DATABASE_HOST)
    c = conn.cursor()

    # Perform the operation on the database
    isDatabaseChanged = False
    if args.add:
        try:
            c.execute("INSERT INTO dnusermap VALUES (%s,%s)", (args.dn, \
                                                             args.user))
            isDatabaseChanged = True
        except MySQLdb.IntegrityError:
            print("WARNING: Distinguished name is already mapped")
            sys.exit(1)
    elif args.delete:
        # Determine if this entry already exists
        c.execute("SELECT dn, user FROM dnusermap WHERE dn=%s AND user=%s", \
                                                      (args.dn, args.user))
        if len(c.fetchall()) > 0:
            # This entry exists and will be deleted
            isDatabaseChanged = True
        c.execute("DELETE FROM dnusermap WHERE dn=%s AND user=%s", (args.dn, \
                                                                args.user))
    
    if args.export:
        if args.user is not None:
            c.execute("SELECT dn, user FROM dnusermap WHERE user=%s ORDER BY user, dn", (args.user))
        else:
            c.execute("SELECT dn, user FROM dnusermap ORDER BY user, dn")
        for line in c.fetchall():
            # Print each entry in the grid mapfile format: "dn" username
            print("\"%s\" %s" %(line[0],line[1]))

    # Commit and close the database
    conn.commit()
    c.close()
    conn.close()

    # This is prototype code for future functionality for this script
    #if isDatabaseChanged:
        # Publish a notification to the AMQP exchange
        # connection = pika.BlockingConnection(pika.ConnectionParameters(EXCHANGE_HOST))
        # channel = connection.channel()
        # channel.exchange_declare(exchange=EXCHANGE_NAME, type='fanout')
        # message = "mapdn database modified"
        # channel.basic_publish(exchange=EXCHANGE_NAME, routing_key='', \
        #                         body=message)
        # connection.close()

if __name__ == '__main__':
    main()
