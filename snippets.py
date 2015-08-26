import logging
import argparse
import sys
import psycopg2

# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)

logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect("dbname='snippets2'")
logging.debug("Database connection established.")


def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="The name of the snippet")
    put_parser.add_argument("snippet", help="The snippet text")
    
    # Subparser for the get command
    logging.debug("Constructing get subparser")
    get_parser = subparsers.add_parser("get", help="Retrieve a snippet")
    get_parser.add_argument("name", help="The name of the snippet")
    
    # Subparser for the catalog command
    logging.debug("Constructing catalog subparser")
    catalog_parser = subparsers.add_parser("catalog", help="List names of stored snippets")
    
    # Subparer for the search command
    logging.debug("Constructing search subparser")
    search_parser = subparsers.add_parser("search", help="Search stored messages")
    search_parser.add_argument("string", help="The string to search for")

    arguments = parser.parse_args(sys.argv[1:])
    
    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")

    if command == "put":
        name, snippet = put(**arguments)
        print("Stored {!r} as {!r}".format(snippet, name))
    elif command == "get":
        snippet = get(**arguments)
        print("Retrieved snippet: {!r}".format(snippet))
    elif command == "catalog":
        print("Catalog of stored snippets: {!r}".format(catalog()))
    elif command == "search":
        results = search(**arguments)
        print("Search returned the following snippets: {!r}".format(results))
    
def search(string):
    """ search for a string within the stored messages"""
    logging.info("Search stored messages for a string")
    cursor = connection.cursor()
    command = "select * from snippets  where message  like %s"
    with connection, connection.cursor() as cursor:
        cursor.execute(command, ("%"+string+"%",))
        row = cursor.fetchall()
    if not row:
        return "nothing matches search term"
    return row

def catalog():
    """ Return a list containing the name of each stored snippet """
    logging.info("Returning catalog of snippet names")
    cursor = connection.cursor()
    command = "select keyword from snippets order by keyword"
    with connection, connection.cursor() as cursor:
        cursor.execute(command)
        row = cursor.fetchall()
    row = [i[0] for i in row]
    return row

def put(name, snippet):
    """Store a snippet with an associated name."""
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
    cursor = connection.cursor()
    
    with connection, connection.cursor() as cursor:
        try:
            command = "insert into snippets values (%s, %s)"
            cursor.execute(command, (name, snippet))
        except psycopg2.IntegrityError as e:
            connection.rollback()
            command = "update snippets set message=%s where keyword=%s"
            cursor.execute(command, (snippet, name))
    
        
    logging.debug("Snippet stored successfully.")
  
    return name, snippet


def get(name):
    """Retrieve the snippet with a given name.

    If there is no such snippet return string explaining error

    Returns the snippet.
    """
    logging.info("Retrieving snippet {!r}".format(name))
    command = "select * from snippets where keyword = %s"
    
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where keyword=%s", (name,))
        row = cursor.fetchone()
    
    logging.debug("Snippet retrieved successfully.")
    if not row:
        return "this entry doesn't exist"
    return row
  

if __name__ == "__main__":
    main()