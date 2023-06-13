import pymongo
from pymongo import MongoClient
from pprint import pprint
import getpass
from menu_definitions import menu_main
from menu_definitions import add_menu
from menu_definitions import delete_menu
from menu_definitions import list_menu


def add(db):
    """
    Present the add menu and execute the user's selection.
    :param db:  The connection to the current database.
    :return:    None
    """
    add_action: str = ''
    while add_action != add_menu.last_action():
        add_action = add_menu.menu_prompt()
        exec(add_action)


def delete(db):
    """
    Present the delete menu and execute the user's selection.
    :param db:  The connection to the current database.
    :return:    None
    """
    delete_action: str = ''
    while delete_action != delete_menu.last_action():
        delete_action = delete_menu.menu_prompt()
        exec(delete_action)


def list_objects(db):
    """
    Present the list menu and execute the user's selection.
    :param db:  The connection to the current database.
    :return:    None
    """
    list_action: str = ''
    while list_action != list_menu.last_action():
        list_action = list_menu.menu_prompt()
        exec(list_action)


def add_department(db):
    """
    Add a new student, making sure that we don't put in any duplicates,
    based on all the candidate keys (AKA unique indexes) on the
    students collection.  Theoretically, we could query MongoDB to find
    the uniqueness constraints in place, and use that information to
    dynamically decide what searches we need to do to make sure that
    we don't violate any of the uniqueness constraints.  Extra credit anyone?
    :param collection:  The pointer to the students collection.
    :return:            None
    """
    # Create a "pointer" to the students collection within the db database.
    collection = db["departments"]
    unique_name: bool = False
    unique_abbreviation: bool = False
    unique_chair_name: bool = False
    unique_building_office: bool = False
    unique_description: bool = False
    name: str = ''
    abbreviation: str = ''
    chairName: str = ''
    building: str = ''
    office: int = -1
    description: str = ''
    while not unique_abbreviation or not unique_chair_name or not unique_building_office or not unique_description:
        name = input("Department name--> ")
        abbreviation = input("Department abbreviation--> ")
        chairName = input("Department chair name--> ")
        building = input("Department building--> ")
        office = input("Department office--> ")
        description = input("Department description--> ")

        name_count: int = collection.count_documents({"name": name})
        unique_name = name_count == 0
        if not unique_name:
            print("We already have a department by that name.  Try again.")
        if unique_name:
            abbreviation_count: int = collection.count_documents({"abbreivation": abbreviation})
            unique_abbreviation = abbreviation_count == 0
            if not unique_abbreviation:
                print("We already have a department with that abbreviation.  Try again.")
            if unique_abbreviation:
                chair_name_count: int = collection.count_documents({"chair_name": chairName})
                unique_chair_name = chair_name_count == 0
                if not unique_chair_name:
                    print("We already have a department with that chair name.  Try again.")
                if unique_chair_name:
                    building_office_count: int = collection.count_documents({"building": building, "office": office})
                    unique_building_office = building_office_count == 0
                    if not unique_building_office:
                        print("We already have a department with that building and office.  Try again.")
                    if unique_building_office:
                        description_count: int = collection.count_documents({"description": description})
                        unique_description = description_count == 0
                        if not unique_description:
                            print("We already have a department with that description.  Try again.")
    # Build a new students document preparatory to storing it
    department = {
        "name": name,
        "abbreviation": abbreviation,
        "chair_name": chairName,
        "building":  building,
        "office": office,
        "description": description
    }
    results = collection.insert_one(department)


def select_department(db):
    """
    Select a student by the combination of the last and first.
    :param db:      The connection to the database.
    :return:        The selected student as a dict.  This is not the same as it was
                    in SQLAlchemy, it is just a copy of the Student document from
                    the database.
    """
    # Create a connection to the students collection from this database
    collection = db["departments"]
    found: bool = False
    name: str = ''
    abbreviation: str = ''
    while not found:
        name = input("department name--> ")
        abbreviation = input("department abbreviation--> ")
        name_count: int = collection.count_documents({"name": name, "abbreviation": abbreviation})
        found = name_count == 1
        if not found:
            print("No department found by that name.  Try again.")
    found_department = collection.find_one({"name": name, "abbreviation": abbreviation})
    return found_department


def delete_department(db):
    """
    Delete a student from the database.
    :param db:  The current database connection.
    :return:    None
    """
    # student isn't a Student object (we have no such thing in this application)
    # rather it's a dict with all the content of the selected student, including
    # the MongoDB-supplied _id column which is a built-in surrogate.
    list_department(db)
    department = select_department(db)
    # Create a "pointer" to the students collection within the db database.
    departments = db["departments"]
    # student["_id"] returns the _id value from the selected student document.
    deleted = departments.delete_one({"abbreviation": department["abbreviation"]})
    # The deleted variable is a document that tells us, among other things, how
    # many documents we deleted.
    print(f"We just deleted: {deleted.deleted_count} departments.")


def list_department(db):
    """
    List all of the students, sorted by last name first, then the first name.
    :param db:  The current connection to the MongoDB database.
    :return:    None
    """
    # No real point in creating a pointer to the collection, I'm only using it
    # once in here.  The {} inside the find simply tells the find that I have
    # no criteria.  Essentially this is analogous to a SQL find * from students.
    # Each tuple in the sort specification has the name of the field, followed
    # by the specification of ascending versus descending.
    departments = db["departments"].find({}).sort([("name", pymongo.ASCENDING)])
    # pretty print is good enough for this work.  It doesn't have to win a beauty contest.
    for department in departments:
        pprint(department)


if __name__ == '__main__':
    password: str = getpass.getpass('Mongo DB password -->')
    username: str = input('Database username [29560633] -->') or \
                    "29560633"
    project: str = input('Mongo project name [CECS323] -->') or \
                   "CECS323"
    hash_name: str = input('7-character database hash [vy1mine] -->') or "vy1mine"
    cluster = f"mongodb+srv://{username}:{password}@{project}.{hash_name}.mongodb.net/?retryWrites=true&w=majority"
    print(f"Cluster: mongodb+srv://{username}:********@{project}.{hash_name}.mongodb.net/?retryWrites=true&w=majority")
    client = MongoClient(cluster)
    # As a test that the connection worked, print out the database names.
    print(client.list_database_names())
    # db will be the way that we refer to the database from here on out.
    db = client["CECS323"]
    # Print off the collections that we have available to us, again more of a test than anything.
    print(db.list_collection_names())
    # student is our students collection within this database.
    # Merely referencing this collection will create it, although it won't show up in Atlas until
    # we insert our first document into this collection.
    departments = db["departments"]
    department_count = departments.count_documents({})
    print(f"Departments in the collection so far: {department_count}")

    # ************************** Set up the students collection
    departments_indexes = departments.index_information()
    if 'departments_abbreviations' in departments_indexes.keys():
        print("abbreviation index present.")
    else:
        # Create a UNIQUE index on just the e-mail address
        departments.create_index([('abbreviation', pymongo.ASCENDING)], unique=True, name='departments_abbreviations')
    if 'departments_chair_names' in departments_indexes.keys():
        print("chair name index present.")
    else:
        # Create a UNIQUE index on just the e-mail address
        departments.create_index([('chair_name', pymongo.ASCENDING)], unique=True, name='departments_chair_names')
    if 'departments_buildings_and_offices' in departments_indexes.keys():
        print("buildings and offices index present.")
    else:
        # Create a single UNIQUE index on BOTH the last name and the first name.
        departments.create_index([('building', pymongo.ASCENDING), ('office', pymongo.ASCENDING)], unique=True,
                                 name="departments_buildings_and_offices")
    if 'departments_descriptions' in departments_indexes.keys():
        print("description index present.")
    else:
        # Create a UNIQUE index on just the e-mail address
        departments.create_index([('description', pymongo.ASCENDING)], unique=True, name='departments_descriptions')
    pprint(departments.index_information())
    main_action: str = ''
    while main_action != menu_main.last_action():
        main_action = menu_main.menu_prompt()
        print('next action: ', main_action)
        exec(main_action)

