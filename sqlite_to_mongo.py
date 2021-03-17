import sqlite3
import pymongo


def create_mdb_connection(collection_name):
    client = pymongo.MongoClient(
        ("mongodb://172.18.0.4/{}?retryWrites=true&w=majority"
    ).format(collection_name))
    return client


def create_sl_connection(extraction_db="rpg_db.sqlite3"):
    sl_conn = sqlite3.connect(extraction_db)
    return sl_conn


def execute_query(curs, query):
    return curs.execute(query).fetchall()


character_query = """
    SELECT * FROM charactercreator_character;
    """

item_query = """
SELECT * FROM armory_item left join armory_weapon on armory_item.item_id = armory_weapon.item_ptr_id;
    """


def insert_documents_from_sqlite(mongo_db, sl_curs, character_query, item_query):

    #items = execute_query(sl_curs, item_query)
    characters = execute_query(sl_curs, character_query)
    for character in characters:
        keys = ["name", "level", "exp", "hp", "strength", "intelligence", "dexterity", "wisdom"]
        doc = {}
        for i, key in enumerate(keys):
            key_value = {key: character[i]}
            doc.update(key_value)
        mongo_db.insert_one(doc)


def show_all(collection):
    all_docs = list(collection.find())
    return all_docs


sl_conn = create_sl_connection()
sl_curs = sl_conn.cursor()
client = create_mdb_connection("rpg_data")
collection = client.rpg_data.rpg_data
collection.drop({})
insert_documents_from_sqlite(collection, sl_curs, character_query, item_query)

print(show_all(collection))