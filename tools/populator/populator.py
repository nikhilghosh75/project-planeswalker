import requests
import json
import io
import csv
import psycopg
import configparser

def format_data_for_insert(card_data):
    cards = []
    for card in card_data:
        row = []
        row.append(card["name"])

        released_at = ""

        # For now, exclude cards without a release date (i.e. cards that weren't release physically)
        if "released_at" in card:
            released_at = card["released_at"]
        else:
            continue

        mana_cost = ""
        if "mana_cost" in card:
            mana_cost = row.append(card["mana_cost"])
        
        row.append(card["type_line"])

        power = ""
        if "power" in card and card["power"].isdigit():
            power = int(card["power"])
        
        toughness = ""
        if "toughness" in card and card["toughness"].isdigit():
            toughness = int(card["toughness"])

        loyalty = ""
        if "loyalty" in card and card["loyalty"].isdigit():
            loyalty = int(card["loyalty"])
        
        oracle_text = ""
        if "oracle_text" in card:
            oracle_text = card["oracle_text"]
        

        if card["image_status"] != "highres_scan":
            continue

        image_url = ""
        if "image-uris" in card:
            image_url = card["image_uris"]["png"]

        cards.append((card["name"], released_at, mana_cost, power, toughness, loyalty, oracle_text, card["set"], image_url))
    
    return cards


def populate_bulk_data():
    # Get the raw data from Scryfall
    raw_data_url = "https://api.scryfall.com/bulk-data/oracle-cards"

    r = requests.get(raw_data_url)
    raw_data = json.loads(r.text)

    r = requests.get(raw_data["download_uri"])
    card_data = json.loads(r.text)

    cards = format_data_for_insert(card_data)

    config = configparser.ConfigParser()
    config.read("populator.conf")

    populator_conf = config["populator"]

    database_name = populator_conf["database_name"]
    username = populator_conf["username"]
    password = populator_conf["password"]
    host = populator_conf["host"]

    print("Starting database population")

    with psycopg.connect(f"dbname={database_name} user={username} password={password} host={host}") as conn:
        with conn.cursor() as cursor:
            columns = ["english_name", "released_at", "mana_cost", "typeline", "power", "toughness", "loyalty", "oracle_text", "set_code", "art_url"]
            sql = """
                    INSERT INTO users (english_name, released_at, mana_cost, typeline, power, toughness, loyalty, oracle_text, set_code, art_url)
                    VALUES %s
                """
            
            cursor.execute(sql, (cards,))
        
        conn.commit()

        cursor.close()
        conn.close()

    print("Database successfully populated")

if __name__ == "__main__":
    populate_bulk_data()