import requests
import json
import io
import csv
import psycopg
import configparser

def copy_scryfall_data_into_output(card_data):
    output = io.StringIO()
    writer = csv.writer(output)

    print("writing Rows to csv:")
    print(f"Progress: 0/{len(card_data)}")

    columns = ["english_name", "released_at", "mana_cost", "typeline", "power", "toughness", "loyalty", "oracle_text", "set_code", "art_url"]
    writer.writerow(columns)

    count = 0
    for card in card_data:
        row = []
        row.append(card["name"])

        # For now, exclude cards without a release date (i.e. cards that weren't release physically)
        if "released_at" in card:
            row.append(card["released_at"])
        else:
            continue

        if "mana_cost" in card:
            row.append(card["mana_cost"])
        else:
            row.append("")
        
        row.append(card["type_line"])

        if "power" in card:
            row.append(card["power"])
        else:
            row.append("")
        
        if "toughness" in card:
            row.append(card["toughness"])
        else:
            row.append("")

        if "loyalty" in card:
            row.append(card["loyalty"])
        else:
            row.append("")
        
        if "oracle_text" in card:
            row.append(card["oracle_text"])
        else:
            row.append("")
        
        row.append(card["set"])

        if card["image_status"] != "highres_scan":
            continue

        if "image-uris" in card:
            row.append(card["image_uris"]["png"])
        else:
            row.append("")
        
        writer.writerow(row)

        print(f"Progress: {count}/{len(card_data)}, last card is {card['name']}", end='\r')
        count += 1

    output.seek(0)
    return output

def populate_bulk_data():
    # Get the raw data from Scryfall
    raw_data_url = "https://api.scryfall.com/bulk-data/oracle-cards"

    r = requests.get(raw_data_url)
    raw_data = json.loads(r.text)

    r = requests.get(raw_data["download_uri"])
    card_data = json.loads(r.text)

    # COPY requires particular sets of formats, so we use csv
    data_stream = copy_scryfall_data_into_output(card_data)

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
            cursor.copy_from(data_stream, 'cards', sep='\t', columns=columns)
        
        conn.commit()

        cursor.close()
        conn.close()

    print("Database successfully populated")

if __name__ == "__main__":
    populate_bulk_data()