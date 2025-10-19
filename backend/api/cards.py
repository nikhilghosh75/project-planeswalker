from fastapi import APIRouter

router = APIRouter()

router.get("/card/{card_id}")
async def get_card_by_id(card_id : int):
    #TODO: make this work
    return {"card_id": 1, 
            "english_name": "Nissa, Worldsoul Speaker", 
            "released_at": "2025-02-14", 
            "mana_cost": "{3}{G}",
            "typeline": "Legendary Creature — Elf Druid", 
            "power": 3,
            "toughness": 3,
            "oracle_text": "Landfall — Whenever a land you control enters, you get {E}{E} (two energy counters).\nYou may pay eight {E} rather than pay the mana cost for permanent spells you cast.",
            "set_code": "DFT",
            "art_url": "https://cards.scryfall.io/png/front/a/4/a471b306-4941-4e46-a0cb-d92895c16f8a.png?1738355341"}