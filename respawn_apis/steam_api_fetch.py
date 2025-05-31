# steam_api_fetch.py

import requests
import pandas as pd
import time
from pathlib import Path
from dotenv import load_dotenv
import os
from typing import Dict, Optional, List

# Load environment variables
load_dotenv()
STEAM_API_KEY = os.getenv("STEAM_API_KEY")

if not STEAM_API_KEY:
    raise ValueError("❌ STEAM_API_KEY not found in environment variables!")

# My comprehensive list of Steam games across different genres for mental health analysis
app_ids = {
    # Violent/Action Games (testing aggression hypothesis)
    "Call of Duty: Modern Warfare II": 1938090,
    "Grand Theft Auto V": 271590,
    "Counter-Strike 2": 730,
    "DOOM Eternal": 782330,
    "Mortal Kombat 11": 976310,
    "Cyberpunk 2077": 1091500,
    "Red Dead Redemption 2": 1174180,
    "Apex Legends": 1172470,
    "Valorant": 1943680,
    "Battlefield 2042": 1517290,
    
    # Peaceful/Relaxing Games (testing wellness hypothesis)
    "Stardew Valley": 413150,
    "Animal Crossing-style": 1062090,  # Spiritfarer
    "Journey": 638230,
    "ABZÛ": 384190,
    "A Short Hike": 1055540,
    "Unpacking": 1135690,
    "Coffee Talk": 914810,
    "Gris": 683320,
    "Firewatch": 383870,
    "What Remains of Edith Finch": 501300,
    
    # Competitive/Esports (testing anxiety hypothesis)
    "Dota 2": 570,
    "Rocket League": 252950,
    "Overwatch 2": 2357570,
    "Rainbow Six Siege": 359550,
    "Team Fortress 2": 440,
    "Chess Ultra": 314340,
    "Counter-Strike: Global Offensive": 10,
    "Age of Empires II": 813780,
    
    # Horror Games (testing anxiety/stress)
    "Phasmophobia": 739630,
    "Dead by Daylight": 381210,
    "Outlast": 238320,
    "Amnesia: The Dark Descent": 57300,
    "Resident Evil 4": 254700,
    "Five Nights at Freddy's": 319510,
    "The Forest": 242760,
    "Subnautica": 264710,
    
    # RPGs/Story-driven (testing immersion/escapism)
    "The Witcher 3": 292030,
    "Skyrim Special Edition": 489830,
    "Mass Effect Legendary Edition": 1328670,
    "Persona 5 Royal": 1687950,
    "Disco Elysium": 632470,
    "Baldur's Gate 3": 1086940,
    "Elden Ring": 1245620,
    "Final Fantasy XIV": 39210,
    
    # Puzzle/Strategy (testing cognitive benefits)
    "Portal 2": 620,
    "The Witness": 210970,
    "Tetris Effect": 1003590,
    "Civilization VI": 289070,
    "Chess Ultra": 314340,
    "Baba Is You": 736260,
    "Return of the Obra Dinn": 653530,
    "The Talos Principle": 257510,
    
    # Social/Party Games (testing social connection)
    "Among Us": 945360,
    "Fall Guys": 1097150,
    "It Takes Two": 1426210,
    "Overcooked! 2": 728880,
    "Gang Beasts": 285900,
    "Human Fall Flat": 477160,
    "Moving Out": 996770,
    "Jackbox Party Pack": 331670,
    
    # Simulation/Building (testing creativity/flow state)
    "Cities: Skylines": 255710,
    "The Sims 4": 1222670,
    "Planet Coaster": 493340,
    "Two Point Hospital": 535930,
    "Prison Architect": 233450,
    "Kerbal Space Program": 220200,
    "RimWorld": 294100,
    "Terraria": 105600,
    
    # Indie/Artistic (testing emotional impact)
    "Hades": 1145360,
    "Celeste": 504230,
    "Hollow Knight": 367520,
    "Ori and the Blind Forest": 261570,
    "Night in the Woods": 481510,
    "Inside": 304430,
    "Limbo": 48000,
    "Papers, Please": 239030,
    
    # Fitness/Active (testing physical wellness)
    "Beat Saber": 620980,
    "Dance Dance Revolution": 739650,
    "Pistol Whip": 1079800,
    "Audio Trip": 1046830
}

def fetch_game_details(app_id: int) -> Optional[Dict]:
    """
    Fetch detailed information for a specific Steam app.
    
    Args:
        app_id (int): Steam application ID
        
    Returns:
        Optional[Dict]: Game data dictionary if successful, None if failed
    """
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data[str(app_id)]["success"]:
            return data[str(app_id)]["data"]
    except Exception as e:
        print(f"❌ Failed to fetch app {app_id}: {e}")
    return None

def extract_game_data(game_data: Dict, app_id: int) -> Dict:
    """
    Extract and format relevant game data.
    
    Args:
        game_data (Dict): Raw game data from Steam API
        app_id (int): Steam application ID
        
    Returns:
        Dict: Formatted game data
    """
    return {
        "app_id": app_id,
        "game_title": game_data.get("name"),  # Changed to match expected column name
        "type": game_data.get("type"),
        "release_date": game_data.get("release_date", {}).get("date"),
        "price_usd": game_data.get("price_overview", {}).get("final", 0) / 100
        if game_data.get("price_overview") else 0,
        "metacritic_score": game_data.get("metacritic", {}).get("score"),
        "recommendations": game_data.get("recommendations", {}).get("total"),
        "categories": ", ".join([c["description"] for c in game_data.get("categories", [])]),
        "genres": ", ".join([g["description"] for g in game_data.get("genres", [])]),
        "short_description": game_data.get("short_description", ""),
        "developers": ", ".join(game_data.get("developers", [])),
        "publishers": ", ".join(game_data.get("publishers", [])),
    }

def fetch_and_save_steam_data() -> None:
    """
    Fetch Steam game data and save to raw data folder.
    """
    rows = []
    
    print("🎮 Fetching Steam game data...")
    for game_name, app_id in app_ids.items():
        print(f"  → Fetching {game_name}...")
        game_data = fetch_game_details(app_id)
        if game_data:
            rows.append(extract_game_data(game_data, app_id))
        time.sleep(1)  # Be polite to Steam's API
    
    df = pd.DataFrame(rows)
    
    # Save to RAW data folder (not cleaned)
    base_dir = Path(__file__).resolve().parents[1]
    raw_dir = base_dir / "respawn_data"
    raw_dir.mkdir(exist_ok=True)
    
    output_path = raw_dir / "steam_games.csv"
    df.to_csv(output_path, index=False)
    print(f"✅ Steam raw data saved to {output_path}")
    print(f"📊 Collected {len(df)} games")

if __name__ == "__main__":
    fetch_and_save_steam_data()