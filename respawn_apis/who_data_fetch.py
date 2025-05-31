# whois_data_fetch.py

import os
import requests
import pandas as pd
import time
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any
from datetime import datetime

# Load API key from .env
load_dotenv()
WHOIS_API_KEY = os.getenv("WHOIS_API_KEY")

if not WHOIS_API_KEY:
    raise EnvironmentError("❌ WHOIS_API_KEY not found in .env file")

# WHOIS API URL
WHOIS_URL = "https://www.whoisxmlapi.com/whoisserver/WhoisService"

# Gaming-related domains for mental health research
GAMING_DOMAINS = [
    # Gaming platforms
    "steam.com",
    "epicgames.com", 
    "xbox.com",
    "playstation.com",
    "nintendo.com",
    "twitch.tv",
    "discord.com",
    
    # Gaming companies
    "activision.com",
    "blizzard.com", 
    "ea.com",
    "ubisoft.com",
    "rockstargames.com",
    "valve.com",
    "riotgames.com",
    
    # Gaming communities/news
    "reddit.com",
    "ign.com",
    "gamespot.com",
    "polygon.com",
    "kotaku.com",
    
    # Mental health resources
    "nami.org",
    "mentalhealth.gov",
    "who.int",
    "apa.org",
    
    # Gaming addiction resources
    "olganon.org",
    "gamingtherapis.org",
    "cgaa.info"
]

def fetch_whois_data(domain: str) -> Optional[Dict[str, Any]]:
    """
    Fetch WHOIS information for a given domain name.
    
    Args:
        domain (str): Domain name to lookup
        
    Returns:
        Optional[Dict[str, Any]]: WHOIS data if successful, None if failed
    """
    params = {
        "apiKey": WHOIS_API_KEY,
        "domainName": domain,
        "outputFormat": "JSON"
    }
    
    try:
        response = requests.get(WHOIS_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Check if the response contains valid data
        if "WhoisRecord" in data:
            return data
        else:
            print(f"⚠️ No WHOIS record found for {domain}")
            return None
            
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTP error for {domain}: {http_err}")
    except requests.exceptions.RequestException as err:
        print(f"❌ Request error for {domain}: {err}")
    except ValueError as json_err:
        print(f"❌ JSON parsing error for {domain}: {json_err}")
    except Exception as e:
        print(f"❌ Unexpected error for {domain}: {e}")
    
    return None

def extract_domain_info(domain: str, whois_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract relevant information from WHOIS data for analysis.
    
    Args:
        domain (str): Domain name
        whois_data (Dict[str, Any]): Raw WHOIS data
        
    Returns:
        Dict[str, Any]: Extracted domain information
    """
    try:
        whois_record = whois_data.get("WhoisRecord", {})
        
        # Extract key information
        domain_info = {
            "domain": domain,
            "registrar_name": whois_record.get("registrarName"),
            "creation_date": whois_record.get("createdDate"),
            "expiration_date": whois_record.get("expiresDate"),
            "updated_date": whois_record.get("updatedDate"),
            "status": whois_record.get("status"),
            "name_servers": ", ".join(whois_record.get("nameServers", [])),
            "registrant_country": None,
            "registrant_organization": None,
            "admin_email": None,
            "tech_email": None,
            "is_gaming_platform": categorize_domain_type(domain),
            "data_collected_date": datetime.now().isoformat()
        }
        
        # Extract registrant information if available
        registrant = whois_record.get("registrant", {})
        if registrant:
            domain_info["registrant_country"] = registrant.get("country")
            domain_info["registrant_organization"] = registrant.get("organization")
        
        # Extract contact emails
        contacts = whois_record.get("contactEmail")
        if contacts:
            domain_info["admin_email"] = contacts
        
        return domain_info
        
    except Exception as e:
        print(f"⚠️ Error extracting data for {domain}: {e}")
        return {
            "domain": domain,
            "error": str(e),
            "data_collected_date": datetime.now().isoformat()
        }

def categorize_domain_type(domain: str) -> str:
    """
    Categorize domain by type for research purposes.
    
    Args:
        domain (str): Domain name
        
    Returns:
        str: Domain category
    """
    gaming_platforms = ["steam.com", "epicgames.com", "xbox.com", "playstation.com", "nintendo.com"]
    gaming_companies = ["activision.com", "blizzard.com", "ea.com", "ubisoft.com", "rockstargames.com"]
    social_platforms = ["twitch.tv", "discord.com", "reddit.com"]
    mental_health = ["nami.org", "mentalhealth.gov", "who.int", "apa.org"]
    gaming_addiction = ["olganon.org", "gamingtherapis.org", "cgaa.info"]
    
    if domain in gaming_platforms:
        return "gaming_platform"
    elif domain in gaming_companies:
        return "gaming_company"
    elif domain in social_platforms:
        return "social_platform"
    elif domain in mental_health:
        return "mental_health_resource"
    elif domain in gaming_addiction:
        return "gaming_addiction_resource"
    else:
        return "other"

def fetch_and_save_whois_data() -> None:
    """
    Fetch WHOIS data for all gaming-related domains and save to CSV.
    """
    print("🔍 Starting WHOIS data collection for gaming mental health research...")
    
    all_domain_data = []
    successful_lookups = 0
    failed_lookups = 0
    
    # Process each domain
    for i, domain in enumerate(GAMING_DOMAINS, 1):
        print(f"  → [{i}/{len(GAMING_DOMAINS)}] Fetching WHOIS for {domain}...")
        
        whois_data = fetch_whois_data(domain)
        
        if whois_data:
            domain_info = extract_domain_info(domain, whois_data)
            all_domain_data.append(domain_info)
            successful_lookups += 1
            print(f"    ✅ Success")
        else:
            # Still record failed lookups for completeness
            all_domain_data.append({
                "domain": domain,
                "error": "Failed to fetch WHOIS data",
                "is_gaming_platform": categorize_domain_type(domain),
                "data_collected_date": datetime.now().isoformat()
            })
            failed_lookups += 1
            print(f"    ❌ Failed")
        
        # Be respectful to the API
        time.sleep(1)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_domain_data)
    
    # Save to raw data folder
    base_dir = Path(__file__).resolve().parents[1]
    raw_dir = base_dir / "respawn_data"
    raw_dir.mkdir(exist_ok=True)
    
    output_path = raw_dir / "whois_gaming_domains.csv"
    df.to_csv(output_path, index=False)
    
    # Print summary
    print(f"\n🎉 WHOIS data collection completed!")
    print(f"✅ Successful lookups: {successful_lookups}")
    print(f"❌ Failed lookups: {failed_lookups}")
    print(f"📊 Total domains processed: {len(GAMING_DOMAINS)}")
    print(f"💾 Data saved to: {output_path}")
    
    # Show sample of collected data
    if not df.empty:
        print(f"\n📋 Sample data:")
        print(df[['domain', 'registrar_name', 'is_gaming_platform']].head())

def test_whois_api() -> bool:
    """
    Test the WHOIS API with a simple domain lookup.
    
    Returns:
        bool: True if API is working, False otherwise
    """
    print("🧪 Testing WHOIS API...")
    test_domain = "example.com"
    
    result = fetch_whois_data(test_domain)
    
    if result:
        print(f"✅ WHOIS API test successful for {test_domain}")
        return True
    else:
        print(f"❌ WHOIS API test failed for {test_domain}")
        return False

if __name__ == "__main__":
    # Test API first
    if test_whois_api():
        print("\n" + "="*50)
        fetch_and_save_whois_data()
    else:
        print("❌ Cannot proceed - WHOIS API test failed")
        print("💡 Please check your API key in the .env file")
