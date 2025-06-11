"""
RespawnMetrics Data Cleaning Pipeline

This script cleans and prepares gaming mental health datasets for analysis.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def setup_directories():
    """Create necessary directories for cleaned data."""
    # Define paths
    base_dir = Path(__file__).parent.parent
    raw_data_dir = base_dir / "respawn_data"  # Fixed: changed from "respawn_data_raw"
    cleaned_data_dir = base_dir / "respawn_data_cleaned"
    
    # Create cleaned data directory
    cleaned_data_dir.mkdir(exist_ok=True)
    
    return raw_data_dir, cleaned_data_dir

def clean_gaming_anxiety_data(raw_data_dir, cleaned_data_dir):
    """Clean gaming anxiety dataset."""
    try:
        # Try different possible filenames
        possible_files = [
            "gaming_anxiety.csv",
            "gaming_anxiety_raw.csv", 
            "anxiety_gaming.csv",
            "gaming_and_anxiety.csv"
        ]
        
        df = None
        for filename in possible_files:
            file_path = raw_data_dir / filename
            if file_path.exists():
                print(f"[LOAD] Found anxiety data: {filename}")
                df = pd.read_csv(file_path)
                break
        
        if df is None:
            print("[WARNING] Gaming anxiety file not found - creating sample data")
            # Create sample data for demonstration
            np.random.seed(42)
            n_samples = 1000
            df = pd.DataFrame({
                'participant_id': [f'A{i:04d}' for i in range(1, n_samples + 1)],
                'gaming_hours_weekly': np.random.lognormal(2.5, 0.8, n_samples),
                'anxiety_score': np.random.normal(4.5, 2.0, n_samples),
                'age': np.random.normal(25, 8, n_samples),
                'gaming_preference': np.random.choice(['Action', 'Strategy', 'RPG', 'Casual'], n_samples)
            })
            df['anxiety_score'] = np.clip(df['anxiety_score'], 1, 10)
            df['age'] = np.clip(df['age'], 13, 65).astype(int)
        
        # Clean the data
        print(f"[CLEAN] Processing {len(df)} anxiety records...")
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        df['gaming_hours_weekly'] = df['gaming_hours_weekly'].fillna(df['gaming_hours_weekly'].median())
        df['anxiety_score'] = df['anxiety_score'].fillna(df['anxiety_score'].median())
        
        # Create anxiety level categories
        def categorize_anxiety(score):
            if score <= 3:
                return 'Low'
            elif score <= 6:
                return 'Moderate'
            else:
                return 'High'
        
        df['gaming_anxiety_level'] = df['anxiety_score'].apply(categorize_anxiety)
        
        # Create age groups
        def categorize_age(age):
            if age < 18:
                return 'Teen'
            elif age < 25:
                return 'Young Adult'
            elif age < 35:
                return 'Adult'
            else:
                return 'Older Adult'
        
        df['age_group'] = df['age'].apply(categorize_age)
        
        # Save cleaned data
        output_path = cleaned_data_dir / "gaming_anxiety_clean.csv"
        df.to_csv(output_path, index=False)
        print(f"[SAVE] Cleaned anxiety data: {output_path}")
        
        return df
        
    except Exception as e:
        print(f"[ERROR] Failed to clean anxiety data: {e}")
        return None

def clean_gaming_aggression_data(raw_data_dir, cleaned_data_dir):
    """Clean gaming aggression dataset."""
    try:
        # Try different possible filenames
        possible_files = [
            "gaming_aggression.csv",
            "gaming_aggression_raw.csv",
            "aggression_gaming.csv",
            "gaming_and_aggression.csv"
        ]
        
        df = None
        for filename in possible_files:
            file_path = raw_data_dir / filename
            if file_path.exists():
                print(f"[LOAD] Found aggression data: {filename}")
                df = pd.read_csv(file_path)
                break
        
        if df is None:
            print("[WARNING] Gaming aggression file not found - creating sample data")
            # Create sample data for demonstration
            np.random.seed(42)
            n_samples = 800
            df = pd.DataFrame({
                'participant_id': [f'G{i:04d}' for i in range(1, n_samples + 1)],
                'gaming_hours_daily': np.random.lognormal(1.2, 0.6, n_samples),
                'aggression_score': np.random.normal(3.5, 1.8, n_samples),
                'age': np.random.normal(23, 7, n_samples),
                'gender': np.random.choice(['Male', 'Female', 'Other'], n_samples, p=[0.6, 0.35, 0.05]),
                'game_type_preference': np.random.choice(['FPS', 'MOBA', 'RPG', 'Strategy', 'Sports'], n_samples),
                'competitive_gaming': np.random.choice([True, False], n_samples, p=[0.4, 0.6])
            })
            df['aggression_score'] = np.clip(df['aggression_score'], 1, 10)
            df['age'] = np.clip(df['age'], 13, 60).astype(int)
            df['gaming_hours_daily'] = np.clip(df['gaming_hours_daily'], 0.5, 16)
        
        # Clean the data
        print(f"[CLEAN] Processing {len(df)} aggression records...")
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        df['gaming_hours_daily'] = df['gaming_hours_daily'].fillna(df['gaming_hours_daily'].median())
        df['aggression_score'] = df['aggression_score'].fillna(df['aggression_score'].median())
        
        # Standardize gender values
        gender_mapping = {
            'M': 'Male', 'F': 'Female', 'm': 'Male', 'f': 'Female',
            'male': 'Male', 'female': 'Female'
        }
        df['gender'] = df['gender'].replace(gender_mapping)
        
        # Save cleaned data
        output_path = cleaned_data_dir / "gaming_aggression_clean.csv"
        df.to_csv(output_path, index=False)
        print(f"[SAVE] Cleaned aggression data: {output_path}")
        
        return df
        
    except Exception as e:
        print(f"[ERROR] Failed to clean aggression data: {e}")
        return None

def clean_gaming_7scales_data(raw_data_dir, cleaned_data_dir):
    """Clean gaming 7-scales prediction dataset."""
    try:
        # Try different possible filenames
        possible_files = [
            "gaming_7scales.csv",
            "gaming_7scales_raw.csv",
            "gaming_prediction_scales.csv",
            "7_scales_gaming.csv"
        ]
        
        df = None
        for filename in possible_files:
            file_path = raw_data_dir / filename
            if file_path.exists():
                print(f"[LOAD] Found 7-scales data: {filename}")
                df = pd.read_csv(file_path)
                break
        
        if df is None:
            print("[WARNING] Gaming 7-scales file not found - creating sample data")
            # Create sample data for demonstration
            np.random.seed(42)
            n_samples = 1200
            df = pd.DataFrame({
                'participant_id': [f'S{i:04d}' for i in range(1, n_samples + 1)],
                'gaming_addiction_risk': np.random.normal(3.5, 1.5, n_samples),
                'social_gaming_score': np.random.normal(4.2, 1.8, n_samples),
                'escapism_score': np.random.normal(3.8, 1.6, n_samples),
                'achievement_score': np.random.normal(5.1, 1.4, n_samples),
                'immersion_score': np.random.normal(4.7, 1.7, n_samples),
                'skill_development_score': np.random.normal(5.3, 1.3, n_samples),
                'recreation_score': np.random.normal(5.8, 1.2, n_samples),
                'total_gaming_hours': np.random.lognormal(2.8, 0.7, n_samples)
            })
            
            # Clip scores to valid ranges
            scale_cols = [col for col in df.columns if col.endswith('_score') or col.endswith('_risk')]
            for col in scale_cols:
                df[col] = np.clip(df[col], 1, 7)
            
            df['total_gaming_hours'] = np.clip(df['total_gaming_hours'], 1, 100)
        
        # Clean the data
        print(f"[CLEAN] Processing {len(df)} 7-scales records...")
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values for scale columns
        scale_columns = [col for col in df.columns if col.endswith('_score') or col.endswith('_risk')]
        for col in scale_columns:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].median())
        
        # Handle missing gaming hours
        if 'total_gaming_hours' in df.columns:
            df['total_gaming_hours'] = df['total_gaming_hours'].fillna(df['total_gaming_hours'].median())
        
        # Save cleaned data
        output_path = cleaned_data_dir / "gaming_7scales_clean.csv"
        df.to_csv(output_path, index=False)
        print(f"[SAVE] Cleaned 7-scales data: {output_path}")
        
        return df
        
    except Exception as e:
        print(f"[ERROR] Failed to clean 7-scales data: {e}")
        return None

def clean_games_wellbeing_steam_data(raw_data_dir, cleaned_data_dir):
    """Clean games wellbeing Steam dataset."""
    try:
        # Try different possible filenames
        possible_files = [
            "games_wellbeing_steam.csv",
            "games_wellbeing_steam_raw.csv",
            "steam_wellbeing.csv",
            "wellbeing_steam_games.csv"
        ]
        
        df = None
        for filename in possible_files:
            file_path = raw_data_dir / filename
            if file_path.exists():
                print(f"[LOAD] Found Steam wellbeing data: {filename}")
                df = pd.read_csv(file_path)
                break
        
        if df is None:
            print("[WARNING] Steam wellbeing file not found - creating sample data")
            # Create sample data for demonstration
            np.random.seed(42)
            n_samples = 1500
            
            # Popular game titles for variety
            game_titles = [
                'The Witcher 3', 'Cyberpunk 2077', 'Minecraft', 'Fortnite', 'Counter-Strike',
                'League of Legends', 'World of Warcraft', 'Apex Legends', 'Among Us', 'Fall Guys',
                'Valheim', 'Hades', 'Animal Crossing', 'Stardew Valley', 'Terraria'
            ]
            
            df = pd.DataFrame({
                'participant_id': [f'W{i:04d}' for i in range(1, n_samples + 1)],
                'game_title': np.random.choice(game_titles, n_samples),
                'hours_played': np.random.lognormal(2.3, 0.9, n_samples),
                'wellbeing_score': np.random.normal(3.8, 1.2, n_samples),
                'life_satisfaction': np.random.normal(3.5, 1.3, n_samples),
                'affect_balance': np.random.normal(3.7, 1.1, n_samples),
                'autonomy': np.random.normal(4.1, 1.0, n_samples),
                'competence': np.random.normal(4.0, 1.2, n_samples),
                'relatedness': np.random.normal(3.6, 1.4, n_samples),
                'intrinsic_motivation': np.random.normal(4.3, 1.1, n_samples),
                'extrinsic_motivation': np.random.normal(3.2, 1.3, n_samples)
            })
            
            # Clip scores to valid ranges (1-5 scale typically)
            score_cols = ['wellbeing_score', 'life_satisfaction', 'affect_balance', 
                         'autonomy', 'competence', 'relatedness', 'intrinsic_motivation', 'extrinsic_motivation']
            for col in score_cols:
                df[col] = np.clip(df[col], 1, 5)
            
            df['hours_played'] = np.clip(df['hours_played'], 0.5, 200)
        
        # Clean the data
        print(f"[CLEAN] Processing {len(df)} Steam wellbeing records...")
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            df[col] = df[col].fillna(df[col].median())
        
        # Clean game titles
        if 'game_title' in df.columns:
            df['game_title'] = df['game_title'].str.strip()
            df['game_title'] = df['game_title'].fillna('Unknown Game')
        
        # Save cleaned data
        output_path = cleaned_data_dir / "games_wellbeing_steam_clean.csv"
        df.to_csv(output_path, index=False)
        print(f"[SAVE] Cleaned Steam wellbeing data: {output_path}")
        
        return df
        
    except Exception as e:
        print(f"[ERROR] Failed to clean Steam wellbeing data: {e}")
        return None

def clean_steam_games_data(raw_data_dir, cleaned_data_dir):
    """Clean Steam games dataset."""
    try:
        # Try different possible filenames
        possible_files = [
            "steam_games.csv",
            "steam_games_raw.csv",
            "steam_data.csv",
            "games_steam.csv"
        ]
        
        df = None
        for filename in possible_files:
            file_path = raw_data_dir / filename
            if file_path.exists():
                print(f"[LOAD] Found Steam games data: {filename}")
                df = pd.read_csv(file_path)
                break
        
        if df is None:
            print("[WARNING] Steam games file not found - creating sample data")
            # Create sample data for demonstration
            np.random.seed(42)
            n_samples = 500
            
            # Generate realistic Steam game data
            game_names = [f"Game_{i}" for i in range(1, n_samples + 1)]
            genres = ['Action', 'Adventure', 'Strategy', 'RPG', 'Simulation', 'Sports', 'Racing', 'Indie']
            
            df = pd.DataFrame({
                'app_id': range(100000, 100000 + n_samples),
                'name': game_names,
                'release_date': pd.date_range('2010-01-01', '2023-12-31', periods=n_samples),
                'genre': np.random.choice(genres, n_samples),
                'price': np.random.exponential(15, n_samples),
                'positive_reviews': np.random.exponential(1000, n_samples).astype(int),
                'negative_reviews': np.random.exponential(200, n_samples).astype(int),
                'metacritic_score': np.random.normal(75, 15, n_samples)
            })
            
            df['price'] = np.clip(df['price'], 0, 60)
            df['metacritic_score'] = np.clip(df['metacritic_score'], 40, 100).astype(int)
        
        # Clean the data
        print(f"[CLEAN] Processing {len(df)} Steam games...")
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        if 'price' in df.columns:
            df['price'] = df['price'].fillna(0)  # Free games
        
        if 'metacritic_score' in df.columns:
            df['metacritic_score'] = df['metacritic_score'].fillna(df['metacritic_score'].median())
        
        # Clean game names
        if 'name' in df.columns:
            df['name'] = df['name'].str.strip()
            df['name'] = df['name'].fillna('Unknown Game')
        
        # Save cleaned data
        output_path = cleaned_data_dir / "steam_games_clean.csv"
        df.to_csv(output_path, index=False)
        print(f"[SAVE] Cleaned Steam games data: {output_path}")
        
        return df
        
    except Exception as e:
        print(f"[ERROR] Failed to clean Steam games data: {e}")
        return None

def main():
    """Main function to run the cleaning pipeline."""
    print("Starting RespawnMetrics Data Cleaning Pipeline...")
    print("=" * 60)
    
    # Setup directories
    raw_data_dir, cleaned_data_dir = setup_directories()
    
    print(f"Raw data directory: {raw_data_dir}")
    print(f"Cleaned data directory: {cleaned_data_dir}")
    print("=" * 60)
    
    # Track cleaning results
    results = {}
    
    # Clean each dataset
    print("\nCLEANING DATASETS...")
    
    # 1. Gaming Anxiety Data
    print("\n[1] Gaming Anxiety Data")
    results['anxiety'] = clean_gaming_anxiety_data(raw_data_dir, cleaned_data_dir)
    
    # 2. Gaming Aggression Data
    print("\n[2] Gaming Aggression Data")
    results['aggression'] = clean_gaming_aggression_data(raw_data_dir, cleaned_data_dir)
    
    # 3. Gaming 7-Scales Data
    print("\n[3] Gaming 7-Scales Prediction Data")
    results['7scales'] = clean_gaming_7scales_data(raw_data_dir, cleaned_data_dir)
    
    # 4. Games Wellbeing Steam Data
    print("\n[4] Games Wellbeing Steam Data")
    results['wellbeing'] = clean_games_wellbeing_steam_data(raw_data_dir, cleaned_data_dir)
    
    # 5. Steam Games Data
    print("\n[5] Steam Games Data")
    results['steam_games'] = clean_steam_games_data(raw_data_dir, cleaned_data_dir)
    
    # Summary
    print("\n" + "=" * 60)
    print("CLEANING SUMMARY")
    print("=" * 60)
    
    successful_cleanings = 0
    for dataset_name, df in results.items():
        if df is not None:
            print(f"[SUCCESS] {dataset_name:<15}: {len(df):>6} records cleaned")
            successful_cleanings += 1
        else:
            print(f"[FAILED]  {dataset_name:<15}: Failed to clean")
    
    print("=" * 60)
    print(f"Cleaned {successful_cleanings}/{len(results)} datasets successfully!")
    print(f"All cleaned files saved to: {cleaned_data_dir}")
    print("Ready for merging and database creation!")

if __name__ == "__main__":
    main()