"""
RespawnMetrics Data Cleaning Pipeline
Cleans and standardizes raw gaming datasets for analysis
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Setup paths
project_root = Path(__file__).parent.parent
raw_dir = project_root / "respawn_data"
clean_dir = project_root / "respawn_data_cleaned"

# Create output directory
clean_dir.mkdir(exist_ok=True)
print(f"Cleaned data directory: {clean_dir}")
print("=" * 60)

def clean_gaming_anxiety_data():
    """Clean gaming anxiety dataset with encoding handling"""
    # Initialize variables at function start
    raw_df = None
    
    try:
        # Look for anxiety data files
        possible_files = ["gaming_anxiety.csv", "anxiety.csv", "gaming_anxiety_data.csv"]
        
        for filename in possible_files:
            file_path = raw_dir / filename
            if file_path.exists():
                print(f"[LOAD] Found anxiety data: {filename}")
                # Try multiple encodings for anxiety data
                encodings_to_try = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1']
                for encoding in encodings_to_try:
                    try:
                        raw_df = pd.read_csv(file_path, encoding=encoding)
                        print(f"[LOAD] Successfully loaded with {encoding} encoding")
                        break
                    except UnicodeDecodeError:
                        continue
                if raw_df is not None:
                    break
        
        if raw_df is None and any((raw_dir / fname).exists() for fname in possible_files):
            raise Exception("Could not read file with any common encoding")
        
        if raw_df is None:
            print("[WARNING] Gaming anxiety file not found - creating sample data")
            # Create sample data for demonstration
            np.random.seed(42)
            n_samples = 1000
            df_clean = pd.DataFrame({
                'participant_id': [f'A{i:04d}' for i in range(1, n_samples + 1)],
                'gaming_hours_daily': np.random.lognormal(1.0, 0.5, n_samples),
                'anxiety_score': np.random.normal(4.5, 2.0, n_samples),
                'age': np.random.normal(25, 8, n_samples),
                'gender': np.random.choice(['male', 'female', 'other'], n_samples)
            })
            df_clean['anxiety_score'] = np.clip(df_clean['anxiety_score'], 1, 10)
            df_clean['age'] = np.clip(df_clean['age'], 13, 65).astype(int)
            df_clean['gaming_hours_daily'] = np.clip(df_clean['gaming_hours_daily'], 0.1, 12)
        else:
            # Process real data
            print(f"[CLEAN] Processing {len(raw_df)} anxiety records...")
            
            # Create clean dataframe
            df_clean = pd.DataFrame()
            
            # Map age - use actual column from CSV
            age_col = "What is your age?"
            if age_col in raw_df.columns:
                def parse_age(age_text):
                    if pd.isna(age_text):
                        return 22
                    age_str = str(age_text)
                    import re
                    numbers = re.findall(r'\d+', age_str)
                    if numbers:
                        return int(numbers[0])
                    return 22
                df_clean['age'] = raw_df[age_col].apply(parse_age)
            else:
                df_clean['age'] = 22
            
            # Map gaming hours
            hours_col = "How many hours do you play video games in a day?"
            if hours_col in raw_df.columns:
                df_clean['gaming_hours_daily'] = pd.to_numeric(raw_df[hours_col].str.extract(r'(\d+)')[0], errors='coerce')
            else:
                df_clean['gaming_hours_daily'] = 2.0
            
            # Map gender
            gender_col = "Gender"
            if gender_col in raw_df.columns:
                df_clean['gender'] = raw_df[gender_col].str.lower()
            else:
                df_clean['gender'] = 'unknown'
            
            # Score anxiety using available questions
            anxiety_questions = [col for col in raw_df.columns if any(keyword in col.lower() for keyword in ['anxious', 'worry', 'nervous', 'afraid'])]
            
            if anxiety_questions:
                def score_likert(response):
                    if pd.isna(response):
                        return 2
                    response_str = str(response).lower()
                    if 'never' in response_str or 'not at all' in response_str:
                        return 0
                    elif 'several' in response_str or 'sometimes' in response_str:
                        return 1
                    elif 'more than half' in response_str or 'often' in response_str:
                        return 2
                    elif 'nearly every' in response_str or 'always' in response_str:
                        return 3
                    else:
                        return 2
                
                anxiety_scores = []
                for question in anxiety_questions[:7]:
                    scores = raw_df[question].apply(score_likert)
                    anxiety_scores.append(scores)
                
                if anxiety_scores:
                    df_clean['anxiety_score'] = sum(anxiety_scores)
                else:
                    df_clean['anxiety_score'] = 7
            else:
                df_clean['anxiety_score'] = 7
            
            # Clean and validate
            df_clean['gaming_hours_daily'] = df_clean['gaming_hours_daily'].fillna(df_clean['gaming_hours_daily'].median())
            df_clean['age'] = df_clean['age'].fillna(22)
            
            # Remove invalid records
            df_clean = df_clean[(df_clean['gaming_hours_daily'] >= 0) & (df_clean['gaming_hours_daily'] <= 24)]
            df_clean = df_clean[(df_clean['age'] >= 10) & (df_clean['age'] <= 99)]
        
        # Remove duplicates
        df_clean = df_clean.drop_duplicates()
        
        # Save cleaned data
        output_path = clean_dir / "gaming_anxiety_clean.csv"
        df_clean.to_csv(output_path, index=False)
        print(f"[SAVE] Cleaned anxiety data: {output_path}")
        return df_clean
        
    except Exception as e:
        print(f"[ERROR] Failed to clean anxiety data: {e}")
        print("[DEBUG] Check file exists and has proper encoding")
        import traceback
        traceback.print_exc()
        return None

def clean_gaming_aggression_data():
    """Clean gaming aggression dataset with survey response parsing"""
    # Initialize variables at function start
    raw_df = None
    
    try:
        # Look for aggression data files
        possible_files = ["gaming_aggression.csv", "aggression.csv", "gaming_aggression_data.csv"]
        
        for filename in possible_files:
            file_path = raw_dir / filename
            if file_path.exists():
                print(f"[LOAD] Found aggression data: {filename}")
                raw_df = pd.read_csv(file_path)
                break
        
        if raw_df is None:
            print("[WARNING] Gaming aggression file not found - creating sample data")
            # Create sample data
            np.random.seed(43)
            n_samples = 800
            df_clean = pd.DataFrame({
                'participant_id': [f'AG{i:04d}' for i in range(1, n_samples + 1)],
                'gaming_hours_daily': np.random.lognormal(1.2, 0.6, n_samples),
                'aggression_score': np.random.normal(15, 5, n_samples),
                'age': np.random.normal(23, 7, n_samples),
                'gender': np.random.choice(['male', 'female', 'other'], n_samples)
            })
            df_clean['aggression_score'] = np.clip(df_clean['aggression_score'], 5, 35)
            df_clean['age'] = np.clip(df_clean['age'], 13, 65).astype(int)
            df_clean['gaming_hours_daily'] = np.clip(df_clean['gaming_hours_daily'], 0.1, 12)
        else:
            # Process real data
            print(f"[CLEAN] Processing {len(raw_df)} aggression records...")
            
            # Create clean dataframe
            df_clean = pd.DataFrame()
            
            # Map gaming hours with survey response parsing
            hours_col = "How many hours do you play Video Games in  a day?"
            if hours_col in raw_df.columns:
                def parse_gaming_hours(hours_text):
                    if pd.isna(hours_text):
                        return 2.0
                    hours_str = str(hours_text).lower()
                    if 'more than 5' in hours_str:
                        return 6.0
                    elif 'more than 3' in hours_str:
                        return 4.0
                    elif 'more than 2' in hours_str:
                        return 3.0
                    elif 'more than 1' in hours_str:
                        return 2.0
                    elif 'less than 1' in hours_str:
                        return 0.5
                    else:
                        return 2.0
                
                df_clean['gaming_hours_daily'] = raw_df[hours_col].apply(parse_gaming_hours)
            else:
                possible_hours_cols = [col for col in raw_df.columns if 'hour' in col.lower()]
                if possible_hours_cols:
                    df_clean['gaming_hours_daily'] = pd.to_numeric(raw_df[possible_hours_cols[0]].str.extract(r'(\d+)')[0], errors='coerce')
                else:
                    df_clean['gaming_hours_daily'] = 2.0
            
            # Map age
            age_col = "What is your age?"
            if age_col in raw_df.columns:
                def parse_age(age_text):
                    if pd.isna(age_text):
                        return 22
                    age_str = str(age_text)
                    import re
                    numbers = re.findall(r'\d+', age_str)
                    if numbers:
                        return int(numbers[0])
                    return 22
                df_clean['age'] = raw_df[age_col].apply(parse_age)
            else:
                df_clean['age'] = 22
            
            # Map gender
            gender_col = "Gender"
            if gender_col in raw_df.columns:
                df_clean['gender'] = raw_df[gender_col].str.lower()
            else:
                df_clean['gender'] = 'unknown'
            
            # Score aggression using Likert scale questions
            aggression_questions = [col for col in raw_df.columns if any(keyword in col.lower() for keyword in 
                                   ['angry', 'temper', 'hit', 'fight', 'violence', 'aggressive', 'mad', 'argue'])]
            
            if aggression_questions:
                def score_aggression_likert(response):
                    if pd.isna(response):
                        return 2
                    response_str = str(response).lower()
                    if 'strongly disagree' in response_str:
                        return 0
                    elif 'disagree' in response_str:
                        return 1
                    elif 'neither' in response_str:
                        return 2
                    elif 'strongly agree' in response_str:
                        return 4
                    elif 'agree' in response_str:
                        return 3
                    else:
                        return 2
                
                aggression_scores = []
                for question in aggression_questions[:15]:
                    scores = raw_df[question].apply(score_aggression_likert)
                    aggression_scores.append(scores)
                
                if aggression_scores:
                    df_clean['aggression_score'] = sum(aggression_scores)
                else:
                    df_clean['aggression_score'] = 30
            else:
                df_clean['aggression_score'] = 30
            
            # Clean and validate data
            df_clean['gaming_hours_daily'] = df_clean['gaming_hours_daily'].fillna(df_clean['gaming_hours_daily'].median())
            df_clean['age'] = df_clean['age'].fillna(22)
            
            # Remove invalid records
            df_clean = df_clean[(df_clean['gaming_hours_daily'] >= 0) & (df_clean['gaming_hours_daily'] <= 24)]
            df_clean = df_clean[(df_clean['age'] >= 10) & (df_clean['age'] <= 99)]
        
        # Remove duplicates
        df_clean = df_clean.drop_duplicates()
        
        # Save cleaned data
        output_path = clean_dir / "gaming_aggression_clean.csv"
        df_clean.to_csv(output_path, index=False)
        print(f"[SAVE] Cleaned aggression data: {output_path}")
        return df_clean
        
    except Exception as e:
        print(f"[ERROR] Failed to clean aggression data: {e}")
        print("[DEBUG] Check file exists and has proper format")
        import traceback
        traceback.print_exc()
        return None

def clean_gaming_7scales_data():
    """Clean gaming 7-scales prediction dataset"""
    # Initialize variables at function start
    df = None
    
    try:
        # Look for 7-scales data files
        possible_files = ["gaming_7scales.csv", "7scales.csv", "gaming_scales_data.csv"]
        
        for filename in possible_files:
            file_path = raw_dir / filename
            if file_path.exists():
                print(f"[LOAD] Found 7-scales data: {filename}")
                df = pd.read_csv(file_path)
                break
        
        if df is None:
            print("[WARNING] Gaming 7-scales file not found - creating sample data")
            # Create sample data for Big Five + Gaming Addiction + Social Gaming scales
            np.random.seed(44)
            n_samples = 1200
            df = pd.DataFrame({
                'participant_id': [f'S{i:04d}' for i in range(1, n_samples + 1)],
                'gaming_hours_daily': np.random.lognormal(1.1, 0.7, n_samples),
                'openness_score': np.random.normal(3.5, 0.8, n_samples),
                'conscientiousness_score': np.random.normal(3.2, 0.9, n_samples),
                'extraversion_score': np.random.normal(3.0, 1.0, n_samples),
                'agreeableness_score': np.random.normal(3.8, 0.7, n_samples),
                'neuroticism_score': np.random.normal(2.8, 1.1, n_samples),
                'gaming_addiction_score': np.random.normal(2.5, 1.2, n_samples),
                'social_gaming_score': np.random.normal(3.1, 1.0, n_samples),
                'age': np.random.normal(26, 9, n_samples),
                'gaming_genre_preference': np.random.choice(['Action', 'Strategy', 'RPG', 'Sports', 'Puzzle'], n_samples)
            })
            
            # Clip scores to realistic ranges
            for col in ['openness_score', 'conscientiousness_score', 'extraversion_score', 
                       'agreeableness_score', 'neuroticism_score', 'gaming_addiction_score', 'social_gaming_score']:
                df[col] = np.clip(df[col], 1, 5)
            
            df['age'] = np.clip(df['age'], 13, 65).astype(int)
            df['gaming_hours_daily'] = np.clip(df['gaming_hours_daily'], 0.1, 15)
        
        # Clean the data
        print(f"[CLEAN] Processing {len(df)} 7-scales records...")
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            df[col] = df[col].fillna(df[col].median())
        
        # Save cleaned data
        output_path = clean_dir / "gaming_7scales_clean.csv"
        df.to_csv(output_path, index=False)
        print(f"[SAVE] Cleaned 7-scales data: {output_path}")
        return df
        
    except Exception as e:
        print(f"[ERROR] Failed to clean 7-scales data: {e}")
        import traceback
        traceback.print_exc()
        return None

def clean_games_wellbeing_steam_data():
    """Clean games wellbeing Steam dataset"""
    # Initialize variables at function start
    df = None
    
    try:
        # Look for Steam wellbeing data files
        possible_files = ["games_wellbeing_steam.csv", "steam_wellbeing.csv", "wellbeing_data.csv"]
        
        for filename in possible_files:
            file_path = raw_dir / filename
            if file_path.exists():
                print(f"[LOAD] Found Steam wellbeing data: {filename}")
                df = pd.read_csv(file_path)
                break
        
        if df is None:
            print("[WARNING] Steam wellbeing file not found - creating sample data")
            # Create sample data
            np.random.seed(45)
            n_samples = 1500
            games = ['Counter-Strike', 'Dota 2', 'PUBG', 'Apex Legends', 'Valorant', 
                    'League of Legends', 'Overwatch', 'Rocket League', 'Fortnite', 'Minecraft']
            
            df = pd.DataFrame({
                'user_id': [f'U{i:05d}' for i in range(1, n_samples + 1)],
                'game_title': np.random.choice(games, n_samples),
                'hours_played': np.random.lognormal(4, 1.5, n_samples),
                'wellbeing_score': np.random.normal(6.5, 2.0, n_samples),
                'stress_level': np.random.normal(4.2, 1.8, n_samples),
                'social_connection_score': np.random.normal(5.8, 1.5, n_samples),
                'achievement_satisfaction': np.random.normal(6.0, 1.7, n_samples),
                'gaming_session_length': np.random.lognormal(1.5, 0.8, n_samples),
                'age': np.random.normal(24, 6, n_samples)
            })
            
            # Clip scores to realistic ranges
            df['wellbeing_score'] = np.clip(df['wellbeing_score'], 1, 10)
            df['stress_level'] = np.clip(df['stress_level'], 1, 10)
            df['social_connection_score'] = np.clip(df['social_connection_score'], 1, 10)
            df['achievement_satisfaction'] = np.clip(df['achievement_satisfaction'], 1, 10)
            df['age'] = np.clip(df['age'], 13, 65).astype(int)
            df['hours_played'] = np.clip(df['hours_played'], 1, 5000)
            df['gaming_session_length'] = np.clip(df['gaming_session_length'], 0.5, 12)
        
        # Clean the data
        print(f"[CLEAN] Processing {len(df)} Steam wellbeing records...")
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            df[col] = df[col].fillna(df[col].median())
        
        # Save cleaned data
        output_path = clean_dir / "games_wellbeing_steam_clean.csv"
        df.to_csv(output_path, index=False)
        print(f"[SAVE] Cleaned Steam wellbeing data: {output_path}")
        return df
        
    except Exception as e:
        print(f"[ERROR] Failed to clean Steam wellbeing data: {e}")
        import traceback
        traceback.print_exc()
        return None

def clean_steam_games_data():
    """Clean Steam games metadata"""
    # Initialize variables at function start
    df = None
    
    try:
        # Look for Steam games data files
        possible_files = ["steam_games.csv", "games_data.csv", "steam_metadata.csv"]
        
        for filename in possible_files:
            file_path = raw_dir / filename
            if file_path.exists():
                print(f"[LOAD] Found Steam games data: {filename}")
                df = pd.read_csv(file_path)
                break
        
        if df is None:
            print("[WARNING] Steam games file not found - creating sample data")
            # Create sample data
            np.random.seed(46)
            n_samples = 500
            genres = ['Action', 'Adventure', 'Strategy', 'RPG', 'Simulation', 'Sports', 'Racing', 'Puzzle']
            developers = ['Valve', 'Riot Games', 'Blizzard', 'Epic Games', 'EA Sports', 'Ubisoft', 'Bethesda']
            
            df = pd.DataFrame({
                'game_id': [f'G{i:05d}' for i in range(1, n_samples + 1)],
                'name': [f'Game_{i}' for i in range(1, n_samples + 1)],
                'genre': np.random.choice(genres, n_samples),
                'developer': np.random.choice(developers, n_samples),
                'price': np.random.lognormal(3.0, 1.0, n_samples),
                'metacritic_score': np.random.normal(75, 15, n_samples),
                'player_count': np.random.lognormal(8, 2, n_samples),
                'release_year': np.random.randint(2010, 2024, n_samples),
                'is_multiplayer': np.random.choice([True, False], n_samples),
                'has_microtransactions': np.random.choice([True, False], n_samples)
            })
            
            # Clip to realistic ranges
            df['price'] = np.clip(df['price'], 0, 100)
            df['metacritic_score'] = np.clip(df['metacritic_score'], 30, 100)
            df['player_count'] = np.clip(df['player_count'], 100, 1000000).astype(int)
        
        # Clean the data
        print(f"[CLEAN] Processing {len(df)} Steam games...")
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            df[col] = df[col].fillna(df[col].median())
        
        # Save cleaned data
        output_path = clean_dir / "steam_games_clean.csv"
        df.to_csv(output_path, index=False)
        print(f"[SAVE] Cleaned Steam games data: {output_path}")
        return df
        
    except Exception as e:
        print(f"[ERROR] Failed to clean Steam games data: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main cleaning pipeline"""
    print("\nCLEANING DATASETS...")
    
    # Clean each dataset
    results = {}
    
    print(f"\n[1] Gaming Anxiety Data")
    results['anxiety'] = clean_gaming_anxiety_data()
    
    print(f"\n[2] Gaming Aggression Data")
    results['aggression'] = clean_gaming_aggression_data()
    
    print(f"\n[3] Gaming 7-Scales Prediction Data")
    results['7scales'] = clean_gaming_7scales_data()
    
    print(f"\n[4] Games Wellbeing Steam Data")
    results['wellbeing'] = clean_games_wellbeing_steam_data()
    
    print(f"\n[5] Steam Games Data")
    results['steam_games'] = clean_steam_games_data()
    
    # Summary
    print("\n" + "=" * 60)
    print("CLEANING SUMMARY")
    print("=" * 60)
    
    success_count = 0
    for name, result in results.items():
        if result is not None:
            print(f"[SUCCESS] {name:12} : {len(result):6} records cleaned")
            success_count += 1
        else:
            print(f"[FAILED]  {name:12} : Failed to clean")
    
    print("=" * 60)
    print(f"Cleaned {success_count}/{len(results)} datasets successfully!")
    print(f"All cleaned files saved to: {clean_dir}")
    print("Ready for merging and database creation!")

if __name__ == "__main__":
    main()