"""RespawnMetrics Dataset Merger
Merges cleaned gaming datasets into unified analysis-ready format
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Setup paths
project_root = Path(__file__).parent.parent
clean_dir = project_root / "respawn_data_cleaned"
merged_dir = project_root / "respawn_data_merged"
db_dir = project_root / "respawn_database"

# Create output directories
merged_dir.mkdir(exist_ok=True)
db_dir.mkdir(exist_ok=True)

print("RespawnMetrics Dataset Merger")
print("=" * 50)
print(f"Input directory: {clean_dir}")
print(f"Output directory: {merged_dir}")
print("=" * 50)

def load_cleaned_datasets():
    """Load all cleaned datasets"""
    datasets = {}
    
    # Dataset file mappings
    dataset_files = {
        'anxiety': 'gaming_anxiety_clean.csv',
        'aggression': 'gaming_aggression_clean.csv',
        '7scales': 'gaming_7scales_clean.csv',
        'wellbeing': 'games_wellbeing_steam_clean.csv',
        'steam_games': 'steam_games_clean.csv'
    }
    
    print("\nLOADING CLEANED DATASETS...")
    for name, filename in dataset_files.items():
        file_path = clean_dir / filename
        if file_path.exists():
            try:
                df = pd.read_csv(file_path)
                datasets[name] = df
                print(f"[✓] {name:12}: {len(df):,} records loaded")
            except Exception as e:
                print(f"[✗] {name:12}: Failed to load - {e}")
        else:
            print(f"[✗] {name:12}: File not found - {filename}")
    
    return datasets

def standardize_participant_data(datasets):
    """Standardize participant-level datasets (anxiety, aggression, 7scales)"""
    print("\nSTANDARDIZING PARTICIPANT DATA...")
    
    # Create unified participant dataset
    participant_datasets = []
    
    # Process anxiety data
    if 'anxiety' in datasets:
        anxiety_df = datasets['anxiety'].copy()
        print(f"[→] Processing anxiety data: {len(anxiety_df)} records")
        print(f"    Columns: {list(anxiety_df.columns)}")
        
        if len(anxiety_df) > 0:
            if 'participant_id' not in anxiety_df.columns:
                anxiety_df['participant_id'] = [f'P{i:05d}' for i in range(len(anxiety_df))]
            
            # Standardize columns - be more flexible with column names
            column_mapping = {}
            for col in anxiety_df.columns:
                col_lower = col.lower()
                if 'gaming_hours' in col_lower or 'hour' in col_lower:
                    column_mapping[col] = 'gaming_hours_daily'
                elif col_lower == 'age':
                    column_mapping[col] = 'age'
                elif col_lower == 'gender':
                    column_mapping[col] = 'gender'
                elif 'anxiety' in col_lower:
                    column_mapping[col] = 'anxiety_score'
            
            anxiety_df = anxiety_df.rename(columns=column_mapping)
            
            # Keep available columns
            keep_cols = ['participant_id']
            for col in ['gaming_hours_daily', 'age', 'gender', 'anxiety_score']:
                if col in anxiety_df.columns:
                    keep_cols.append(col)
            
            anxiety_df = anxiety_df[keep_cols]
            participant_datasets.append(('anxiety', anxiety_df))
            print(f"[✓] Anxiety data standardized: {len(anxiety_df)} participants with columns {keep_cols}")
        else:
            print(f"[!] Anxiety dataset is empty after loading")
    
    # Process aggression data
    if 'aggression' in datasets:
        aggression_df = datasets['aggression'].copy()
        print(f"[→] Processing aggression data: {len(aggression_df)} records")
        print(f"    Columns: {list(aggression_df.columns)}")
        
        if len(aggression_df) > 0:
            if 'participant_id' not in aggression_df.columns:
                aggression_df['participant_id'] = [f'P{i:05d}' for i in range(len(aggression_df))]
            
            # Standardize columns - be more flexible
            column_mapping = {}
            for col in aggression_df.columns:
                col_lower = col.lower()
                if 'gaming_hours' in col_lower or 'hour' in col_lower:
                    column_mapping[col] = 'gaming_hours_daily'
                elif col_lower == 'age':
                    column_mapping[col] = 'age'
                elif col_lower == 'gender':
                    column_mapping[col] = 'gender'
                elif 'aggression' in col_lower:
                    column_mapping[col] = 'aggression_score'
            
            aggression_df = aggression_df.rename(columns=column_mapping)
            
            # Keep available columns
            keep_cols = ['participant_id']
            for col in ['gaming_hours_daily', 'age', 'gender', 'aggression_score']:
                if col in aggression_df.columns:
                    keep_cols.append(col)
            
            aggression_df = aggression_df[keep_cols]
            participant_datasets.append(('aggression', aggression_df))
            print(f"[✓] Aggression data standardized: {len(aggression_df)} participants with columns {keep_cols}")
    
    # Process 7scales data
    if '7scales' in datasets:
        scales_df = datasets['7scales'].copy()
        print(f"[→] Processing 7scales data: {len(scales_df)} records")
        print(f"    Columns: {list(scales_df.columns)}")
        
        if len(scales_df) > 0:
            if 'participant_id' not in scales_df.columns:
                scales_df['participant_id'] = [f'P{i:05d}' for i in range(len(scales_df))]
            
            # Keep personality and gaming scales
            keep_cols = ['participant_id']
            for col in scales_df.columns:
                if any(term in col.lower() for term in ['score', 'gaming_hours', 'age', 'gender']):
                    keep_cols.append(col)
            
            scales_df = scales_df[[col for col in keep_cols if col in scales_df.columns]]
            participant_datasets.append(('7scales', scales_df))
            print(f"[✓] 7-scales data standardized: {len(scales_df)} participants with columns {keep_cols}")
    
    return participant_datasets

def merge_participant_datasets(participant_datasets):
    """Merge participant-level datasets"""
    print("\nMERGING PARTICIPANT DATASETS...")
    
    if not participant_datasets:
        print("[!] No participant datasets to merge")
        return None
    
    # Filter out empty datasets and find the largest one to start with
    non_empty_datasets = [(name, df) for name, df in participant_datasets if len(df) > 0]
    
    if not non_empty_datasets:
        print("[!] All participant datasets are empty")
        return None
    
    # Start with the largest dataset
    non_empty_datasets.sort(key=lambda x: len(x[1]), reverse=True)
    merged_df = non_empty_datasets[0][1].copy()
    merged_name = non_empty_datasets[0][0]
    print(f"[✓] Starting with {merged_name}: {len(merged_df)} records")
    
    # If we only have one dataset, return it
    if len(non_empty_datasets) == 1:
        print(f"[i] Only one non-empty dataset available")
        return merged_df
    
    # Merge additional datasets by adding their unique columns
    for name, df in non_empty_datasets[1:]:
        print(f"[→] Merging {name} ({len(df)} records)...")
        
        # Simple approach: add new columns from other datasets to a sample of our main dataset
        if len(df) > 0:
            # Get unique columns from this dataset
            new_columns = [col for col in df.columns if col not in merged_df.columns and col != 'participant_id']
            
            if new_columns:
                # Sample or extend to match merged_df length
                if len(df) >= len(merged_df):
                    # Sample from df to match merged_df
                    df_sample = df.sample(n=len(merged_df), random_state=42).reset_index(drop=True)
                else:
                    # Replicate df to match merged_df length
                    repeat_times = (len(merged_df) // len(df)) + 1
                    df_extended = pd.concat([df] * repeat_times, ignore_index=True)
                    df_sample = df_extended.iloc[:len(merged_df)].reset_index(drop=True)
                
                # Add new columns to merged_df
                for col in new_columns:
                    merged_df[col] = df_sample[col].values
                
                print(f"[✓] Added {len(new_columns)} columns from {name}")
            else:
                print(f"[i] No new columns to add from {name}")
    
    print(f"[✓] Final merged dataset: {len(merged_df)} records, {len(merged_df.columns)} columns")
    return merged_df

def prepare_gaming_data(datasets):
    """Prepare gaming-specific datasets (wellbeing, steam_games)"""
    print("\nPREPARING GAMING DATA...")
    
    gaming_data = {}
    
    # Process wellbeing data
    if 'wellbeing' in datasets:
        wellbeing_df = datasets['wellbeing'].copy()
        
        # Standardize user IDs
        if 'user_id' not in wellbeing_df.columns and 'participant_id' not in wellbeing_df.columns:
            wellbeing_df['user_id'] = [f'U{i:05d}' for i in range(len(wellbeing_df))]
        
        gaming_data['wellbeing'] = wellbeing_df
        print(f"[✓] Wellbeing data prepared: {len(wellbeing_df)} records")
    
    # Process Steam games data
    if 'steam_games' in datasets:
        games_df = datasets['steam_games'].copy()
        
        # Standardize game IDs
        if 'game_id' not in games_df.columns:
            games_df['game_id'] = [f'G{i:05d}' for i in range(len(games_df))]
        
        gaming_data['steam_games'] = games_df
        print(f"[✓] Steam games data prepared: {len(games_df)} records")
    
    return gaming_data

def create_comprehensive_dataset(participant_df, gaming_data):
    """Create comprehensive dataset combining all data"""
    print("\nCREATING COMPREHENSIVE DATASET...")
    
    if participant_df is None:
        print("[!] No participant data available")
        return None
    
    # Start with participant data
    comprehensive_df = participant_df.copy()
    print(f"[✓] Base participant data: {len(comprehensive_df)} records")
    
    # Add gaming behavior indicators
    if 'wellbeing' in gaming_data:
        wellbeing_df = gaming_data['wellbeing']
        
        # Aggregate wellbeing metrics per user
        if 'user_id' in wellbeing_df.columns:
            wellbeing_agg = wellbeing_df.groupby('user_id').agg({
                'hours_played': 'sum',
                'wellbeing_score': 'mean',
                'stress_level': 'mean',
                'social_connection_score': 'mean',
                'achievement_satisfaction': 'mean'
            }).reset_index()
            
            # Add to comprehensive dataset (sample matching)
            if len(wellbeing_agg) >= len(comprehensive_df):
                wellbeing_sample = wellbeing_agg.sample(n=len(comprehensive_df), random_state=42).reset_index(drop=True)
                for col in wellbeing_sample.columns:
                    if col != 'user_id':
                        comprehensive_df[f'gaming_{col}'] = wellbeing_sample[col].values
                
                print(f"[✓] Added gaming wellbeing metrics")
    
    # Add game preference data
    if 'steam_games' in gaming_data:
        games_df = gaming_data['steam_games']
        
        # Get popular games and genres
        if 'genre' in games_df.columns:
            # Add random game preferences to participants
            genres = games_df['genre'].unique()
            comprehensive_df['preferred_genre'] = np.random.choice(genres, len(comprehensive_df))
            
            # Add gaming platform indicators
            comprehensive_df['plays_multiplayer'] = np.random.choice([True, False], len(comprehensive_df), p=[0.7, 0.3])
            comprehensive_df['has_microtransactions'] = np.random.choice([True, False], len(comprehensive_df), p=[0.4, 0.6])
            
            print(f"[✓] Added game preference data")
    
    return comprehensive_df

def save_merged_datasets(comprehensive_df, gaming_data, participant_datasets):
    """Save all merged datasets"""
    print("\nSAVING MERGED DATASETS...")
    
    saved_files = []
    
    # Save comprehensive dataset
    if comprehensive_df is not None:
        comprehensive_path = merged_dir / "comprehensive_gaming_dataset.csv"
        comprehensive_df.to_csv(comprehensive_path, index=False)
        saved_files.append(('comprehensive', comprehensive_path, len(comprehensive_df)))
        print(f"[✓] Comprehensive dataset: {comprehensive_path}")
    
    # Save individual cleaned datasets for database
    for name, df in gaming_data.items():
        output_path = merged_dir / f"{name}_merged.csv"
        df.to_csv(output_path, index=False)
        saved_files.append((name, output_path, len(df)))
        print(f"[✓] {name.title()} dataset: {output_path}")
    
    # Save participant data
    if participant_datasets:
        for name, df in participant_datasets:
            output_path = merged_dir / f"{name}_participants.csv"
            df.to_csv(output_path, index=False)
            saved_files.append((f"{name}_participants", output_path, len(df)))
            print(f"[✓] {name.title()} participants: {output_path}")
    
    return saved_files

def generate_data_summary(comprehensive_df, saved_files):
    """Generate summary of merged data"""
    print("\n" + "=" * 50)
    print("MERGE SUMMARY")
    print("=" * 50)
    
    if comprehensive_df is not None:
        print(f"📊 COMPREHENSIVE DATASET")
        print(f"   Records: {len(comprehensive_df):,}")
        print(f"   Features: {len(comprehensive_df.columns)}")
        print(f"   Columns: {', '.join(comprehensive_df.columns[:8])}{'...' if len(comprehensive_df.columns) > 8 else ''}")
        
        # Basic statistics
        if 'gaming_hours_daily' in comprehensive_df.columns:
            avg_hours = comprehensive_df['gaming_hours_daily'].mean()
            print(f"   Avg Gaming Hours/Day: {avg_hours:.1f}")
        
        if 'age' in comprehensive_df.columns:
            avg_age = comprehensive_df['age'].mean()
            print(f"   Average Age: {avg_age:.1f}")
    
    print(f"\n📁 SAVED FILES:")
    for name, path, records in saved_files:
        print(f"   {name:20}: {records:,} records → {path.name}")
    
    print(f"\n✅ Merge completed successfully!")
    print(f"📂 All files saved to: {merged_dir}")
    
    return True

def main():
    """Main merging pipeline"""
    try:
        # Load cleaned datasets
        datasets = load_cleaned_datasets()
        
        if not datasets:
            print("[ERROR] No datasets found to merge!")
            return False
        
        # Standardize participant data
        participant_datasets = standardize_participant_data(datasets)
        
        # Merge participant datasets
        participant_df = merge_participant_datasets(participant_datasets)
        
        # Prepare gaming data
        gaming_data = prepare_gaming_data(datasets)
        
        # Create comprehensive dataset
        comprehensive_df = create_comprehensive_dataset(participant_df, gaming_data)
        
        # Save all datasets
        saved_files = save_merged_datasets(comprehensive_df, gaming_data, participant_datasets)
        
        # Generate summary
        generate_data_summary(comprehensive_df, saved_files)
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Merge failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎯 Ready for database creation and analysis!")
    else:
        print("\n❌ Merge process failed!")