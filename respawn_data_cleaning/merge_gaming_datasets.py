"""
RespawnMetrics Dataset Merge Pipeline

This script merges cleaned gaming mental health datasets into unified analysis datasets.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def setup_directories():
    """Setup directory paths for merge process."""
    base_dir = Path(__file__).parent.parent
    cleaned_data_dir = base_dir / "respawn_data_cleaned"
    merged_data_dir = base_dir / "respawn_data_merged"
    
    # Create merged data directory
    merged_data_dir.mkdir(exist_ok=True)
    
    return cleaned_data_dir, merged_data_dir

def load_cleaned_datasets(cleaned_data_dir):
    """Load all cleaned datasets from CSV files."""
    print("[LOAD] Loading cleaned datasets...")
    
    datasets = {}
    
    # Define expected cleaned files - FIXED to match cleaning script output
    expected_files = {
        'anxiety': 'gaming_anxiety_clean.csv',
        'aggression': 'gaming_aggression_clean.csv',
        'wellbeing': 'games_wellbeing_steam_clean.csv',  # Fixed filename
        'prediction_scales': 'gaming_7scales_clean.csv',  # Fixed filename
        'steam_games': 'steam_games_clean.csv'  # Added steam games
    }
    
    for dataset_name, filename in expected_files.items():
        file_path = cleaned_data_dir / filename
        if file_path.exists():
            try:
                df = pd.read_csv(file_path)
                datasets[dataset_name] = df
                print(f"[OK] {dataset_name}: {len(df):,} records loaded")
            except Exception as e:
                print(f"[ERROR] Failed to load {dataset_name}: {e}")
        else:
            print(f"[WARNING] File not found: {filename}")
    
    return datasets

def standardize_column_names(datasets):
    """Standardize column names across all datasets."""
    print("[PROCESS] Standardizing column names...")
    
    # Define column mapping for standardization
    column_mapping = {
        # Gaming hours variations
        'gaming_hours_weekly': 'gaming_hours_weekly',
        'gaming_hours_per_week': 'gaming_hours_weekly',
        'weekly_gaming_hours': 'gaming_hours_weekly',
        'hours_per_week': 'gaming_hours_weekly',
        'gaming_hours_daily': 'gaming_hours_daily',
        'hours_played': 'hours_played',
        
        # Participant ID variations
        'participant_id': 'participant_id',
        'id': 'participant_id',
        'user_id': 'participant_id',
        'subject_id': 'participant_id',
        
        # Age variations
        'age': 'age',
        'participant_age': 'age',
        'user_age': 'age',
        
        # Gaming preference variations
        'gaming_preference': 'gaming_preference',
        'game_preference': 'gaming_preference',
        'preferred_genre': 'gaming_preference',
        'favorite_genre': 'gaming_preference',
        'game_type_preference': 'gaming_preference'
    }
    
    for dataset_name, df in datasets.items():
        print(f"[STANDARDIZE] Processing {dataset_name}...")
        
        # Create a copy to avoid modifying original
        df_standardized = df.copy()
        
        # Apply column name standardization
        for old_name, new_name in column_mapping.items():
            if old_name in df_standardized.columns and old_name != new_name:
                df_standardized = df_standardized.rename(columns={old_name: new_name})
                print(f"  [RENAME] {old_name} -> {new_name}")
        
        datasets[dataset_name] = df_standardized
    
    return datasets

def create_master_dataset(datasets):
    """Create a master dataset by combining all datasets with common columns."""
    print("[MERGE] Creating master dataset with common columns...")
    
    # Find common columns across all datasets
    if not datasets:
        print("[ERROR] No datasets available for merging")
        return None
    
    # Get all column names from all datasets
    all_columns = set()
    for df in datasets.values():
        all_columns.update(df.columns)
    
    # Find columns that exist in most datasets
    column_counts = {}
    for col in all_columns:
        count = sum(1 for df in datasets.values() if col in df.columns)
        column_counts[col] = count
    
    # Select common core columns - updated based on actual data
    core_columns = ['participant_id', 'age']
    
    # Add gaming hours columns if they exist
    gaming_hours_cols = ['gaming_hours_weekly', 'gaming_hours_daily', 'hours_played']
    for col in gaming_hours_cols:
        if any(col in df.columns for df in datasets.values()):
            core_columns.append(col)
            break  # Only add one gaming hours column
    
    # Add gaming preference if it exists
    if any('gaming_preference' in df.columns for df in datasets.values()):
        core_columns.append('gaming_preference')
    
    # Add columns that appear in at least 2 datasets
    additional_columns = [col for col, count in column_counts.items() 
                         if count >= 2 and col not in core_columns]
    
    selected_columns = core_columns + additional_columns
    print(f"[COLUMNS] Selected columns for master dataset: {selected_columns}")
    
    # Combine datasets
    combined_dfs = []
    
    for dataset_name, df in datasets.items():
        print(f"[PROCESS] Adding {dataset_name} to master dataset...")
        
        # Select only available columns
        available_columns = [col for col in selected_columns if col in df.columns]
        if len(available_columns) < 2:  # Skip if too few columns
            print(f"  [SKIP] {dataset_name}: too few matching columns")
            continue
            
        df_subset = df[available_columns].copy()
        
        # Add source indicator
        df_subset['data_source'] = dataset_name
        
        combined_dfs.append(df_subset)
        print(f"  [ADD] {len(df_subset)} records with {len(available_columns)} columns")
    
    # Concatenate all datasets
    if combined_dfs:
        master_df = pd.concat(combined_dfs, ignore_index=True, sort=False)
        print(f"[COMBINED] Master dataset created: {len(master_df):,} total records")
        
        # Remove duplicates based on participant_id if it exists
        if 'participant_id' in master_df.columns:
            original_len = len(master_df)
            master_df = master_df.drop_duplicates(subset=['participant_id'], keep='first')
            if len(master_df) < original_len:
                print(f"[DEDUPE] Removed {original_len - len(master_df)} duplicate participants")
        
        return master_df
    else:
        print("[ERROR] No datasets could be combined")
        return None

def create_analysis_specific_datasets(datasets, merged_data_dir):
    """Create specialized datasets for different types of analysis."""
    print("[SPECIALIZE] Creating analysis-specific datasets...")
    
    specialized_datasets = {}
    
    # 1. Mental Health Focus Dataset
    print("[CREATE] Mental health focused dataset...")
    mental_health_dfs = []
    
    for dataset_name, df in datasets.items():
        if any(col in df.columns for col in ['anxiety_score', 'wellbeing_score', 'aggression_score']):
            # Select mental health relevant columns
            mental_health_cols = ['participant_id', 'age']
            
            # Add gaming hours if available
            for hours_col in ['gaming_hours_weekly', 'gaming_hours_daily', 'hours_played']:
                if hours_col in df.columns:
                    mental_health_cols.append(hours_col)
                    break
            
            # Add gaming preference if available
            if 'gaming_preference' in df.columns:
                mental_health_cols.append('gaming_preference')
                
            # Add all mental health related columns
            mental_health_cols.extend([col for col in df.columns 
                                     if any(keyword in col.lower() 
                                           for keyword in ['anxiety', 'wellbeing', 'aggression', 'depression', 'stress'])])
            
            available_cols = [col for col in mental_health_cols if col in df.columns]
            if len(available_cols) > 2:  # Must have more than just basic columns
                df_mental = df[available_cols].copy()
                df_mental['source_dataset'] = dataset_name
                mental_health_dfs.append(df_mental)
                print(f"  [ADD] {dataset_name}: {len(df_mental)} records")
    
    if mental_health_dfs:
        try:
            # First, ensure all DataFrames have consistent columns
            all_columns = set()
            for df in mental_health_dfs:
                all_columns.update(df.columns)
            
            # Standardize all DataFrames to have the same columns
            standardized_dfs = []
            for df in mental_health_dfs:
                df_std = df.copy()
                # Add missing columns with NaN
                for col in all_columns:
                    if col not in df_std.columns:
                        df_std[col] = np.nan
                # Reorder columns consistently
                df_std = df_std[sorted(all_columns)]
                standardized_dfs.append(df_std)
            
            mental_health_dataset = pd.concat(standardized_dfs, ignore_index=True, sort=False)
            specialized_datasets['mental_health'] = mental_health_dataset
            
            # Save to file
            output_file = merged_data_dir / "mental_health_analysis_dataset.csv"
            mental_health_dataset.to_csv(output_file, index=False)
            print(f"[SAVE] Mental health dataset: {len(mental_health_dataset)} records -> {output_file}")
            
        except Exception as e:
            print(f"[ERROR] Failed to create mental health dataset: {e}")
            # Create simplified version
            simplified_df = mental_health_dfs[0].copy()  # Just use first dataset
            simplified_df['source_dataset'] = 'simplified'
            specialized_datasets['mental_health'] = simplified_df
            
            output_file = merged_data_dir / "mental_health_analysis_dataset.csv"
            simplified_df.to_csv(output_file, index=False)
            print(f"[SAVE] Simplified mental health dataset: {len(simplified_df)} records -> {output_file}")
    
    # 2. Gaming Behavior Dataset
    print("[CREATE] Gaming behavior focused dataset...")
    gaming_behavior_dfs = []
    
    for dataset_name, df in datasets.items():
        # Select gaming behavior relevant columns
        gaming_cols = ['participant_id', 'age']
        
        # Add gaming hours columns
        for hours_col in ['gaming_hours_weekly', 'gaming_hours_daily', 'hours_played']:
            if hours_col in df.columns:
                gaming_cols.append(hours_col)
        
        # Add other gaming-related columns
        gaming_cols.extend([col for col in df.columns 
                           if any(keyword in col.lower() 
                                 for keyword in ['gaming', 'play', 'hours', 'time', 'frequency', 'preference'])])
        
        available_cols = [col for col in gaming_cols if col in df.columns]
        # Must have gaming hours or related gaming data
        has_gaming_data = any(col in available_cols for col in ['gaming_hours_weekly', 'gaming_hours_daily', 'hours_played', 'gaming_preference'])
        
        if has_gaming_data:
            df_gaming = df[available_cols].copy()
            df_gaming['source_dataset'] = dataset_name
            gaming_behavior_dfs.append(df_gaming)
            print(f"  [ADD] {dataset_name}: {len(df_gaming)} records")
    
    if gaming_behavior_dfs:
        try:
            # First, ensure all DataFrames have consistent columns
            all_columns = set()
            for df in gaming_behavior_dfs:
                all_columns.update(df.columns)
            
            # Standardize all DataFrames to have the same columns
            standardized_dfs = []
            for df in gaming_behavior_dfs:
                df_std = df.copy()
                # Add missing columns with NaN
                for col in all_columns:
                    if col not in df_std.columns:
                        df_std[col] = np.nan
                # Reorder columns consistently
                df_std = df_std[sorted(all_columns)]
                standardized_dfs.append(df_std)
            
            gaming_behavior_dataset = pd.concat(standardized_dfs, ignore_index=True, sort=False)
            specialized_datasets['gaming_behavior'] = gaming_behavior_dataset
            
            # Save to file
            output_file = merged_data_dir / "gaming_behavior_analysis_dataset.csv"
            gaming_behavior_dataset.to_csv(output_file, index=False)
            print(f"[SAVE] Gaming behavior dataset: {len(gaming_behavior_dataset)} records -> {output_file}")
            
        except Exception as e:
            print(f"[ERROR] Failed to create gaming behavior dataset: {e}")
            # Create simplified version
            simplified_df = gaming_behavior_dfs[0].copy()  # Just use first dataset
            simplified_df['source_dataset'] = 'simplified'
            specialized_datasets['gaming_behavior'] = simplified_df
            
            output_file = merged_data_dir / "gaming_behavior_analysis_dataset.csv"
            simplified_df.to_csv(output_file, index=False)
            print(f"[SAVE] Simplified gaming behavior dataset: {len(simplified_df)} records -> {output_file}")
    
    # 3. Prediction Scales Dataset (if available)
    if 'prediction_scales' in datasets:
        print("[CREATE] Prediction scales dataset...")
        scales_df = datasets['prediction_scales'].copy()
        specialized_datasets['prediction_scales'] = scales_df
        
        # Save to file
        output_file = merged_data_dir / "prediction_scales_analysis_dataset.csv"
        scales_df.to_csv(output_file, index=False)
        print(f"[SAVE] Prediction scales dataset: {len(scales_df)} records -> {output_file}")
    
    # 4. Steam Games Dataset (if available)
    if 'steam_games' in datasets:
        print("[CREATE] Steam games dataset...")
        steam_df = datasets['steam_games'].copy()
        specialized_datasets['steam_games'] = steam_df
        
        # Save to file
        output_file = merged_data_dir / "steam_games_analysis_dataset.csv"
        steam_df.to_csv(output_file, index=False)
        print(f"[SAVE] Steam games dataset: {len(steam_df)} records -> {output_file}")
    
    return specialized_datasets

def generate_merge_summary(datasets, master_dataset, specialized_datasets, merged_data_dir):
    """Generate comprehensive summary of the merge process."""
    print("\n" + "="*60)
    print("[REPORT] DATASET MERGE SUMMARY")
    print("="*60)
    
    # Original datasets summary
    print("\n[ORIGINAL] Cleaned datasets loaded:")
    total_original_records = 0
    for dataset_name, df in datasets.items():
        print(f"  [OK] {dataset_name}: {len(df):,} records, {len(df.columns)} columns")
        total_original_records += len(df)
    
    print(f"\n[TOTAL] Original records: {total_original_records:,}")
    
    # Master dataset summary
    if master_dataset is not None:
        print(f"\n[MASTER] Combined dataset: {len(master_dataset):,} records, {len(master_dataset.columns)} columns")
        print(f"[COLUMNS] {list(master_dataset.columns)}")
    
    # Specialized datasets summary
    print(f"\n[SPECIALIZED] Analysis-specific datasets created: {len(specialized_datasets)}")
    for dataset_name, df in specialized_datasets.items():
        print(f"  [OK] {dataset_name}: {len(df):,} records, {len(df.columns)} columns")
    
    # Data source distribution
    if master_dataset is not None and 'data_source' in master_dataset.columns:
        print(f"\n[SOURCES] Data source distribution in master dataset:")
        source_counts = master_dataset['data_source'].value_counts()
        for source, count in source_counts.items():
            percentage = (count / len(master_dataset)) * 100
            print(f"  [SOURCE] {source}: {count:,} records ({percentage:.1f}%)")
    
    print(f"\n[FOLDER] Merged datasets saved to: {merged_data_dir}")
    
    # Create detailed summary file
    summary_file = merged_data_dir / "merge_summary_report.txt"
    with open(summary_file, 'w') as f:
        f.write("RespawnMetrics Dataset Merge Summary\n")
        f.write("="*40 + "\n\n")
        
        f.write("Original Datasets:\n")
        for dataset_name, df in datasets.items():
            f.write(f"  {dataset_name}: {len(df):,} records, {len(df.columns)} columns\n")
            f.write(f"    Columns: {list(df.columns)}\n\n")
        
        if master_dataset is not None:
            f.write(f"Master Dataset: {len(master_dataset):,} records\n")
            f.write(f"  Columns: {list(master_dataset.columns)}\n\n")
        
        f.write("Specialized Datasets:\n")
        for dataset_name, df in specialized_datasets.items():
            f.write(f"  {dataset_name}: {len(df):,} records, {len(df.columns)} columns\n")
            f.write(f"    Columns: {list(df.columns)}\n\n")
    
    print(f"[SAVE] Detailed summary saved: {summary_file}")

def merge_gaming_datasets():
    """Main function to coordinate the dataset merging process."""
    print("[MERGE] Starting RespawnMetrics dataset merge process...")
    print("="*60)
    
    # Setup directories
    cleaned_data_dir, merged_data_dir = setup_directories()
    print(f"[FOLDER] Cleaned data directory: {cleaned_data_dir}")
    print(f"[FOLDER] Merged data directory: {merged_data_dir}")
    
    # Load cleaned datasets
    datasets = load_cleaned_datasets(cleaned_data_dir)
    
    if not datasets:
        print("[ERROR] No datasets loaded. Please run data cleaning first.")
        return
    
    # Standardize column names
    datasets = standardize_column_names(datasets)
    
    # Create master dataset
    master_dataset = create_master_dataset(datasets)
    
    if master_dataset is not None:
        # Save master dataset
        master_output_file = merged_data_dir / "master_gaming_mental_health_dataset.csv"
        master_dataset.to_csv(master_output_file, index=False)
        print(f"[SAVE] Master dataset saved: {master_output_file}")
    
    # Create specialized datasets
    specialized_datasets = create_analysis_specific_datasets(datasets, merged_data_dir)
    
    # Generate summary report
    generate_merge_summary(datasets, master_dataset, specialized_datasets, merged_data_dir)
    
    print("\n[COMPLETE] Dataset merge process finished successfully!")
    print("[NEXT] Ready for database creation!")

if __name__ == "__main__":
    merge_gaming_datasets()