import os
import pandas as pd
from typing import Tuple
import numpy as np

def get_file_paths() -> Tuple[str, ...]:
    """
    Get file paths for all raw datasets.
    
    Returns:
        Tuple[str, ...]: Tuple containing paths to all raw data files
    """
    # Get the current script directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Paths to raw data files (relative to this script's folder)
    data_folder = os.path.join(base_dir, "..", "respawn_data")
    
    return (
        os.path.join(data_folder, "gaming_7scales_prediction.xlsx"),
        os.path.join(data_folder, "gaming_aggression.csv"),
        os.path.join(data_folder, "gaming_anxiety.csv"),
        os.path.join(data_folder, "video_games_wellbeing.csv"),
        os.path.join(data_folder, "steam_games.csv")
    )

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names by making them lowercase and replacing spaces with underscores.
    
    Args:
        df (pd.DataFrame): Input dataframe
        
    Returns:
        pd.DataFrame: Dataframe with cleaned column names
    """
    df_copy = df.copy()
    df_copy.columns = [col.strip().lower().replace(" ", "_").replace("-", "_") for col in df_copy.columns]
    return df_copy

def clean_seven_scales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the 7-scales prediction dataset.
    
    Args:
        df (pd.DataFrame): Raw 7-scales dataset
        
    Returns:
        pd.DataFrame: Cleaned dataset
    """
    df_clean = clean_column_names(df)
    
    # Handle missing values
    df_clean = df_clean.dropna(how='all')  # Remove completely empty rows
    
    # Fill numeric columns with median, categorical with mode
    for col in df_clean.columns:
        if df_clean[col].dtype in ['float64', 'int64']:
            df_clean[col] = df_clean[col].fillna(df_clean[col].median())
        else:
            df_clean[col] = df_clean[col].fillna(df_clean[col].mode().iloc[0] if not df_clean[col].mode().empty else 'Unknown')
    
    return df_clean

def clean_wellbeing_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the gaming wellbeing dataset.
    
    Args:
        df (pd.DataFrame): Raw wellbeing dataset
        
    Returns:
        pd.DataFrame: Cleaned dataset
    """
    df_clean = clean_column_names(df)
    
    # Handle missing values
    df_clean = df_clean.dropna(how='all')
    
    # Convert numeric strings to proper numeric types
    numeric_cols = ['age', 'hours', 'earnings']
    for col in numeric_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Clean categorical data
    if 'platform' in df_clean.columns:
        df_clean['platform'] = df_clean['platform'].str.title()
    
    return df_clean

def load_and_clean_datasets() -> None:
    """
    Load all raw datasets, clean them, and save to cleaned data folder.
    """
    print("🧹 Starting data cleaning process...")
    
    # Get file paths
    (seven_scales_file, aggression_file, anxiety_file, 
     wellbeing_file, steam_file) = get_file_paths()
    
    # Check if files exist before loading
    files_to_check = {
        "7-scales": seven_scales_file,
        "aggression": aggression_file,
        "anxiety": anxiety_file,
        "wellbeing": wellbeing_file,
        "steam": steam_file
    }
    
    for name, file_path in files_to_check.items():
        if not os.path.exists(file_path):
            print(f"⚠️  Warning: {name} file not found at {file_path}")
    
    # Load datasets
    try:
        print("📊 Loading datasets...")
        seven_df = pd.read_excel(seven_scales_file) if os.path.exists(seven_scales_file) else pd.DataFrame()
        aggr_df = pd.read_csv(aggression_file, encoding='latin1') if os.path.exists(aggression_file) else pd.DataFrame()
        anx_df = pd.read_csv(anxiety_file, encoding='latin1') if os.path.exists(anxiety_file) else pd.DataFrame()
        well_df = pd.read_csv(wellbeing_file, encoding='latin1') if os.path.exists(wellbeing_file) else pd.DataFrame()
        steam_df = pd.read_csv(steam_file, encoding='latin1') if os.path.exists(steam_file) else pd.DataFrame()
        
        print("✅ Datasets loaded successfully")
        
        # Clean datasets
        print("🔧 Cleaning datasets...")
        
        if not seven_df.empty:
            seven_df_clean = clean_seven_scales(seven_df)
        else:
            seven_df_clean = pd.DataFrame()
            
        if not well_df.empty:
            well_df_clean = clean_wellbeing_data(well_df)
        else:
            well_df_clean = pd.DataFrame()
            
        # Basic cleaning for other datasets
        aggr_df_clean = clean_column_names(aggr_df) if not aggr_df.empty else pd.DataFrame()
        anx_df_clean = clean_column_names(anx_df) if not anx_df.empty else pd.DataFrame()
        steam_df_clean = clean_column_names(steam_df) if not steam_df.empty else pd.DataFrame()
        
        print("✅ Datasets cleaned successfully")
        
        # Create cleaned data folder
        base_dir = os.path.dirname(os.path.abspath(__file__))
        cleaned_data_folder = os.path.join(base_dir, "..", "respawn_data_cleaned")
        os.makedirs(cleaned_data_folder, exist_ok=True)
        
        # Save cleaned datasets
        print("💾 Saving cleaned datasets...")
        
        if not seven_df_clean.empty:
            seven_df_clean.to_csv(os.path.join(cleaned_data_folder, "gaming_7scales_prediction_clean.csv"), index=False)
            seven_df_clean.to_excel(os.path.join(cleaned_data_folder, "gaming_7scales_prediction_clean.xlsx"), index=False)
            print("  ✅ 7-scales data saved")
        
        if not aggr_df_clean.empty:
            aggr_df_clean.to_csv(os.path.join(cleaned_data_folder, "gaming_aggression_clean.csv"), index=False)
            print("  ✅ Aggression data saved")
        
        if not anx_df_clean.empty:
            anx_df_clean.to_csv(os.path.join(cleaned_data_folder, "gaming_anxiety_clean.csv"), index=False)
            print("  ✅ Anxiety data saved")
        
        if not well_df_clean.empty:
            well_df_clean.to_csv(os.path.join(cleaned_data_folder, "video_games_wellbeing_clean.csv"), index=False)
            print("  ✅ Wellbeing data saved")
        
        if not steam_df_clean.empty:
            steam_df_clean.to_csv(os.path.join(cleaned_data_folder, "steam_games_clean.csv"), index=False)
            print("  ✅ Steam data saved")
        
        print("🎉 All datasets cleaned and saved successfully!")
        
    except Exception as e:
        print(f"❌ Error during data cleaning: {e}")
        raise

if __name__ == "__main__":
    load_and_clean_datasets()