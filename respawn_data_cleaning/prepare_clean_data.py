import pandas as pd
from pathlib import Path
from typing import Tuple, Optional
import re

def setup_paths() -> Tuple[Path, Path]:
    """
    Set up file paths for data processing.
    
    Returns:
        Tuple[Path, Path]: Cleaned data path and prepared data path
    """
    base_path = Path(__file__).resolve().parents[1]
    cleaned_data_path = base_path / "respawn_data_cleaned"
    prepared_data_path = base_path / "respawn_data_prepared"
    prepared_data_path.mkdir(exist_ok=True)
    
    return cleaned_data_path, prepared_data_path

def load_cleaned_datasets(cleaned_data_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load all cleaned datasets.
    
    Args:
        cleaned_data_path (Path): Path to cleaned data folder
        
    Returns:
        Tuple of DataFrames: wellbeing, steam, seven_scales, aggression, anxiety
    """
    print("📊 Loading cleaned datasets...")
    
    # Load datasets with error handling
    try:
        wellbeing_df = pd.read_csv(cleaned_data_path / "video_games_wellbeing_clean.csv")
        print(f"  ✅ Wellbeing: {len(wellbeing_df)} rows")
    except FileNotFoundError:
        print("  ⚠️ Wellbeing dataset not found")
        wellbeing_df = pd.DataFrame()
    
    try:
        steam_df = pd.read_csv(cleaned_data_path / "steam_games_clean.csv")
        print(f"  ✅ Steam: {len(steam_df)} rows")
    except FileNotFoundError:
        print("  ⚠️ Steam dataset not found")
        steam_df = pd.DataFrame()
    
    try:
        seven_scales_df = pd.read_excel(cleaned_data_path / "gaming_7scales_prediction_clean.xlsx")
        print(f"  ✅ 7-Scales: {len(seven_scales_df)} rows")
    except FileNotFoundError:
        print("  ⚠️ 7-Scales dataset not found")
        seven_scales_df = pd.DataFrame()
    
    try:
        aggression_df = pd.read_csv(cleaned_data_path / "gaming_aggression_clean.csv")
        print(f"  ✅ Aggression: {len(aggression_df)} rows")
    except FileNotFoundError:
        print("  ⚠️ Aggression dataset not found")
        aggression_df = pd.DataFrame()
    
    try:
        anxiety_df = pd.read_csv(cleaned_data_path / "gaming_anxiety_clean.csv")
        print(f"  ✅ Anxiety: {len(anxiety_df)} rows")
    except FileNotFoundError:
        print("  ⚠️ Anxiety dataset not found")
        anxiety_df = pd.DataFrame()
    
    return wellbeing_df, steam_df, seven_scales_df, aggression_df, anxiety_df

def analyze_datasets(wellbeing_df: pd.DataFrame, steam_df: pd.DataFrame) -> None:
    """
    Analyze dataset structures for merging possibilities.
    
    Args:
        wellbeing_df (pd.DataFrame): Wellbeing dataset
        steam_df (pd.DataFrame): Steam dataset
    """
    print("\n🔍 Analyzing dataset structures...")
    
    if not wellbeing_df.empty:
        print(f"Wellbeing columns: {list(wellbeing_df.columns)}")
        if 'game_title' in wellbeing_df.columns:
            print(f"Sample game titles: {wellbeing_df['game_title'].head().tolist()}")
    
    if not steam_df.empty:
        print(f"Steam columns: {list(steam_df.columns)}")
        if 'game_title' in steam_df.columns:
            print(f"Sample game titles: {steam_df['game_title'].head().tolist()}")

def normalize_game_titles(title: Optional[str]) -> str:
    """
    Normalize game titles for better matching.
    
    Args:
        title (Optional[str]): Game title to normalize
        
    Returns:
        str: Normalized title
    """
    if pd.isna(title) or title is None:
        return ""
    
    # Convert to lowercase and remove special characters
    normalized = str(title).lower()
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized

def merge_wellbeing_steam(wellbeing_df: pd.DataFrame, steam_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge wellbeing and steam datasets on game titles.
    
    Args:
        wellbeing_df (pd.DataFrame): Wellbeing dataset
        steam_df (pd.DataFrame): Steam dataset
        
    Returns:
        pd.DataFrame: Merged dataset
    """
    if wellbeing_df.empty or steam_df.empty:
        print("⚠️ Cannot merge: one or both datasets are empty")
        return wellbeing_df if not wellbeing_df.empty else steam_df
    
    print("\n🔗 Attempting to merge wellbeing and steam data...")
    
    # Check if game_title columns exist
    wellbeing_has_title = 'game_title' in wellbeing_df.columns
    steam_has_title = 'game_title' in steam_df.columns
    
    if not wellbeing_has_title or not steam_has_title:
        print(f"⚠️ Missing game_title column - Wellbeing: {wellbeing_has_title}, Steam: {steam_has_title}")
        print("Creating combined dataset without merge...")
        
        # Add dataset source column
        wellbeing_df['data_source'] = 'wellbeing'
        steam_df['data_source'] = 'steam'
        
        return pd.concat([wellbeing_df, steam_df], ignore_index=True, sort=False)
    
    # Normalize titles for better matching
    wellbeing_df_copy = wellbeing_df.copy()
    steam_df_copy = steam_df.copy()
    
    wellbeing_df_copy['game_title_normalized'] = wellbeing_df_copy['game_title'].apply(normalize_game_titles)
    steam_df_copy['game_title_normalized'] = steam_df_copy['game_title'].apply(normalize_game_titles)
    
    # Attempt merge
    merged_df = wellbeing_df_copy.merge(
        steam_df_copy, 
        on='game_title_normalized', 
        how='left', 
        suffixes=('', '_steam')
    )
    
    # Drop the normalized column
    merged_df = merged_df.drop('game_title_normalized', axis=1)
    
    print(f"✅ Merged dataset created with {len(merged_df)} rows")
    print(f"   - Matched games: {merged_df['game_title_steam'].notna().sum()}")
    print(f"   - Unmatched games: {merged_df['game_title_steam'].isna().sum()}")
    
    return merged_df

def create_analysis_ready_datasets(
    merged_df: pd.DataFrame, 
    seven_scales_df: pd.DataFrame,
    aggression_df: pd.DataFrame,
    anxiety_df: pd.DataFrame,
    prepared_data_path: Path
) -> None:
    """
    Create analysis-ready datasets for different research questions.
    
    Args:
        merged_df (pd.DataFrame): Merged wellbeing and steam data
        seven_scales_df (pd.DataFrame): 7-scales prediction data
        aggression_df (pd.DataFrame): Gaming aggression data
        anxiety_df (pd.DataFrame): Gaming anxiety data
        prepared_data_path (Path): Path to save prepared data
    """
    print("\n💾 Creating analysis-ready datasets...")
    
    # Save main merged dataset
    if not merged_df.empty:
        merged_df.to_csv(prepared_data_path / "games_wellbeing_steam_prepared.csv", index=False)
        print("  ✅ Main merged dataset saved")
    
    # Save individual datasets for specialized analysis
    if not seven_scales_df.empty:
        seven_scales_df.to_csv(prepared_data_path / "gaming_7scales_prepared.csv", index=False)
        seven_scales_df.to_excel(prepared_data_path / "gaming_7scales_prepared.xlsx", index=False)
        print("  ✅ 7-scales dataset saved")
    
    if not aggression_df.empty:
        aggression_df.to_csv(prepared_data_path / "gaming_aggression_prepared.csv", index=False)
        print("  ✅ Aggression dataset saved")
    
    if not anxiety_df.empty:
        anxiety_df.to_csv(prepared_data_path / "gaming_anxiety_prepared.csv", index=False)
        print("  ✅ Anxiety dataset saved")
    
    # Create a summary dataset combining key metrics
    if not merged_df.empty and not seven_scales_df.empty:
        print("  🔗 Creating combined mental health summary...")
        # This would combine key metrics from different datasets
        # Implementation depends on actual column structures
    
    print(f"📁 All prepared datasets saved to: {prepared_data_path}")

def main() -> None:
    """
    Main function to prepare all datasets for analysis.
    """
    print("🚀 Starting data preparation process...")
    
    # Setup paths
    cleaned_data_path, prepared_data_path = setup_paths()
    
    # Load datasets
    wellbeing_df, steam_df, seven_scales_df, aggression_df, anxiety_df = load_cleaned_datasets(cleaned_data_path)
    
    # Analyze structures
    analyze_datasets(wellbeing_df, steam_df)
    
    # Merge main datasets
    merged_df = merge_wellbeing_steam(wellbeing_df, steam_df)
    
    # Create analysis-ready datasets
    create_analysis_ready_datasets(
        merged_df, seven_scales_df, aggression_df, anxiety_df, prepared_data_path
    )
    
    print("\n🎉 Data preparation completed successfully!")
    print(f"📊 Prepared data saved to: {prepared_data_path}")

if __name__ == "__main__":
    main()
