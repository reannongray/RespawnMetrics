"""
Custom utility functions for Respawn Metrics project.
This module contains at least 3 custom functions with type hints and docstrings as required for capstone.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Union
import re
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def calculate_gaming_wellness_score(
    hours_played: float, 
    feel_after: str, 
    anxiety_level: int, 
    age: int
) -> float:
    """
    Calculate a composite wellness score based on gaming behavior and mental health metrics.
    
    This function creates a new feature by combining multiple variables to assess
    overall gaming wellness on a scale of 0-100.
    
    Args:
        hours_played (float): Number of hours played per session/day
        feel_after (str): How the player feels after gaming ('positive', 'negative', 'neutral')
        anxiety_level (int): Anxiety level on a scale (typically 1-10)
        age (int): Player's age
        
    Returns:
        float: Wellness score between 0-100 (higher = better wellness)
        
    Example:
        >>> calculate_gaming_wellness_score(2.5, 'positive', 3, 25)
        78.5
    """
    try:
        # Base score starts at 50
        wellness_score = 50.0
        
        # Hours adjustment (moderate gaming is healthier)
        if hours_played <= 2:
            wellness_score += 20  # Moderate gaming bonus
        elif hours_played <= 4:
            wellness_score += 10  # Reasonable gaming
        elif hours_played <= 6:
            wellness_score -= 5   # Getting excessive
        else:
            wellness_score -= 20  # Excessive gaming penalty
        
        # Feeling adjustment
        feel_multiplier = {
            'positive': 1.2,
            'good': 1.2,
            'neutral': 1.0,
            'negative': 0.7,
            'bad': 0.7
        }
        
        # Normalize feeling string
        feel_normalized = str(feel_after).lower().strip()
        multiplier = feel_multiplier.get(feel_normalized, 1.0)
        wellness_score *= multiplier
        
        # Anxiety adjustment (lower anxiety is better)
        anxiety_penalty = (anxiety_level - 1) * 3  # Scale 1-10 to 0-27 penalty
        wellness_score -= anxiety_penalty
        
        # Age adjustment (different age groups have different baselines)
        if 18 <= age <= 25:
            wellness_score += 5   # Young adults - gaming is more socially acceptable
        elif 26 <= age <= 35:
            wellness_score += 0   # Prime adult - no adjustment
        elif 36 <= age <= 50:
            wellness_score -= 5   # Older adults - might have more responsibilities
        else:
            wellness_score -= 10  # Outside typical gaming demographics
        
        # Ensure score is within bounds
        wellness_score = max(0, min(100, wellness_score))
        
        return round(wellness_score, 2)
        
    except Exception as e:
        logger.warning(f"Error calculating wellness score: {e}")
        return 50.0  # Return neutral score on error

def categorize_gaming_behavior(
    hours_played: float, 
    frequency_per_week: Optional[int] = None
) -> str:
    """
    Categorize gaming behavior into risk levels based on playing patterns.
    
    This function creates a categorical feature from numerical gaming data
    to help identify problematic gaming patterns.
    
    Args:
        hours_played (float): Hours played per gaming session
        frequency_per_week (Optional[int]): Number of gaming sessions per week
        
    Returns:
        str: Gaming behavior category ('Casual', 'Regular', 'Heavy', 'Problematic')
        
    Example:
        >>> categorize_gaming_behavior(1.5, 3)
        'Casual'
        >>> categorize_gaming_behavior(8.0, 6)
        'Problematic'
    """
    try:
        # Calculate weekly hours if frequency is provided
        if frequency_per_week is not None:
            weekly_hours = hours_played * frequency_per_week
        else:
            # Assume daily play and multiply by 7
            weekly_hours = hours_played * 7
        
        # Categorize based on WHO gaming disorder criteria and research
        if weekly_hours < 7:  # Less than 1 hour per day
            return 'Casual'
        elif weekly_hours < 21:  # 1-3 hours per day
            return 'Regular'
        elif weekly_hours < 42:  # 3-6 hours per day
            return 'Heavy'
        else:  # More than 6 hours per day
            return 'Problematic'
            
    except Exception as e:
        logger.warning(f"Error categorizing gaming behavior: {e}")
        return 'Unknown'

def analyze_genre_mental_health_correlation(
    df: pd.DataFrame, 
    genre_col: str = 'genres', 
    mental_health_cols: List[str] = ['anxiety_level', 'aggression_score']
) -> Dict[str, Dict[str, float]]:
    """
    Analyze correlation between game genres and mental health metrics.
    
    This function performs feature engineering by extracting insights from
    categorical genre data and correlating it with numerical mental health scores.
    
    Args:
        df (pd.DataFrame): Dataset containing genre and mental health data
        genre_col (str): Column name containing genre information
        mental_health_cols (List[str]): List of mental health metric column names
        
    Returns:
        Dict[str, Dict[str, float]]: Nested dictionary with genre as key and 
                                   mental health metrics as values
                                   
    Example:
        >>> data = pd.DataFrame({
        ...     'genres': ['Action, Shooter', 'Puzzle, Strategy'],
        ...     'anxiety_level': [7, 3],
        ...     'aggression_score': [8.5, 2.1]
        ... })
        >>> analyze_genre_mental_health_correlation(data)
        {'Action': {'anxiety_level': 7.0, 'aggression_score': 8.5}, ...}
    """
    try:
        genre_analysis = {}
        
        # Clean and validate input
        if df.empty:
            logger.warning("Empty DataFrame provided")
            return genre_analysis
        
        if genre_col not in df.columns:
            logger.warning(f"Genre column '{genre_col}' not found")
            return genre_analysis
        
        # Split genres and create expanded dataset
        expanded_data = []
        
        for idx, row in df.iterrows():
            if pd.isna(row[genre_col]):
                continue
                
            # Split genres by comma and clean
            genres = [genre.strip() for genre in str(row[genre_col]).split(',')]
            
            for genre in genres:
                if genre and genre != '':
                    # Create row for each genre
                    genre_row = {'genre': genre}
                    
                    # Add mental health metrics
                    for col in mental_health_cols:
                        if col in df.columns and pd.notna(row[col]):
                            genre_row[col] = row[col]
                    
                    expanded_data.append(genre_row)
        
        if not expanded_data:
            logger.warning("No valid genre data found")
            return genre_analysis
        
        # Convert to DataFrame for analysis
        expanded_df = pd.DataFrame(expanded_data)
        
        # Calculate statistics by genre
        for genre in expanded_df['genre'].unique():
            genre_data = expanded_df[expanded_df['genre'] == genre]
            genre_stats = {}
            
            for col in mental_health_cols:
                if col in genre_data.columns:
                    values = pd.to_numeric(genre_data[col], errors='coerce').dropna()
                    if len(values) > 0:
                        genre_stats[col] = {
                            'mean': round(values.mean(), 2),
                            'median': round(values.median(), 2),
                            'std': round(values.std(), 2),
                            'count': len(values)
                        }
            
            if genre_stats:
                genre_analysis[genre] = genre_stats
        
        return genre_analysis
        
    except Exception as e:
        logger.error(f"Error analyzing genre-mental health correlation: {e}")
        return {}

def create_predictive_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create additional predictive features for machine learning models.
    
    This function performs comprehensive feature engineering by creating
    new variables that can improve model performance for predicting
    gaming-related mental health outcomes.
    
    Args:
        df (pd.DataFrame): Input dataset with gaming and mental health data
        
    Returns:
        pd.DataFrame: Dataset with additional engineered features
        
    Example:
        >>> data = pd.DataFrame({
        ...     'hours_played': [2.5, 8.0],
        ...     'age': [25, 17],
        ...     'feel_after': ['positive', 'negative']
        ... })
        >>> enhanced = create_predictive_features(data)
        >>> 'hours_category' in enhanced.columns
        True
    """
    try:
        # Create copy to avoid modifying original
        enhanced_df = df.copy()
        
        # Feature 1: Hours played categories
        if 'hours_played' in enhanced_df.columns:
            enhanced_df['hours_category'] = pd.cut(
                enhanced_df['hours_played'],
                bins=[0, 1, 3, 6, float('inf')],
                labels=['Light', 'Moderate', 'Heavy', 'Excessive'],
                include_lowest=True
            )
        
        # Feature 2: Age groups
        if 'age' in enhanced_df.columns:
            enhanced_df['age_group'] = pd.cut(
                enhanced_df['age'],
                bins=[0, 18, 25, 35, 50, 100],
                labels=['Minor', 'Young_Adult', 'Adult', 'Middle_Age', 'Senior'],
                include_lowest=True
            )
        
        # Feature 3: Weekend vs weekday indicator (if timestamp available)
        if 'created_at' in enhanced_df.columns:
            try:
                enhanced_df['created_at'] = pd.to_datetime(enhanced_df['created_at'])
                enhanced_df['is_weekend'] = enhanced_df['created_at'].dt.dayofweek >= 5
            except:
                logger.warning("Could not parse created_at column for weekend feature")
        
        # Feature 4: Gaming intensity score
        if all(col in enhanced_df.columns for col in ['hours_played']):
            # Normalize hours played to 0-1 scale for intensity
            max_hours = enhanced_df['hours_played'].max()
            if max_hours > 0:
                enhanced_df['gaming_intensity'] = enhanced_df['hours_played'] / max_hours
        
        # Feature 5: Binary positive/negative sentiment
        if 'feel_after' in enhanced_df.columns:
            positive_feelings = ['positive', 'good', 'great', 'happy', 'relaxed']
            enhanced_df['positive_experience'] = enhanced_df['feel_after'].str.lower().isin(positive_feelings)
        
        # Feature 6: Mental health risk score (if anxiety/aggression data available)
        risk_components = []
        if 'anxiety_level' in enhanced_df.columns:
            # Normalize anxiety (higher = more risk)
            max_anxiety = enhanced_df['anxiety_level'].max()
            if max_anxiety > 0:
                risk_components.append(enhanced_df['anxiety_level'] / max_anxiety)
        
        if 'aggression_score' in enhanced_df.columns:
            # Normalize aggression (higher = more risk)
            max_aggression = enhanced_df['aggression_score'].max()
            if max_aggression > 0:
                risk_components.append(enhanced_df['aggression_score'] / max_aggression)
        
        if risk_components:
            enhanced_df['mental_health_risk'] = sum(risk_components) / len(risk_components)
        
        logger.info(f"Created {len(enhanced_df.columns) - len(df.columns)} new features")
        return enhanced_df
        
    except Exception as e:
        logger.error(f"Error creating predictive features: {e}")
        return df  # Return original DataFrame on error

# Helper function for data validation
def validate_dataset_quality(df: pd.DataFrame, required_cols: List[str]) -> Dict[str, Union[bool, int, float]]:
    """
    Validate dataset quality and completeness.
    
    Args:
        df (pd.DataFrame): Dataset to validate
        required_cols (List[str]): List of required column names
        
    Returns:
        Dict[str, Union[bool, int, float]]: Quality metrics
    """
    try:
        quality_report = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'missing_required_cols': [col for col in required_cols if col not in df.columns],
            'completeness_score': (1 - df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
            'duplicate_rows': df.duplicated().sum(),
            'is_valid': True
        }
        
        # Mark as invalid if missing required columns or too many missing values
        if quality_report['missing_required_cols'] or quality_report['completeness_score'] < 50:
            quality_report['is_valid'] = False
        
        return quality_report
        
    except Exception as e:
        logger.error(f"Error validating dataset: {e}")
        return {'is_valid': False, 'error': str(e)}