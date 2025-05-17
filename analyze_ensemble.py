import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def analyze_ensemble_data(ensemble_data):
    """
    Analyze ensemble forecast data and calculate statistics.
    
    Args:
        ensemble_data (pd.DataFrame): Combined ensemble data
        
    Returns:
        dict: Analysis results including statistics and metrics
    """
    try:
        if ensemble_data.empty:
            logger.warning("No ensemble data to analyze")
            return {}
            
        logger.info(f"Analyzing ensemble data with shape: {ensemble_data.shape}")
        logger.info(f"Columns in ensemble data: {ensemble_data.columns.tolist()}")
        logger.info(f"Number of unique models: {ensemble_data['model'].nunique()}")
        logger.info(f"Models present: {ensemble_data['model'].unique().tolist()}")
        logger.info(f"Forecast hours range: {ensemble_data['forecast_hour'].min()} to {ensemble_data['forecast_hour'].max()}")
        
        # Group by forecast hour and calculate statistics
        analysis = {}
        
        for variable in ['temperature', 'wind_speed', 'wind_direction', 'precipitation']:
            logger.info(f"Processing variable: {variable}")
            logger.info(f"Non-null values for {variable}: {ensemble_data[variable].count()}")
            logger.info(f"Value range for {variable}: {ensemble_data[variable].min()} to {ensemble_data[variable].max()}")
            
            stats = ensemble_data.groupby('forecast_hour')[variable].agg([
                'mean',
                'std',
                'min',
                'max',
                'median'
            ]).reset_index()
            
            logger.info(f"Stats shape for {variable}: {stats.shape}")
            logger.info(f"Stats columns: {stats.columns.tolist()}")
            
            # Calculate confidence intervals
            stats['ci_lower'] = stats['mean'] - 1.96 * stats['std']
            stats['ci_upper'] = stats['mean'] + 1.96 * stats['std']
            
            analysis[variable] = stats
            
        # Calculate model agreement
        model_agreement = ensemble_data.groupby(['forecast_hour', 'model']).agg({
            'temperature': 'mean',
            'wind_speed': 'mean',
            'wind_direction': 'mean',
            'precipitation': 'mean'
        }).reset_index()
        
        logger.info(f"Model agreement shape: {model_agreement.shape}")
        logger.info(f"Model agreement columns: {model_agreement.columns.tolist()}")
        
        analysis['model_agreement'] = model_agreement
        
        return analysis
        
    except Exception as e:
        logger.error(f"Error analyzing ensemble data: {str(e)}")
        raise 