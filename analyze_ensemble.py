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
            
        # Group by forecast hour and calculate statistics
        analysis = {}
        
        for variable in ['temperature', 'wind_speed', 'wind_direction', 'precipitation']:
            stats = ensemble_data.groupby('forecast_hour')[variable].agg([
                'mean',
                'std',
                'min',
                'max',
                'median'
            ]).reset_index()
            
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
        
        analysis['model_agreement'] = model_agreement
        
        return analysis
        
    except Exception as e:
        logger.error(f"Error analyzing ensemble data: {str(e)}")
        raise 