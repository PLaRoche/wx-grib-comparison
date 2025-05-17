import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import logging
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def create_ensemble_visualization(analysis_results, output_dir):
    """
    Create visualizations for ensemble analysis results.
    
    Args:
        analysis_results (dict): Analysis results from analyze_ensemble_data
        output_dir (str): Directory to save visualizations
    """
    try:
        if not analysis_results:
            logger.warning("No analysis results to visualize")
            return
            
        logger.info(f"Creating visualizations with keys: {list(analysis_results.keys())}")
            
        # Set style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
        # Get current time for x-axis labels
        current_time = datetime.utcnow()
        
        # Create plots for each variable
        for variable in ['temperature', 'wind_speed', 'wind_direction', 'precipitation']:
            if variable not in analysis_results:
                logger.warning(f"Variable {variable} not found in analysis results")
                continue
                
            stats = analysis_results[variable]
            logger.info(f"Plotting {variable} with data shape: {stats.shape}")
            logger.info(f"Data range for {variable}: {stats['mean'].min()} to {stats['mean'].max()}")
            
            # Create figure
            plt.figure(figsize=(12, 6))
            
            # Convert forecast hours to datetime for x-axis
            x_dates = [current_time + timedelta(hours=h) for h in stats['forecast_hour']]
            
            # Plot mean and confidence intervals
            plt.plot(x_dates, stats['mean'], 'b-', label='Mean')
            plt.fill_between(
                x_dates,
                stats['ci_lower'],
                stats['ci_upper'],
                alpha=0.2,
                label='95% Confidence Interval'
            )
            
            # Plot min and max
            plt.plot(x_dates, stats['min'], 'r--', alpha=0.5, label='Min')
            plt.plot(x_dates, stats['max'], 'r--', alpha=0.5, label='Max')
            
            # Customize plot
            plt.title(f'{variable.title()} Forecast')
            plt.xlabel('Time (UTC)')
            plt.ylabel(variable.title())
            plt.legend()
            plt.grid(True)
            
            # Format x-axis to show hours
            plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M\n%d %b'))
            plt.gcf().autofmt_xdate()  # Rotate and align the tick labels
            
            # Save plot
            output_file = os.path.join(output_dir, f'{variable}_forecast.png')
            plt.savefig(output_file, bbox_inches='tight')
            plt.close()
            
        # Create model agreement plot
        if 'model_agreement' in analysis_results:
            model_agreement = analysis_results['model_agreement']
            logger.info(f"Creating model agreement plots with data shape: {model_agreement.shape}")
            
            for variable in ['temperature', 'wind_speed', 'wind_direction', 'precipitation']:
                plt.figure(figsize=(12, 6))
                
                for model in model_agreement['model'].unique():
                    model_data = model_agreement[model_agreement['model'] == model]
                    logger.info(f"Plotting {variable} for model {model} with {len(model_data)} points")
                    
                    # Convert forecast hours to datetime for x-axis
                    x_dates = [current_time + timedelta(hours=h) for h in model_data['forecast_hour']]
                    
                    plt.plot(
                        x_dates,
                        model_data[variable],
                        label=model
                    )
                
                plt.title(f'{variable.title()} Model Agreement')
                plt.xlabel('Time (UTC)')
                plt.ylabel(variable.title())
                plt.legend()
                plt.grid(True)
                
                # Format x-axis to show hours
                plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M\n%d %b'))
                plt.gcf().autofmt_xdate()  # Rotate and align the tick labels
                
                output_file = os.path.join(output_dir, f'{variable}_model_agreement.png')
                plt.savefig(output_file, bbox_inches='tight')
                plt.close()
                
        logger.info(f"Visualizations saved to {output_dir}")
        
    except Exception as e:
        logger.error(f"Error creating visualizations: {str(e)}")
        raise 