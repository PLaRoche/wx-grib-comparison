import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import logging
import os

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
            
        # Set style
        plt.style.use('seaborn')
        sns.set_palette("husl")
        
        # Create plots for each variable
        for variable in ['temperature', 'wind_speed', 'wind_direction', 'precipitation']:
            if variable not in analysis_results:
                continue
                
            stats = analysis_results[variable]
            
            # Create figure
            plt.figure(figsize=(12, 6))
            
            # Plot mean and confidence intervals
            plt.plot(stats['forecast_hour'], stats['mean'], 'b-', label='Mean')
            plt.fill_between(
                stats['forecast_hour'],
                stats['ci_lower'],
                stats['ci_upper'],
                alpha=0.2,
                label='95% Confidence Interval'
            )
            
            # Plot min and max
            plt.plot(stats['forecast_hour'], stats['min'], 'r--', alpha=0.5, label='Min')
            plt.plot(stats['forecast_hour'], stats['max'], 'r--', alpha=0.5, label='Max')
            
            # Customize plot
            plt.title(f'{variable.title()} Forecast')
            plt.xlabel('Forecast Hour')
            plt.ylabel(variable.title())
            plt.legend()
            plt.grid(True)
            
            # Save plot
            output_file = os.path.join(output_dir, f'{variable}_forecast.png')
            plt.savefig(output_file)
            plt.close()
            
        # Create model agreement plot
        if 'model_agreement' in analysis_results:
            model_agreement = analysis_results['model_agreement']
            
            for variable in ['temperature', 'wind_speed', 'wind_direction', 'precipitation']:
                plt.figure(figsize=(12, 6))
                
                for model in model_agreement['model'].unique():
                    model_data = model_agreement[model_agreement['model'] == model]
                    plt.plot(
                        model_data['forecast_hour'],
                        model_data[variable],
                        label=model
                    )
                
                plt.title(f'{variable.title()} Model Agreement')
                plt.xlabel('Forecast Hour')
                plt.ylabel(variable.title())
                plt.legend()
                plt.grid(True)
                
                output_file = os.path.join(output_dir, f'{variable}_model_agreement.png')
                plt.savefig(output_file)
                plt.close()
                
        logger.info(f"Visualizations saved to {output_dir}")
        
    except Exception as e:
        logger.error(f"Error creating visualizations: {str(e)}")
        raise 