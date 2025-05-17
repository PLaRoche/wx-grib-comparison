import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import logging
import os
from datetime import datetime, timedelta
import numpy as np
from matplotlib.patches import Patch

logger = logging.getLogger(__name__)

def create_ensemble_visualization(analysis_results, output_dir):
    """
    Create visualizations for ensemble analysis results.
    
    Args:
        analysis_results (dict): Dictionary containing analysis results
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
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
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
            
            # Format x-axis to show every hour
            plt.gca().xaxis.set_major_locator(plt.matplotlib.dates.HourLocator(interval=1))
            plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H'))
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
                # Create a figure with two subplots
                fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12), height_ratios=[2, 1])
                
                # Assign a color to each model for the violin plot
                model_list = list(model_agreement['model'].unique())
                colors = sns.color_palette("husl", len(model_list))
                legend_patches = []
                
                for i, model in enumerate(model_list):
                    model_data = model_agreement[model_agreement['model'] == model]
                    logger.info(f"Plotting {variable} for model {model} with {len(model_data)} points")
                    
                    # Group data by forecast hour
                    grouped_data = model_data.groupby('forecast_hour')[variable].apply(list).reset_index()
                    # Only keep hours with at least 2 data points
                    filtered = grouped_data[grouped_data[variable].apply(lambda x: len(x) > 1)]
                    if filtered.empty:
                        logger.info(f"Skipping violin plot for model {model} and variable {variable} (not enough data)")
                        continue
                    
                    # Create violin plot
                    parts = ax1.violinplot(
                        filtered[variable].values,
                        positions=filtered['forecast_hour'],
                        showmeans=True,
                        showextrema=True
                    )
                    # Set color for this model's violins
                    for pc in parts['bodies']:
                        pc.set_facecolor(colors[i])
                        pc.set_alpha(0.7)
                    # Add to legend
                    legend_patches.append(Patch(facecolor=colors[i], edgecolor='k', label=model, alpha=0.7))
                    # Add mean line
                    ax1.plot(filtered['forecast_hour'], 
                             [np.mean(x) for x in filtered[variable]], 
                             color=colors[i], linestyle='--', alpha=0.7)
                
                # Customize violin plot
                ax1.set_title(f'{variable.title()} Model Agreement - Distribution')
                ax1.set_xlabel('Forecast Hour')
                ax1.set_ylabel(variable.title())
                ax1.grid(True)
                # Add model legend
                if legend_patches:
                    ax1.legend(handles=legend_patches, title='Model')
                # Show per-hour x-axis
                ax1.set_xticks(sorted(model_agreement['forecast_hour'].unique()))
                
                # Plot 2: Line plot for comparison
                for i, model in enumerate(model_list):
                    model_data = model_agreement[model_agreement['model'] == model]
                    x_dates = [current_time + timedelta(hours=h) for h in model_data['forecast_hour']]
                    ax2.plot(
                        x_dates,
                        model_data[variable],
                        label=model,
                        color=colors[i],
                        alpha=0.7
                    )
                # Customize line plot
                ax2.set_title(f'{variable.title()} Model Agreement - Time Series')
                ax2.set_xlabel('Time (UTC)')
                ax2.set_ylabel(variable.title())
                ax2.legend()
                ax2.grid(True)
                # Format x-axis to show every hour
                ax2.xaxis.set_major_locator(plt.matplotlib.dates.HourLocator(interval=1))
                ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H'))
                plt.gcf().autofmt_xdate()  # Rotate and align the tick labels
                # Adjust layout and save
                plt.tight_layout()
                output_file = os.path.join(output_dir, f'{variable}_model_agreement.png')
                plt.savefig(output_file, bbox_inches='tight')
                plt.close()
                
        logger.info(f"Visualizations saved to {output_dir}")
        
    except Exception as e:
        logger.error(f"Error creating visualizations: {str(e)}")
        raise 