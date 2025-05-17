import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import logging
import os
from datetime import datetime, timedelta
import numpy as np
from matplotlib.patches import Patch
from matplotlib.backends.backend_pdf import PdfPages

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
        
        # Create PDF file for combined plots
        pdf_path = os.path.join(output_dir, 'ensemble_analysis.pdf')
        with PdfPages(pdf_path) as pdf:
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
                
                # Format x-axis to show every 6 hours with date
                plt.gca().xaxis.set_major_locator(plt.matplotlib.dates.HourLocator(interval=6))
                plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%m/%d %H'))
                plt.gcf().autofmt_xdate()  # Rotate and align the tick labels
                
                # Save individual plot
                output_file = os.path.join(output_dir, f'{variable}_forecast.png')
                plt.savefig(output_file, bbox_inches='tight')
                
                # Add to PDF
                pdf.savefig(bbox_inches='tight')
                plt.close()
                
            # Create model agreement plot
            if 'model_agreement' in analysis_results:
                model_agreement = analysis_results['model_agreement']
                logger.info(f"Creating model agreement plots with data shape: {model_agreement.shape}")
                
                for variable in ['temperature', 'wind_speed', 'wind_direction', 'precipitation']:
                    # Create a figure with two subplots
                    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12), height_ratios=[2, 1])
                    
                    # Get all data points for the variable
                    all_data = []
                    forecast_hours = []
                    for model in model_agreement['model'].unique():
                        model_data = model_agreement[model_agreement['model'] == model]
                        all_data.extend(model_data[variable].values)
                        forecast_hours.extend(model_data['forecast_hour'].values)
                    
                    # Create a single violin plot for all data
                    data_by_hour = []
                    positions = []
                    for hour in sorted(model_agreement['forecast_hour'].unique()):
                        hour_data = model_agreement[model_agreement['forecast_hour'] == hour][variable].values
                        if len(hour_data) > 1:  # Only include hours with multiple data points
                            data_by_hour.append(hour_data)
                            positions.append(hour)
                    
                    if data_by_hour:  # Only create violin plot if we have data
                        parts = ax1.violinplot(
                            data_by_hour,
                            positions=positions,
                            showmeans=True,
                            showextrema=True
                        )
                        
                        # Set color for the violin plot
                        for pc in parts['bodies']:
                            pc.set_facecolor('lightblue')
                            pc.set_alpha(0.7)
                    
                    # Add mean line for all data points
                    mean_by_hour = model_agreement.groupby('forecast_hour')[variable].mean()
                    ax1.plot(mean_by_hour.index, mean_by_hour.values, 
                            color='blue', linestyle='--', alpha=0.7, label='Ensemble Mean')
                    
                    # Add individual data points
                    for hour in sorted(model_agreement['forecast_hour'].unique()):
                        hour_data = model_agreement[model_agreement['forecast_hour'] == hour][variable].values
                        if len(hour_data) == 1:  # Plot single points as scatter
                            ax1.scatter(hour, hour_data[0], color='red', alpha=0.5, s=30)
                    
                    # Customize violin plot
                    ax1.set_title(f'{variable.title()} Ensemble Distribution')
                    ax1.set_xlabel('Forecast Hour')
                    ax1.set_ylabel(variable.title())
                    ax1.grid(True)
                    ax1.legend()
                    # Show per-hour x-axis
                    ax1.set_xticks(sorted(model_agreement['forecast_hour'].unique()))
                    
                    # Plot 2: Line plot for comparison
                    model_list = list(model_agreement['model'].unique())
                    colors = sns.color_palette("husl", len(model_list))
                    color_dict = dict(zip(model_list, colors))  # Create color mapping dictionary
                    
                    # Create legend patches for the violin plot
                    legend_patches = []
                    for model, color in color_dict.items():
                        legend_patches.append(Patch(facecolor=color, alpha=0.7, label=model))
                    ax1.legend(handles=legend_patches, title='Models')
                    
                    for i, model in enumerate(model_list):
                        model_data = model_agreement[model_agreement['model'] == model]
                        x_dates = [current_time + timedelta(hours=h) for h in model_data['forecast_hour']]
                        ax2.plot(
                            x_dates,
                            model_data[variable],
                            label=model,
                            color=color_dict[model],  # Use consistent colors
                            alpha=0.7,
                            linewidth=2  # Make lines more visible
                        )
                    # Customize line plot
                    ax2.set_title(f'{variable.title()} Model Agreement - Time Series')
                    ax2.set_xlabel('Time (UTC)')
                    ax2.set_ylabel(variable.title())
                    ax2.legend(title='Models')  # Add title to legend
                    ax2.grid(True)
                    # Format x-axis to show every 6 hours with date
                    ax2.xaxis.set_major_locator(plt.matplotlib.dates.HourLocator(interval=6))
                    ax2.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%m/%d %H'))
                    plt.gcf().autofmt_xdate()  # Rotate and align the tick labels
                    
                    # Save individual plot
                    output_file = os.path.join(output_dir, f'{variable}_model_agreement.png')
                    plt.savefig(output_file, bbox_inches='tight')
                    
                    # Add to PDF
                    pdf.savefig(bbox_inches='tight')
                    plt.close()
                
        logger.info(f"Visualizations saved to {output_dir}")
        logger.info(f"Combined PDF saved to {pdf_path}")
        
    except Exception as e:
        logger.error(f"Error creating visualizations: {str(e)}")
        raise 