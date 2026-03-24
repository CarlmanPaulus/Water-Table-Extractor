import tkinter as tk
from tkinter import ttk
import pandas as pd
import os
import numpy as np
from tqdm import tqdm
import time
from datetime import datetime
import openpyxl
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
import sys
import io
import threading
import time  # For simulating a long process
import logging
from threading import Thread

#basepath
MLM_base_path = r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\WT\MLM"
CMC_base_path = r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\WT\CMC"
SBW_base_path = r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\WT\SBW"
Marudi_base_path = r"C:\Users\agust\OneDrive - Sarawak Government\P51 Marudi Study\Processed Data Marudi"

# List of directories, grouped for readability
directories = [
    #MA
    ("MA_baro",     fr"{MLM_base_path}\MA\MA1_baro"),
    ("MA1_diver",   fr"{MLM_base_path}\MA\MA1_diver"),
    ("MA2_diver",   fr"{MLM_base_path}\MA\MA2_diver"),
    # #MB
    ("MB_baro",     fr"{MLM_base_path}\MB\MB1_baro"),
    ("MB_diver1",   fr"{MLM_base_path}\MB\MB1_diver"),
    ("MB_diver2",   fr"{MLM_base_path}\MB\MB2_diver"),
    ("MACS_diver",  fr"{MLM_base_path}\MACS"),
    # #MC
    ("MC_baro",     fr"{MLM_base_path}\MC\MC1_baro"),
    ("MC_diver1",   fr"{MLM_base_path}\MC\MC1_diver"),
    ("MC_diver2",   fr"{MLM_base_path}\MC\MC2_diver"),
    # #MD
    ("MD_baro",     fr"{MLM_base_path}\MD\MD1_baro"),
    ("MD_diver1",   fr"{MLM_base_path}\MD\MD1_diver"),
    ("MD_diver2",   fr"{MLM_base_path}\MD\MD2_diver"),
    
    # CMC
    ("CA Baro",     fr"{CMC_base_path}\CA1_Baro"),
    ("CA",          fr"{CMC_base_path}\CA1_diver"),
    ("CACS",        fr"{CMC_base_path}\CACS_diver"),
    # ("CA2",         fr"{CMC_base_path}\CA2_diver"),
    ("CB1",          fr"{CMC_base_path}\CB1_diver"),
    ("CB Baro",     fr"{CMC_base_path}\CB_baro"),
    ("CB2",         fr"{CMC_base_path}\CB2_diver"),
    
    # #SBW 
    ("NA12 diver",        fr"{SBW_base_path}\NA\NA12_diver"),
    ("NForest diver",     fr"{SBW_base_path}\Naman Forest\NF_diver"),
    ("Q1 diver",          fr"{SBW_base_path}\Q Lines\Q1_diver"),
    ("Q8 diver",          fr"{SBW_base_path}\Q Lines\Q8_diver"),
    ("Q9 diver",          fr"{SBW_base_path}\Q Lines\Q9_diver"),
    # ("NL2-23 diver",      fr"{SBW_base_path}\NL2-23\NL2-23_diver"),
    ("NL2-22 baro",       fr"{SBW_base_path}\NL2-22\NL2-22_baro"),
    ("NL2-22 diver",      fr"{SBW_base_path}\NL2-22\NL2-22_diver"),
    ("NL2-43 diver",      fr"{SBW_base_path}\NL2-43\NL2-43_diver"),
    # ("NL2-23 baro",       fr"{SBW_base_path}\NL2-23\NL2-23_baro"),
    # ("NPK37",             fr"{SBW_base_path}\NPK\NPK37_diver"),
    ("SACS diver",        fr"{SBW_base_path}\SBW ACS\SACS_diver"),
    ("SACS baro",         fr"{SBW_base_path}\SBW ACS\SACS_baro"),
    
    # # Marudi TS2
    ("SSD1 baro",     fr"{Marudi_base_path}\TS2\SSD1_baro"),
    ("SSD1 wt1",      fr"{Marudi_base_path}\TS2\SSD1_wt1"),
    ("SSD1 wt2",      fr"{Marudi_base_path}\TS2\SSD1_wt2"),
    ("SSD2 baro",     fr"{Marudi_base_path}\TS2\SSD2_baro"),
    ("SSD2 wt1",      fr"{Marudi_base_path}\TS2\SSD2_wt1"),
    ("SSD2 wt2",      fr"{Marudi_base_path}\TS2\SSD2_wt2"),
    ("SSD3 wt1",      fr"{Marudi_base_path}\TS2\SSD3_wt1"),
    ("SSD3 wt2",      fr"{Marudi_base_path}\TS2\SSD3_wt2"),

    # Marudi TS4A
    ("SSD8 baro",   fr"{Marudi_base_path}\TS4A\SSD8_baro"),
    ("SSD8 wt1",    fr"{Marudi_base_path}\TS4A\SSD8_wt1"),
    ("SSD8 wt2",    fr"{Marudi_base_path}\TS4A\SSD8_wt2"),

    # # Marudi T5
    ("SSD10 baro",     fr"{Marudi_base_path}\T5\SSD10_baro"),
    ("SSD10 wt1",      fr"{Marudi_base_path}\T5\SSD10_wt1"),
    ("SSD10 wt2",      fr"{Marudi_base_path}\T5\SSD10_wt2"),
    ("SSD11 baro",     fr"{Marudi_base_path}\T5\SSD11_baro"),
    ("SSD11 wt1",      fr"{Marudi_base_path}\T5\SSD11_wt1"),
    ("SSD11 wt2",      fr"{Marudi_base_path}\T5\SSD11_wt2"),
    ("SSD12 baro",     fr"{Marudi_base_path}\T5\SSD12_baro"),
    ("SSD12 wt1",      fr"{Marudi_base_path}\T5\SSD12_wt1"),
    ("SSD12 wt2",      fr"{Marudi_base_path}\T5\SSD12_wt2"),

    # Marudi TS7
    ("SSD13 baro",      fr"{Marudi_base_path}\TS7\SSD13_baro"),
    ("SSD13 wt1",      fr"{Marudi_base_path}\TS7\SSD13_wt1"),
    ("SSD13 wt2",      fr"{Marudi_base_path}\TS7\SSD13_wt2"),
    ("SSD14 Baro",    fr"{Marudi_base_path}\TS7\SSD14_baro"),
    ("SSD14 wt1",     fr"{Marudi_base_path}\TS7\SSD14_wt1"),
    ("SSD14 wt2",     fr"{Marudi_base_path}\TS7\SSD14_wt2"),
    ("SSD15 Baro",    fr"{Marudi_base_path}\TS7\SSD15_baro"),
    ("SSD15 wt1",     fr"{Marudi_base_path}\TS7\SSD15_wt1"),
    ("SSD15 wt2",     fr"{Marudi_base_path}\TS7\SSD15_wt2"),
    ]


# Variable to hold selected directories
selected_directories = []  # This will store the selected directories

def update_selected_directories(*args):
    global selected_directories
    selected_directories = [dir_path for var, dir_path in zip(check_vars, [d[1] for d in directories]) if var.get()]
    
    # Print feedback for selected directories
    print("Selected Directories:")
    for directory in selected_directories:
        print(directory)

      
def process_directory(directory_path):
    
    # Change to the specified directory
    os.chdir(directory_path)
    
    # Get today's date
    today_date = datetime.today().strftime("%Y-%m-%d")  # Convert date to string
    date_format = '%d/%m/%y %H:%M:%S'

    current_directory = os.getcwd()                # Get the current working directory 
    sitename = os.path.basename(current_directory) # Return the last component of the path as sitename

    remark = "Combined"
    # Create new folder inside current folder
    output_folder = sitename + "_updated"

    # Replace 'folder_path' with the actual path to your folder
    input_folder = os.getcwd()  

    # Check if "SITE_updated" folder exists
    # If not, create new
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)        # Create new directory/folder_Updated

    # Loop through all files in the input folder
    for filename in os.listdir(input_folder):    # returning list of file in the current
                                                 # working directory (input_folder) as "filename"
                                                 # including SITE_updated folder   
        if filename.endswith('.xlsx'):
            input_path = os.path.join(input_folder, filename)
            
            # Read the CSV file
            # df = pd.read_excel(input_path, parse_dates=["Date Time"])
            df = pd.read_excel(input_path)
            df['Date Time'] = pd.to_datetime(df['Date Time'], format=date_format)
                        
            # removes rows where the 'Abs Pres (kPa) c:1 2' column contains missing values (NaN)
            df = df.dropna(subset=['Abs Pres (kPa) c:1 2'])
               
            # Delete specific columns if they exist
            columns_to_delete = ['Temp (°C) c:2',
                                 'Unnamed: 4','Water Level ACt (meters)','Good Battery',
                                 'Coupler Attached',
                                 'Coupler Detached',
                                 'Stopped','End Of File',
                                 'Abs Pres Barom. (kPa) c:1 2',
                                 'Unnamed: 2','Host Connected', 'Max: Temp (°C)',
                                 'Bad Battery','Temp (°C)','Temp (°C) c:1',
                                 'Unnamed: 5']
            columns_exist = [col for col in columns_to_delete if col in df.columns]
            if columns_exist:
                df = df.drop(columns_exist, axis=1)
                
            first_row_index = df.index[0]
            df.loc[first_row_index, 'EOF'] = -1111
                      
            last_row_index = df.index[-1]
            df.loc[last_row_index, 'EOF'] = -9999
            
            # Save the modified dataframe to a new CSV file
            output_filename = os.path.splitext(filename)[0] + '_modify.csv'
            output_path = os.path.join(input_folder, output_filename)
            df.to_csv(output_path, index=False)
            
            # Get the last row in the 'Date Time' column
            last_row = df['Date Time'].iloc[-1]
            
            # Check if the last row is not NaT
            if pd.notnull(last_row):
                
                # Extract the date and hour from the last row
                date_time = pd.to_datetime(last_row, dayfirst=True)
                date = date_time.strftime('%Y-%m-%d')
                hour = date_time.strftime('%H%M') 
            
            # Modify the file name based on the date and hour and SITENAME
            new_file_name = f"{sitename}_{date}_{hour}.csv"
            new_file_path = os.path.join(output_folder, new_file_name)

            # Replace with new file
            if os.path.exists(new_file_path):
                os.remove(new_file_path)
                
            # Rename the file
            os.rename(output_path, new_file_path)
            
    #=====================# Combine all CSV files in "SITE_updated" folder #==========================#
            
    # Create and enter new directory
    new_directory  = os.path.join(os.getcwd(), output_folder)
    os.chdir(new_directory)
    input_folder = os.getcwd()
    output_folder = os.getcwd()
    
    # Create an empty list to store the individual dataframes
    dfs = []

    # Iterate over the files in the input folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            input_path = os.path.join(input_folder, filename)
            
            # Read the CSV file
            df = pd.read_csv(input_path, parse_dates=["Date Time"])

            # Append the dataframe to the list
            dfs.append(df)

    # Concatenate the individual dataframes into one main dataframe
    main_df = pd.concat(dfs)
    main_df['Date Time'] = pd.to_datetime(main_df['Date Time'])

    # Generate a complete date range based on the minimum and maximum dates in the dataframe
    date_range = pd.date_range(start=main_df['Date Time'].min(), 
                               end=main_df['Date Time'].max(), freq='30 min')

    # Merge the dataframe with the complete date range to gap-fill the missing rows
    main_df = pd.merge(main_df, pd.DataFrame({'Date Time': date_range}), 
                       on='Date Time', how='outer')

    # 'Date Time' is already in datetime format
    # Change the display format of 'Date Time' column
    # main_df['Date Time'] = pd.to_datetime(main_df['Date Time'], format='%d-%m-%Y %H:%M')

    # Sort the dataframe by the date time column
    main_df = main_df.sort_values(by='Date Time')

    # Drop duplicate rows based on the 'Date Time' and 'Abs Pres (kPa) c:1 2' columns,
    # keeping the first occurrence
    main_df = main_df.drop_duplicates(subset=['Date Time', 'Abs Pres (kPa) c:1 2'], 
                                      keep='first')

    # Calculate the time difference between consecutive rows
    main_df['Time Difference'] = main_df['Date Time'].diff().dt.total_seconds() / 60

    # Drop rows with NaN 'Abs Pres (kPa) c:1 2' and 'Time Difference' less than 30
    main_df = main_df[~((main_df['Abs Pres (kPa) c:1 2'].isna()) & (main_df['Time Difference'] < 30))]  
    main_df = main_df.drop('Time Difference', axis=1)
    main_df.set_index('Date Time', inplace=True)
    main_df_sorted = main_df[['Abs Pres (kPa) c:1 2', 'EOF']].resample('30min', label='right', closed='right').mean()

    # Save the combined and sorted dataframe to a CSV file in the output folder
    output_path = os.path.join(output_folder, sitename+'_'+remark+'.xlsx')
    main_df_sorted.to_excel(output_path, index=True)

    ## Excel Cell Auto FIT============================================================================
    
    # Load the Excel workbook
    workbook = openpyxl.load_workbook(sitename+'_'+remark+'.xlsx')

    # Select the worksheet you want to autofit
    worksheet = workbook['Sheet1']  # Replace 'Sheet1' with the actual sheet name

    # Loop through all columns and autofit their widths
    for column in worksheet.columns:
        max_length = 0
        column_letter = openpyxl.utils.get_column_letter(column[0].column)  # Get the column letter

        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(cell.value)
            except:
                pass
        adjusted_width = (max_length + 2) * 1.7  # Adjust the multiplication factor as needed
        worksheet.column_dimensions[column_letter].width = adjusted_width

    # Save the modified workbook
    workbook.save(sitename+'_'+remark+'.xlsx')

    # Close the workbook
    workbook.close()
    
    # Print the combined and sorted dataframe
    #print(main_df_sorted.tail())
    print ("✅ "+sitename)

def start_processing():
    thread = Thread(target=process_directories_with_progress)
    thread.start()

# Function to process the selected directories
def process_directories():
    
    if not selected_directories:
        print("No directories selected.")
        return
    
    print("Processing Selected Directories:")
    for directory in selected_directories:
        process_directory(directory)  # Call the single directory processing function




def get_last_position():
    """Retrieve last window position from environment variables."""
    x = os.environ.get("WINDOW_POS_X")
    y = os.environ.get("WINDOW_POS_Y")
    if x is not None and y is not None:
        return int(x), int(y)
    return None, None

def save_position(x, y):
    """Save the window position to environment variables."""
    os.environ["WINDOW_POS_X"] = str(x)
    os.environ["WINDOW_POS_Y"] = str(y)

def on_closing():
    """Handle the window closing event."""
    x = root.winfo_x()
    y = root.winfo_y()
    save_position(x, y)
    root.destroy()

# Function to process the selected directories with a progress bar
def process_directories_with_progress():
    if not selected_directories:
        print("No directories selected.")
        return
    
    total = len(selected_directories)
    progress_bar['value'] = 0  # Reset progress bar
    progress_bar['maximum'] = total  # Set maximum value to the number of directories
    
    print("🔄️ Processing Selected Directories:")
    for i, directory in enumerate(selected_directories, start=1):
        process_directory(directory)  # Call the single directory processing function
        progress_bar['value'] = i  # Update progress bar
        root.update_idletasks()  # Force the GUI to update
    
    print("☑️  DONE.")

#############################
# Set up the main window
root = tk.Tk()
root.title("P14: Watertable Monitoring")

# Load last position
last_x, last_y = get_last_position()

# Create a frame to contain the checkboxes
main_frame = ttk.Frame(root, padding=5) # padding mean margin frame with content
main_frame.pack(fill="both", expand=True)

# Create checkboxes for each directory and arrange them in a grid grouped by site
check_vars = []
site_columns = {
    "MLM": [],
    "CMC": [],
    "SBW": [],
    "Marudi": [],
}

group_colors = {
    "MLM": "#FFDDC1",  # Light peach
    "CMC": "#D5E8D4",  # Light green
    "SBW": "#D4E4FF",  # Light blue
    "Marudi": "#FF7F7F"
}

# Group checkboxes by site
for label, dir_path in directories:
    if "MA" in label or "MB" in label or "MC" in label or "MD" in label:
        site_columns["MLM"].append((label,dir_path))
    elif "CB" in label or "CA" in label:
        site_columns["CMC"].append((label, dir_path))
    elif "NA12" in label or "NF" in label or "Q" in label or "NL2" in label or "SACS" in label or "NPK" in label :
        site_columns["SBW"].append((label, dir_path))
    elif "SSD" in label or "T5" in label or "TS7" in label:
        site_columns["Marudi"].append((label, dir_path))

for col_index, (site_name, entries) in enumerate(site_columns.items()):
    # Create a colored frame for the site group
    site_frame = tk.Frame(main_frame, bg=group_colors[site_name], padx=5, pady=5)
    site_frame.grid(row=0, column=col_index, padx=10, pady=5, sticky="nsew")

    # Add a label for the site group
    site_label = tk.Label(site_frame, text=site_name, bg=group_colors[site_name], font=('Arial', 15, 'bold'))
    site_label.pack(anchor="center", pady=(0,3))  # Align to the center

    # Create a list to store variables for this group
    group_vars = []

    # Create checkboxes for each entry in the site
    for label, dir_path in entries:
        var = tk.BooleanVar()
        check_vars.append(var)
        group_vars.append(var)
        
        # Bind the `update_selected_directories` function to the `BooleanVar`
        var.trace_add("write", update_selected_directories)
        
        cb = tk.Checkbutton(site_frame, text=label, variable=var, bg=group_colors[site_name], anchor="w")
        cb.pack(anchor="w")

    # Add "Check All" and "Uncheck All" buttons
    button_frame = tk.Frame(site_frame, bg=group_colors[site_name])
    button_frame.pack(fill="x", pady=(23, 0))

    def check_all(vars=group_vars):
        for var in vars:
            var.set(True)

    def uncheck_all(vars=group_vars):
        for var in vars:
            var.set(False)

    check_all_button = tk.Button(button_frame, text="Check All", command=check_all, bg=group_colors[site_name])
    check_all_button.pack(side="left", padx=3)

    uncheck_all_button = tk.Button(button_frame, text="Uncheck All", command=uncheck_all, bg=group_colors[site_name])
    uncheck_all_button.pack(side="left", padx=3)


# Add a button to process the selected directories
process_button = ttk.Button(root, text="Process Site", command=process_directories)
process_button.pack(pady=1)

# Create a progress bar widget
progress_bar = ttk.Progressbar(root, orient="horizontal", mode="determinate", length=500)
progress_bar.pack(pady=10)  # Add some padding for aesthetics

process_button.config(command=start_processing)

#####
# Update the window to get the size after packing the widgets
root.update_idletasks()  # Update the window to calculate size

# Get the window size
window_width = root.winfo_width()
window_height = root.winfo_height()

# Get the screen dimensions
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()

# Calculate x and y coordinates to center the window or use last known position
if last_x is not None and last_y is not None:
    x_position = last_x
    y_position = last_y
else:
    x_position = (screen_width // 2) - (window_width // 2)
    y_position = (screen_height // 2) - (window_height // 2)

# Set the geometry of the window
root.geometry(f"{window_width}x{window_height}+{x_position}+{y_position}")



# Bind the closing event to save position
root.protocol("WM_DELETE_WINDOW", on_closing)

# Run the Tkinter event loop
root.mainloop()

