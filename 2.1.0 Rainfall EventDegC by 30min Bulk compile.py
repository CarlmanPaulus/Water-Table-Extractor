import pandas as pd
import os
import numpy as np
from tqdm import tqdm
import time
import pandas as pd
import pandas as pd
from openpyxl.styles import PatternFill
import datetime as dt
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
import tkinter as tk
from tkinter import ttk
import time  # For simulating a long process
import logging
from threading import Thread

remark="Combined"         # final file all data combined
threshold_time_diff = 60  # Second between tipping will filter out considered as not rain "optional value"
QC_minute = 5  

date_format = '%d/%m/%y %H:%M:%S'

directories = [    
    #MLM
    ("MA1_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\MLM\MA\MA1_excel"),
    ("MA2_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\MLM\MA\MA2_excel"),
    ("MB_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\MLM\MB\MB_excel"),
    ("MC1_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\MLM\MC\MC1_excel"),
    ("MC2_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\MLM\MC\MC2_excel"),
    ("MD1_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\MLM\MD\MD1_excel"),
    ("MD2_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\MLM\MD\MD2_excel"),

    #CMC
    ("CA_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\CMC\HOBO data (Excel)\CA"),
    ("CB1_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\CMC\HOBO data (Excel)\CB1"),
    ("CB2_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\CMC\HOBO data (Excel)\CB2"),

    # SBW
    ("NL2-43_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\SBW\Hobo data (Excel)\NL2_43"),
    ("NL2-22_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\SBW\Hobo data (Excel)\NL2_22"),
    ("SBW_pt",r"C:\Users\agust\OneDrive - Sarawak Government\P14 Water Table Monitoring\PT\SBW\Hobo data (Excel)\SBW"),
              ]

def process_directory(directory_path):
    
           # frist and last minute to QC "optional value"
    
    os.chdir(directory_path)
    input_folder = os.getcwd()                # Get the current working directory 
    sitename = os.path.basename(input_folder) # Return the last component of the path as sitename

    #Creat new folder inside current folder
    output_folder = sitename+"_updated" 

    # Check if "SITE_updated" folder is existed
    # IF not creat new
    if not os.path.exists(output_folder):
        os.makedirs(output_folder) #create new directory/folder_Updated

    # Loop through all files in the input folder
    for filename in os.listdir(input_folder): # returning list of file in the current 
                                              # working directory (input_folder) as "filename"
                                              # including SITE_updated folder
        if filename.endswith('.xlsx'): # True only for file with .csv extension
            input_path = os.path.join(input_folder, filename) #assign csv files into variable 

            # Read each the CSV file
            # parse_dates to set "Date Time" column in date time data types (NaT)
            # dayfrist = True to read date with day-month-year
            # df = pd.read_excel(input_path, parse_dates=["Date Time"])
            # df = pd.read_excel(input_path, parse_dates=["Date Time"])
            df = pd.read_excel(input_path)
            df['Date Time'] = pd.to_datetime(df['Date Time'], format=date_format)

            # Rename headers
            df = df.rename(columns={
                "Date Time": "DateTime",
                "Temp (°C) c:1": "DegC",
                "Temp (°C) c:2":"DegC",
                "Event (units) c:3": "Event",
                "Event (units)": "Event"
            })
            
            df['TimeDiff'] = df['DateTime'].diff().dt.total_seconds()
            
            # TimeDiff created new column
            # diff() to calculate time different with previous row
            
            # Check if 'DegC' column exists, if not, add it with NaN values
            if 'DegC' not in df.columns:
                df['DegC'] = pd.NA  # Adding the column with NaN values
            
            # Start of QC-----------------------------------------------------------------------------
            
            if 'Coupler Attached' in df.columns:
                # Check if 'Host Connected' column does not exist
                if 'Host Connected' not in df.columns:
                    # Find the index of the first row where 'Coupler Attached' is not NaN
                    index_to_cut = df[df['Coupler Attached'].notna()].index

                    if not index_to_cut.empty:
                        first_index = index_to_cut[0]
                        # Check if there are more than 48 rows (~1day) after the first 'Coupler Attached' not NaN row
                        if len(df) - first_index < 48:
                        # Keep only the rows up to the first 'Coupler Attached' not NaN row (inclusive)
                            df = df.loc[:first_index]
            
            start_time = df['DateTime'].min() #start_time will store min/first value of time in dataframe
            first_5min = start_time + timedelta(minutes= QC_minute) # calculate first 5 minutes
            # NEW dataframe for first five minutes
            df_range_start = df.loc[(df['DateTime'] >= start_time) & (df['DateTime'] <= first_5min)]
            
            # Convert any 0 value in 'Event' column to NaN
            df_copy_start = df_range_start.copy()
            df_copy_start['Event'] = df_copy_start['Event'].replace(0, np.nan)
            
            end_time = df['DateTime'].max() #end_time will store max/last value of time in dataframe
            last_5min = end_time - timedelta(minutes = QC_minute) # calculate first 5 minutes 
            # NEW dataframe for last five minutes
            df_range_end = df.loc[(df['DateTime'] >= last_5min) & (df['DateTime'] <= end_time)]
            
            df_QC = pd.concat([df_copy_start, df_range_end])
            
            df_QC.loc[df_QC['TimeDiff'].isna(), 'TimeDiff'] = 1000
            
            df_QC1 = df_QC[(df_QC['TimeDiff'].shift(-1) > threshold_time_diff)|(df_QC.index == df_QC.index[-1])] # QC1
            df_QC2 = df_QC1[(df_QC1.index == df_QC1.index[-1]) | (df_QC1['TimeDiff'] > threshold_time_diff)]     # QC2
            
            if df_QC2['TimeDiff'].iloc[-1] < 120: #2 minutes
                df_QC2.at[df_QC2.index[-1], 'Event'] = np.nan
            
            df_not_QC = df.loc[((df['DateTime'] > first_5min) & (df['DateTime'] < last_5min))]
            
            df_combined = pd.concat([df_QC2, df_not_QC])
            
            # Sort the rows of the combined DataFrame based on the DateTime column
            # f_sorted = df_combined.sort_values('DateTime')
            df = df_combined.sort_values(by='DateTime')

            # Initialize excluded_df to handle the case where no frames are processed            
            excluded_df = pd.DataFrame()
            
            # Check if 'Host Connected' column exists before processing
            if 'Host Connected' in df.columns:
                # Find all indices where 'Host Connected' is not NaN
                indices_to_cut = df[df['Host Connected'].notna()].index

                # Initialize an empty list to store DataFrames
                frames = []

                for index in indices_to_cut:
                    # Get the timestamp of the current 'Host Connected' not NaN row
                    timestamp = df.loc[index, 'DateTime']

                    # Define the time window
                    start_time = timestamp - pd.Timedelta(minutes=5)
                    end_time = timestamp + pd.Timedelta(minutes=5)

                    # Filter the DataFrame to include only rows within the time window
                    filtered_df = df[(df['DateTime'] >= start_time) & (df['DateTime'] <= end_time)]
                    filtered_df2 = filtered_df.copy()
                    filtered_df2['TimeDiff2'] = filtered_df2['DateTime'].diff().dt.total_seconds()
                    filtered_df2.loc[filtered_df2['TimeDiff2'].isna(), 'TimeDiff2'] = 1000
                    
                    # Append the filtered DataFrame to the list
                    frames.append(filtered_df2)

                    # Check if there are any DataFrames to concatenate
                if frames:
                    # Concatenate all the filtered DataFrames
                    combined_df = pd.concat(frames).drop_duplicates().reset_index(drop=True)

                    # Merge original df with combined_df to find the excluded rows
                    merged_df = df.merge(combined_df, how='outer', indicator=True)

                    # Extract rows that are only in the original df
                    excluded_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge']).reset_index(drop=True)

                    # Quality control: Find rows with large time differences in the combined DataFrame
                    # df_QC_Host1 = combined_df[(combined_df['TimeDiff'].shift(-1) > threshold_time_diff) | (combined_df.index == combined_df.index[-1])]
                    df_QC_Host1 = combined_df[(combined_df['TimeDiff2'].shift(-1) > threshold_time_diff) | (combined_df.index == combined_df.index[-1])]
                    df_QC_Host2 = df_QC_Host1[(df_QC_Host1['TimeDiff2'] > threshold_time_diff)]
                    # df_QC_Host2 = df_QC_Host1[(df_QC_Host1['TimeDiff'] > threshold_time_diff)]

                    # Combine the quality-controlled rows with the excluded rows
                    df = pd.concat([df_QC_Host2, excluded_df]).drop_duplicates().reset_index(drop=True)
                    df = df.sort_values(by='DateTime')
                else:
                    # Handle the case where no frames are available
                    df = df.copy()  # Optionally, you can just keep the original DataFrame
                        
            # Columns to check
            columns_to_check = ['Host Connected', 'Coupler Attached', 'Stopped', 'End Of File','Coupler Detached']
            
            # Construct the condition dynamically based on existing columns
            condition = False
            for column in columns_to_check:
                if column in df.columns:
                    condition |= df[column].notna()
                    
            # Apply the condition if any column is present
            if condition is not False:
                df = df.drop(df[(condition) & (df['DegC'].isna())].index)
                    
            # Create a list of column to delete
            columns_to_delete = ['Coupler Attached',
                                 'Coupler Detached',
                                 'Stopped','End Of File',
                                 'Abs Pres Barom. (kPa) c:1 2',
                                 'Unnamed: 2',
                                 'Unnamed: 5',
                                 'Bad Battery']


            # Create empty list named column_exist
            # List from columns_to_delete will compared column by column with column in data frame df
            # If TRUE then append to list in column_exist variable 
            columns_exist = [col for col in columns_to_delete if col in df.columns]

            # All column name that in column_exist will drop from data frame
            # "axis = 1" mean drop column
            if columns_exist:
                df = df.drop(columns_exist, axis=1)


            


            # Filter Event with same count number / keep only Event value not same
            # ffill mean foward fill, bring value from previous row to next row
            # df = df[df['Event'].shift(1).fillna(method='ffill') != df['Event']]
            df = df[df['Event'].shift(1).ffill() != df['Event']]


            df.set_index('DateTime', inplace=True)


            # End of QC-----------------------------------------------------------------------------



            ## RESAMPLING Event and DegC
            # Resample Event and calculate count
            #Event_count = df_sorted['Event'].resample('30T').count()*0.2
            # Event_count = df['Event'].resample('30T', label='right', closed='right').count() * 0.2 # sum at the end of 30min interval
            Event_count = df['Event'].resample('30min', label='right', closed='right').count() * 0.2

            # Resample DegC and calculate mean 
            # DegC_mean = df['DegC'].resample('30T', label='right', closed='right').mean()
            DegC_mean = df['DegC'].resample('30min', label='right', closed='right').mean()

            # Create a new DataFrame with both results
            df_result = pd.concat([DegC_mean, Event_count], axis=1)

            
            first_row_index = df_result.index[0]

            # Check if the first row is in datetime data type
            if pd.notnull(first_row_index):
                date_time = pd.to_datetime (first_row_index,dayfirst=True) #assign date_time to store last row date and time
                date = date_time.strftime('%Y-%m-%d') # extract the date from first row
                time = date_time.strftime('%H%M')     # extract the time from first row
                
            df_result.loc[first_row_index, 'EOF'] = -1111
            

            # Get the last row in the 'Date Time' column
            last_row_index = df_result.index[-1]

            # Check if the last row is in date time data type NaT
            if pd.notnull(last_row_index):
                date_time = pd.to_datetime (last_row_index, dayfirst=True) # assign date_time to store last row date and time
                date = date_time.strftime('%Y-%m-%d')                      # extract the date from last row
                hour = date_time.strftime('%H%M')                          # extract the time from last row

            # assign value -9999 to last row "EOF" column  for file mark
            df_result.loc[last_row_index, 'EOF'] = -9999

            #os.path.splitext(filename) will divide whole file name into root(name of file) [0] and extension [1]
            output_filename = os.path.splitext(filename)[0] + '_modify.csv'
            output_path = os.path.join(input_folder,output_filename) # Create and join output path with file name as print
            df_result.to_csv(output_path, index=True)                # Save the dataframe df_result to new CSV file
            df_result.dtypes

            # Modify the file name based on the date and hour and SITENAME
            new_file_name = f"{sitename}_{date}_{hour}.csv"
            new_file_path = os.path.join(output_folder, new_file_name)

            # Remove and replace all processed csv file in SITE_updated folder
            if os.path.exists(new_file_path):
                os.remove(new_file_path)

            # Rename based on date and time, and move renamed file into "_updated" folder
            # 'Output_path' old file with name, 'new_file_path' new file with name
            os.rename(output_path, new_file_path)

    #=====================# Combine all CSV files in "SITE_updated" folder #==========================#

    # Creat new directory to "SITE_updated" folder
    new_directory  = os.path.join(os.getcwd(),output_folder)

    # Chnage to new directory to "SITE_updated" folder
    os.chdir(new_directory)

    # Required since working directory change
    # Update the CWD
    input_folder = os.getcwd()
    output_folder = os.getcwd()

    # Create an empty list to store the individual dataframes
    dfs = []

    # Iterate over the files in the input folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            input_path = os.path.join(input_folder, filename)

            # Read the CSV file
            df = pd.read_csv(input_path, parse_dates=["DateTime"])

            # Append the dataframe to the list
            dfs.append(df)

    # Concatenate the individual dataframes into one main dataframe
    main_df = pd.concat(dfs,ignore_index=True)
    main_df['DateTime'] = pd.to_datetime(main_df['DateTime'], errors='coerce')
    main_df.sort_values(["DateTime"],ignore_index=True,inplace=True)

    # Generate a complete date range based on the minimum and maximum dates in the dataframe
    date_range = pd.date_range(start=main_df['DateTime'].min(), 
                                end=main_df['DateTime'].max(), freq='30 min')

    # Create new data frame for complete based on first and last datetime in combined df
    complete_date = pd.DataFrame({"DateTime":date_range})
    #complete_date.to_excel("TESTconcat.xlsx.")

    # Merge the dataframe with the complete date range to gap-fill the missing rows
    main_df2 = pd.concat([main_df, complete_date])


    # Find duplicate rows based on 'DateTime' in newly merged df "main_df2"
    duplicate_rows = main_df2.duplicated(subset=["DateTime"], keep=False)


    # Create dataframe that include only the duplicate rows
    duplicates_df = main_df2[duplicate_rows]

    # Group all duplicates by 'DateTime' 
    # "Aggregate" using one or more operations over the specified axis
    # Use aggregate to sum all value in "Event" column for same group 
    # keep "first" value of the 'EOF' column for each group, but no problem since other have NAN
    sum_duplicates = duplicates_df.groupby('DateTime').agg({'DegC':'mean','Event': 'first', 'EOF': 'mean'})
    

    # Create new data frame with no duplicates included named "no_duplicate"
    no_duplicate = main_df2.drop_duplicates(subset=["DateTime"],keep=False,ignore_index=True)
    

    # Merge data frame with sum_duplicate and no_duplicate
    # Based on column "DateTime","Event","EOF"
    # 'how = outer' mean merge all data from both dataframe 
    duplicate_merge = pd.merge(no_duplicate, sum_duplicates, on=["DateTime", "DegC", "Event", "EOF"], how="outer")

    # print(duplicate_merge['DateTime'].apply(type).value_counts())
    
    # duplicate_merge = duplicate_merge.sort_values(by='DateTime').reset_index(drop=True)
    #duplicate_merge = pd.merge(no_duplicate, sum_duplicates, on=["DateTime","DegC","Event","EOF"],how="outer")
    

    # Sort the dataframe by the date time column
    duplicate_merge = duplicate_merge.sort_values(by='DateTime',ignore_index=False)


    # Rename headers
    duplicate_merge = duplicate_merge.rename(columns={
                        "Event": "Rain"})

    # Save the combined and sorted dataframe to a CSV file in the output folder
    output_path = os.path.join(output_folder, sitename+'_'+remark+'.xlsx')
    duplicate_merge.to_excel(output_path, index=False)


    print ("✅ " + sitename )

# Function to process the selected directories
def process_directories():
    
    if not selected_directories:
        print("No directories selected.")
        return
    
    print("Processing Selected Directories:")
    for directory in selected_directories:
        process_directory(directory)  # Call the single directory processing function

selected_directories = []    
# Function to get selected directories
def get_selected_directories():
    global selected_directories  # Declare the variable as global to modify it
    selected_directories = [dir_path for var, dir_path in zip(check_vars, [d[1] for d in directories]) if var.get()]
    
    # Print the selected directories for feedback
    print("Selected Directories:")
    for directory in selected_directories:
        print(directory)

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

def start_processing():
    thread = Thread(target=process_directories_with_progress)
    thread.start()

# Function to process the selected directories with a progress bar
def process_directories_with_progress():
    if not selected_directories:
        print("No directories selected.")
        return
    
    total = len(selected_directories)
    progress_bar['value'] = 0  # Reset progress bar
    progress_bar['maximum'] = total  # Set maximum value to the number of directories
    
    print("Processing Selected Directories:")
    for i, directory in enumerate(selected_directories, start=1):
        process_directory(directory)  # Call the single directory processing function
        progress_bar['value'] = i  # Update progress bar
        root.update_idletasks()  # Force the GUI to update
    
    print("All directories processed.")
    
## Set up main window
root = tk.Tk()
root.title("P14: Rainfall")

# Load last position
last_x, last_y = get_last_position()

# Create a frame to contain the checkboxes
main_frame = ttk.Frame(root, padding=10)
main_frame.pack(fill="both", expand=True)

# Create checkboxes for each directory and arrange them in a grid grouped by site
check_vars = []
site_columns = {
    "MLM": [],
    "CMC": [],
    "SBW": [],
    # "Marudi": [],
}

group_colors = {
    "MLM": "#FFDDC1",  # Light peach
    "CMC": "#D5E8D4",  # Light green
    "SBW": "#D4E4FF",  # Light blue
}

# Group checkboxes by site
for label, dir_path in directories:
    if "MA" in label or "MB" in label or "MC" in label or "MD" in label:
        site_columns["MLM"].append((label,dir_path))
    elif "CA" in label or "CB" in label:
        site_columns["CMC"].append((label, dir_path))
    elif "SBW" in label or "NL2" in label or "Ba" in label:
        site_columns["SBW"].append((label, dir_path))

for col_index, (site_name, entries) in enumerate(site_columns.items()):
    # Create a colored frame for the site group
    site_frame = tk.Frame(main_frame, bg=group_colors[site_name], padx=10, pady=10)
    site_frame.grid(row=0, column=col_index, padx=10, pady=5, sticky="nsew")

    # Add a label for the site group
    site_label = tk.Label(site_frame, text=site_name, bg=group_colors[site_name], font=('Arial', 12, 'bold'))
    site_label.pack(anchor="w", pady=(0, 5))  # Align to the left

    # Create a list to store variables for this group
    group_vars = []

    # Create checkboxes for each entry in the site
    for label, dir_path in entries:
        var = tk.BooleanVar()
        check_vars.append(var)
        group_vars.append(var)
        cb = tk.Checkbutton(site_frame, text=label, variable=var, bg=group_colors[site_name], anchor="w")
        cb.pack(anchor="w")

    # Add "Check All" and "Uncheck All" buttons
    button_frame = tk.Frame(site_frame, bg=group_colors[site_name])
    button_frame.pack(fill="x", pady=(5, 0))

    def check_all(vars=group_vars):
        for var in vars:
            var.set(True)

    def uncheck_all(vars=group_vars):
        for var in vars:
            var.set(False)

    check_all_button = tk.Button(button_frame, text="Check All", command=check_all, bg=group_colors[site_name])
    check_all_button.pack(side="left", padx=5)

    uncheck_all_button = tk.Button(button_frame, text="Uncheck All", command=uncheck_all, bg=group_colors[site_name])
    uncheck_all_button.pack(side="left", padx=5)


# Add a button to confirm the selection
run_button = ttk.Button(root, text="Select Site", command=get_selected_directories)
run_button.pack(pady=5)
# Add a button to process the selected directories
process_button = ttk.Button(root, text="Process Site", command=process_directories)
process_button.pack(pady=5)
 
# Create a progress bar widget
progress_bar = ttk.Progressbar(root, orient="horizontal", mode="determinate", length=300)
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
