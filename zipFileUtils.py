import glob
import os
import gzip
import shutil
import re

def get_most_recent_file_with_prefix(directory, prefix,extention):
    # Use glob to find files starting with the given prefix
    search_pattern = os.path.join(directory, f"{prefix}*.{extention}")
    files = glob.glob(search_pattern)
    
    if not files:
        return None  # No files matching the prefix
    
    # Get the most recent file based on modification time
    most_recent_file = max(files, key=os.path.getmtime)
    return most_recent_file

def unzip(file_path,output_csv_path):
    with gzip.open(file_path,"rt") as gz_file:
        with open(output_csv_path,"w",newline='') as csv_file:
            csv_file.write(gz_file.read())
    #print(f"CSV extracted and saved to : {output_csv_path}")

def extract_file_date_from_name(fullPath):
    basename:str = os.path.basename(fullPath)
    # Regex pattern to extract the date in YYYY-MM-DD format
    pattern = r"\d{4}-\d{2}-\d{2}"
    # Search for the date in the string
    match = re.search(pattern, basename)
    if match:
        date = match.group(0)
        return date
    else:
        return None

def unzip_recent_file_with_prefix(directory, prefix,extention,outputDir)->tuple[str,str]:
    most_recent_file = get_most_recent_file_with_prefix(directory, prefix,extention)
    
    if most_recent_file:
        outputPath = os.path.join(outputDir, os.path.basename(most_recent_file).replace(extention,'csv'))
        unzip(most_recent_file,outputPath)
        #print(f"The most recent file is: {most_recent_file}")
        return (outputPath,most_recent_file)
    else:
        #print("No files found with the given prefix.")
        return (None,None)

def delete_file(file_path):
    try:
        os.remove(file_path)
        print(f"File '{file_path}' has been deleted successfully.")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except PermissionError:
        print(f"Permission denied: Unable to delete '{file_path}'.")
    except Exception as e:
        print(f"An error occurred while trying to delete the file: {e}")

def move_processed_file(file_path,destination):
    if os.path.exists(file_path):
        try:
            # Move the file
            shutil.move(file_path, destination)
            #print(f"File moved to {destination}")
        except Exception as e:
            print(f"Error moving file: {e}")
    else:
        print(f"Source file {file_path} does not exist.")