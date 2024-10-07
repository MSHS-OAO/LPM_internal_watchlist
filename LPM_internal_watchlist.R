# Libraries ---------------------------------------------------------------
# Common Packages
library(here)
library(rmarkdown)
library(shiny)
library(tidyverse)
library(rstudioapi)
library(DBI)
library(odbc)
library(readr)
library(dplyr)

# Source Global Functions -------------------------------------------------
# Source scripts that house global functions used within this script.
# Global functions would include functions that are used regularly
# throughout the rest of the script and should make code more readable.
source()


# Assigning Directory(ies) ------------------------------------------------
# Define variables for frequently used root directories or full directories.
# (This section could be combined with the Constants section.)
# Reminder: avoid using setwd()
#Read in Files-----------------------------------------------------------------
dir_testing <- paste0("/SharedDrive/deans/Presidents/SixSigma/MSHS Productivity/",
                        "Productivity/Analysis/Labor Metric Expansion and In-house Watchlist/Source Data/")
setwd(dir_testing)

## Shared Drive Path (Generic) --------------------------------------------
sdp <- paste0("//researchsan02b/shr2/deans/Presidents")
J_drive <- paste0("//researchsan02b/shr2/deans/Presidents")

## J-drive Automatic Check ------------------------------------------------
# Alternative mapping of the Windows Shared Drive using the drive letter
# and an if-else check.
# This code helps when the shared folder is mapped differently for different
# users.
# This code tests whether a user has the J drive mapped to Presidents or
# deans
if ("Presidents" %in% list.files("J://")) {
  user_directory <- "J:/Presidents/"
} else {
  user_directory <- "J:/deans/Presidents/"
}


# Here is the final path
user_path <- paste0(user_directory, project_path,"*.*")


# Constants ---------------------------------------------------------------
# Define constants that will be used throughout the code. These are the
# variables that are calculated here and not changed in the rest of the code.


# Data Import -------------------------------------------------------------
# Importing data that is needed in the code whether it’s from the shared drive
# or OneDrive or some other location.
# Read in raw data 
data <- read.csv(file.choose(), check.names = FALSE)

#Checking column headers
print(head(data))

# Get the original column headers
original_headers <- colnames(data)

# Concatenate the original headers with the first row
new_headers <- paste(original_headers, data[1, ], sep = " ")

# Set the new headers as the column names
colnames(data) <- new_headers

# Remove the first row since it's now part of the headers
data <- data[-1, ]

# Data References ---------------------------------------------------------
# (aka Mapping Tables)
# Files that need to be imported for mappings and look-up tables.
# (This section may be combined into the Data Import section.)


# Creation of Functions --------------------------------------------------
# These are functions that will be commonly used within the rest of the script.
# It might make sense to keep these files in a separate file that is sourced
# in the "Source Global Functions" section above.

library(dplyr)

# Average Function
calculate_metric_averages <- function(data, metric) {
  #trimming whitespace
  colnames(data) <- trimws(colnames(data))
  
  #Check for columns with NA or empty string names and removes them
  valid_columns <- !is.na(colnames(data)) & colnames(data) != ""
  data <- data[, valid_columns]
  
  # Find columns that contain the specified metric
  metric_columns <- names(data)[grepl(metric, names(data))]
  
  # Check if any metric columns were found
  if (length(metric_columns) == 0) {
    stop("The specified metric does not exist in the dataframe.")
  }
  
  # Replace blanks with NA in the dataset
  data[data == ""] <- NA
  
  # Select the last 3, 13, and 26 periods
  n_periods <- c(3, 13, 26)
  
  # Create a list to hold averages for each period
  averages <- list()
  
  for (n in n_periods) {
    # Extract the last 'n' columns for the given metric
    selected_columns <- tail(metric_columns, n) #Add in date component
    
    # Convert the columns to numeric (cleaning $ and commas)
    data[selected_columns] <- lapply(data[selected_columns], function(x) {
      as.numeric(gsub("[\\$,]", "", x))  # Remove $ and commas for numeric conversion
    })
    
    # Calculate the mean for each period, skipping NA
    mean_values <- data %>%
      summarise(across(all_of(selected_columns), ~ mean(.x, na.rm = TRUE)))
    
    # Store the result in the list
    averages[[paste("Average", n, "Periods")]] <- mean_values
  }
  
  # Combine the results into a dataframe
  return(bind_rows(averages))
}

# Example. User specifies metric. Can modify # of periods.
metric_to_calculate <- "Actual Measure Amount"
averages_result <- calculate_metric_averages(data, metric_to_calculate)

print(averages_result)
#-------------Testing-------------------
# Median Function
calculate_metric_medians <- function(data, metric) {
  colnames(data) <- trimws(colnames(data))
  
  valid_columns <- !is.na(colnames(data)) & colnames(data) != ""
  data <- data[, valid_columns]
  
  metric_columns <- names(data)[grepl(metric, names(data))]
  
  if (length(metric_columns) == 0) {
    stop("The specified metric does not exist in the dataframe.")
  }
  
  data[data == ""] <- NA
  n_periods <- c(3, 13, 26)
  
  medians <- list()
  
  for (n in n_periods) {
    selected_columns <- tail(metric_columns, n)
    
    data[selected_columns] <- lapply(data[selected_columns], function(x) {
      as.numeric(gsub("[\\$,]", "", x))  # Remove $ and commas for numeric conversion (Can do this in pre-processing?)
    })
    
    median_values <- data %>%
      summarise(across(all_of(selected_columns), median, na.rm = TRUE))
    
    medians[[paste("Median", n, "Periods")]] <- median_values
  }
  
  return(as.data.frame(medians))
}

#example
metric_to_calculate <- "Overtime Hours"
medians_result <- calculate_metric_medians(data, metric_to_calculate)

print(medians_result)

#---------Productivity Index Rolling Average Function----------------
library(zoo)  # for rollapply


# Function to clean column names
clean_column_names <- function(data) {
  colnames(data) <- make.names(colnames(data), unique = TRUE)  # Makes valid R names
  colnames(data) <- gsub("\\.", " ", colnames(data))  # Replace dots with spaces
  return(data)
}
# Data Pre-processing -----------------------------------------------------
# Cleaning raw data and ensuring that all values are accounted for such as
# blanks and NA. As well as excluding data that may not be used or needed. This
# section can be split into multiple ones based on the data pre-processing
# needed.
# One of the first steps could be to perform initial checks to make sure data is
# in the correct format.  This might also be done as soon as the data is
# imported.


# Data Formatting ---------------------------------------------------------
# How the data will look during the output of the script.
# For example, if you have a data table that needs the numbers to show up as
# green or red depending on whether they meet a certain threshold.


# Quality Checks ----------------------------------------------------------
# Checks that are performed on the output to confirm data consistency and
# expected outputs.


# Visualization -----------------------------------------------------------
# How the data will be plotted or how the data table will look including axis
# titles, scales, and color schemes of graphs or data tables.
# (This section may be combined with the Data Formatting section.)


# File Saving -------------------------------------------------------------
# Writing files or data for storage


# Script End --------------------------------------------------------------
