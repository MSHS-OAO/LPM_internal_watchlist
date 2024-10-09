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
#Pay Cycle from DB
oao_con <- dbConnect(odbc(), "OAO Cloud DB Production")
dates <- tbl(oao_con, "LPM_MAPPING_PAYCYCLE") %>%
  rename(
    DATE = PAYCYCLE_DATE,
    START.DATE = PP_START_DATE,
    END.DATE = PP_END_DATE,
    PREMIER.DISTRIBUTION = PREMIER_DISTRIBUTION
  ) %>%
  collect()

#Table of distribution dates
dist_dates <- dates %>%
  select(END.DATE, PREMIER.DISTRIBUTION) %>%
  distinct() %>%
  drop_na() %>%
  arrange(END.DATE) %>%
  #filter only on distribution end dates
  filter(PREMIER.DISTRIBUTION %in% c(TRUE, 1),
         #filter 3 weeks from run date (21 days) for data collection lag before run date
         END.DATE < as.POSIXct(Sys.Date() - 21))
#Table of non-distribution dates
non_dist_dates <- dates %>%
  select(END.DATE, PREMIER.DISTRIBUTION) %>%
  distinct() %>%
  drop_na() %>%
  arrange(END.DATE) %>%
  #filter only on distribution end dates
  filter(PREMIER.DISTRIBUTION %in% c(FALSE, 0),
         #filter 3 weeks from run date (21 days) for data collection lag before run date
         END.DATE < as.POSIXct(Sys.Date() - 21))
#Selecting current and previous distribution dates
distribution <- format(dist_dates$END.DATE[nrow(dist_dates)],"%m/%d/%Y")
previous_distribution <- format(dist_dates$END.DATE[nrow(dist_dates)-1],"%m/%d/%Y")
#Confirming distribution dates
cat("Current distribution is", distribution,
    "\nPrevious distribution is", previous_distribution)
answer <- select.list(choices = c("Yes", "No"),
                      preselect = "Yes",
                      multiple = F,
                      title = "Correct distribution?",
                      graphics = T)
if (answer == "No") {
  distribution <- select.list(choices =
                                format(sort.POSIXlt(dist_dates$END.DATE, decreasing = T),
                                       "%m/%d/%Y"),
                              multiple = F,
                              title = "Select current distribution",
                              graphics = T)
  which(distribution == format(dist_dates$END.DATE, "%m/%d/%Y"))
  previous_distribution <- format(dist_dates$END.DATE[which(distribution == format(dist_dates$END.DATE, "%m/%d/%Y"))-1],"%m/%d/%Y")
}


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

# Create a copy of the original data to modify
cleaned_data <- data

# Remove $ and , from all data while keeping column names the same
cleaned_data[] <- lapply(cleaned_data, function(x) {
  gsub("[$,]", "", x)  # Remove $ and , from each element
})

# Replace the original data with the cleaned data
data <- cleaned_data

# Data References ---------------------------------------------------------
# (aka Mapping Tables)
# Files that need to be imported for mappings and look-up tables.
# (This section may be combined into the Data Import section.)

# Creation of Functions --------------------------------------------------
# These are functions that will be commonly used within the rest of the script.
# It might make sense to keep these files in a separate file that is sourced
# in the "Source Global Functions" section above.
library(dplyr)

#function returns the values of the last 3, 13 and 26 pay periods
old_calculate_metric_summary <- function(data, metric, summary_type = "mean") {
  # Trim whitespace in column names
  colnames(data) <- trimws(colnames(data))
  
  # Remove columns with NA or empty string names
  valid_columns <- !is.na(colnames(data)) & colnames(data) != ""
  data <- data[, valid_columns]
  
  # Find columns that contain the specified metric at the end of the column name
  metric_columns <- names(data)[grepl(paste0(" ", metric, "$"), names(data))]
  
  # Check if any metric columns were found
  if (length(metric_columns) == 0) {
    stop("The specified metric does not exist in the dataframe.")
  }
  data[data == ""] <- NA
  n_periods <- c(3, 13, 26)
  summaries <- list()
  
  # Group by Department
  data_grouped <- data %>%
    group_by(Department)
  
  for (n in n_periods) {
    # Extract the last n columns for the given metric
    selected_columns <- tail(metric_columns, n)
    
    # Convert the columns to numeric (cleaning $ and commas)
    data_grouped[selected_columns] <- lapply(data_grouped[selected_columns], function(x) {
      as.numeric(gsub("[\\$,]", "", x))  # Remove $ and commas for numeric conversion
    })
    
    # Calculate the summary (mean or median) for each department and each period, skipping NA
    summary_values <- data_grouped %>%
      summarise(across(all_of(selected_columns), 
                       function(x) {
                         if (summary_type == "mean") {
                           mean(x, na.rm = TRUE)
                         } else if (summary_type == "median") {
                           median(x, na.rm = TRUE)
                         } else {
                           stop("Invalid summary type. Choose 'mean' or 'median'.")
                         }
                       }, 
                       .names = "{.col}_{.fn}")) %>%
      ungroup()
    
    # Store the result in the list with appropriate labeling
    summaries[[paste(summary_type, n, "Periods")]] <- summary_values
  }
  
  # Combine the results into a dataframe
  return(bind_rows(summaries))
}

# Example usage for averages
metric_to_calculate <- "Actual Measure Amount"
averages_result <- old_calculate_metric_summary(data, metric_to_calculate, summary_type = "mean")
print(averages_result)

# Example usage for medians
medians_result <- old_calculate_metric_summary(data, metric_to_calculate, summary_type = "median")
print(medians_result)

library(dplyr)
#Function to calculate average of last 3, 13 and 26 pay periods. User specifies the metric.
#Add in department ID/code. Add with report builder if possible
calculate_metric_summary <- function(data, metric, summary_type = "mean") {
  # Trim whitespace in column names
  colnames(data) <- trimws(colnames(data))
  
  # Remove columns with NA or empty string names
  valid_columns <- !is.na(colnames(data)) & colnames(data) != ""
  data <- data[, valid_columns]
  
  # Find columns that contain the specified metric at the end of the column name
  metric_columns <- names(data)[grepl(paste0(" ", metric, "$"), names(data))]
  
  # Check if any metric columns were found
  if (length(metric_columns) == 0) {
    stop("The specified metric does not exist in the dataframe.")
  }
  
  # Replace blanks with NA in the dataset
  data[data == ""] <- NA
  
  # Define periods to calculate (last 3, 13, and 26 periods)
  n_periods <- c(3, 13, 26)
  
  # Group by Department
  data_grouped <- data %>% group_by(Department)
  
  # Calculate summaries for each department based on the chosen summary type
  summary_values <- data_grouped %>% summarise(
    Average_Last_3_Periods = if (summary_type == "mean") {
      mean(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3)))), na.rm = TRUE)
    } else if (summary_type == "median") {
      median(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3)))), na.rm = TRUE)
    },
    Average_Last_13_Periods = if (summary_type == "mean") {
      mean(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13)))), na.rm = TRUE)
    } else if (summary_type == "median") {
      median(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13)))), na.rm = TRUE)
    },
    Average_Last_26_Periods = if (summary_type == "mean") {
      mean(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26)))), na.rm = TRUE)
    } else if (summary_type == "median") {
      median(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26)))), na.rm = TRUE)
    },
    .groups = "drop"
  )
  
  return(summary_values)
}

# Example usage for averages
metric_to_calculate <- "Overtime Labor Expense"
averages_result_V2 <- calculate_metric_summary(data, metric_to_calculate, summary_type = "mean")
print(averages_result)

# Example usage for medians
medians_result_V2 <- calculate_metric_summary(data, metric_to_calculate, summary_type = "median")
print(medians_result)

OT_hours_averages_result <- calculate_metric_summary_averages(data, metric_to_calculate, summary_type = "mean")
OT_LE_averages_result <- calculate_metric_summary_averages(data, metric_to_calculate, summary_type = "mean")
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
