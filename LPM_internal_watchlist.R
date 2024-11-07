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
library(purrr)
library(tidyr)

# Source Global Functions -------------------------------------------------
source()

# Assigning Directory(ies) ------------------------------------------------
# Define variables for frequently used root directories or full directories.
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
  gsub("[$,%]", "", x)
})

# Replace the original data with the cleaned data
data <- cleaned_data
# Data References ---------------------------------------------------------
# (aka Mapping Tables)
# Files that need to be imported for mappings and look-up tables.
# (This section may be combined into the Data Import section.)

# Creation of Functions --------------------------------------------------
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
  data_grouped <- data %>% group_by(`Department CODE`)
  
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

# Correlation Coefficient (Volume to Worked Hours)
# Function to calculate correlation using individual pay periods
calculate_metric_correlation <- function(data, metric1, metric2) {
  
  colnames(data) <- trimws(colnames(data))
  
  # Find columns for both metrics (ending with the metric name)
  metric1_columns <- names(data)[grepl(paste0(" ", metric1, "$"), names(data))]
  metric2_columns <- names(data)[grepl(paste0(" ", metric2, "$"), names(data))]
  
  # Ensure both metrics have the same number of columns
  if (length(metric1_columns) != length(metric2_columns)) {
    stop("The two metrics must have the same number of time periods.")
  }
  
  # Replace blanks with NA
  data[data == ""] <- NA
  
  # Group by Department CODE and calculate the correlation across pay periods
  correlation_result <- data %>%
    group_by(`Department CODE`) %>%
    summarise(
      Correlation_Last_3_Periods = cor(
        as.numeric(unlist(select(cur_data(), tail(metric1_columns, 3)))),
        as.numeric(unlist(select(cur_data(), tail(metric2_columns, 3)))),
        use = "pairwise.complete.obs" 
      ),
      Correlation_Last_13_Periods = cor(
        as.numeric(unlist(select(cur_data(), tail(metric1_columns, 13)))),
        as.numeric(unlist(select(cur_data(), tail(metric2_columns, 13)))),
        use = "pairwise.complete.obs"
      ),
      Correlation_Last_26_Periods = cor(
        as.numeric(unlist(select(cur_data(), tail(metric1_columns, 26)))),
        as.numeric(unlist(select(cur_data(), tail(metric2_columns, 26)))),
        use = "pairwise.complete.obs"
      ),
      .groups = "drop"
    )
  
  return(correlation_result)
}
#---------Applying function to calculate individual metrics---------------
# Premium Pay Spend average (OT + Agency)
Premium_Pay_avg <- calculate_metric_summary(data, "Premium Pay Expense", summary_type = "mean")

# Premium Pay Spend median (OT + Agency)
Premium_Pay_med <- calculate_metric_summary(data, "Premium Pay Expense", summary_type = "median")

# Overtime Pay Spend average
OT_expense_avg <- calculate_metric_summary(data, "Overtime Labor Expense", summary_type = "mean")

# Overtime Pay Spend median
OT_expense_med <- calculate_metric_summary(data, "Overtime Labor Expense", summary_type = "median")

# Paid LE Average
Paid_LE_avg <- calculate_metric_summary(data, "Actual Paid Labor Expense", summary_type = "mean")

# Worked LE average (sub-calculation)
Worked_LE_avg <- calculate_metric_summary(data, "Worked Expenses", summary_type = "mean")

# Premium Pay % of Worked LE Average (NOT PAID)
Premium_Pay_pct_Worked_LE_avg <- Premium_Pay_avg %>%
  inner_join(Worked_LE_avg, by = "Department CODE") %>%
  mutate(
    Premium_Pay_Percentage_3_Periods = sprintf("%.2f%%", 
                                               (Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100),
    Premium_Pay_Percentage_13_Periods = sprintf("%.2f%%", 
                                                (Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100),
    Premium_Pay_Percentage_26_Periods = sprintf("%.2f%%", 
                                                (Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100)
  ) %>%
  select(`Department CODE`, starts_with("Premium_Pay_Percentage"))

# Premium Pay % of Worked LE Median
Premium_Pay_pct_Worked_LE_med <- calculate_metric_summary(data, "Premium Pay % of Worked LE", summary_type = "median")

# OT Pay % of Worked LE Average (NOT PAID)
OT_Pay_pct_Worked_LE_avg <- OT_expense_avg %>%
  inner_join(Worked_LE_avg, by = "Department CODE") %>%
  mutate(
    OT_Pay_Percentage_3_Periods = sprintf("%.2f%%", 
                                               (Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100),
    OT_Pay_Percentage_13_Periods = sprintf("%.2f%%", 
                                                (Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100),
    OT_Pay_Percentage_26_Periods = sprintf("%.2f%%", 
                                                (Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100)
  ) %>%
  select(`Department CODE`, starts_with("OT_Pay_Percentage"))

# OT Pay % of Worked LE Median (NOT PAID)
OT_Pay_pct_Worked_LE_med <- calculate_metric_summary(data, "OT Pay % of Worked LE", summary_type = "median")

# Productivity Index Median
Productivity_Index_med <- calculate_metric_summary(data, "Worked Hours Productivity Index", summary_type = "median")

# Labor Expense Index Median
LE_Index_med <- calculate_metric_summary(data, "Labor Expense Index", summary_type = "median")

#Labor Expense Variance Median
LE_Variance_med <- calculate_metric_summary(data, "Labor Expense Variance", summary_type = "median")

#FTE Variance Median
FTE_Variance_med <- calculate_metric_summary(data, "Worked FTE Variance", summary_type = "median")

#Worked FTE Average
Worked_FTE_avg <- calculate_metric_summary(data, "Worked FTE", summary_type = "mean")

#Paid Labor Expense Average
Paid_LE_avg <- calculate_metric_summary(data, "Actual Paid Labor Expense", summary_type = "mean")

#Target Worked FTE Average
Target_Worked_FTE_avg <- calculate_metric_summary(data, "Total Target Wrked FTE", summary_type = "mean")

#Target Labor Expense Average
Target_LE_avg <- calculate_metric_summary(data, "Target Labor Expense", summary_type = "mean")

#Productivity Index Average
Productivity_Index_avg <- Target_Worked_FTE_avg %>%
  inner_join(Worked_FTE_avg, by = "Department CODE") %>%
  mutate(
    Productivity_Index_3_Periods = sprintf("%.2f", 
                                           Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    Productivity_Index_13_Periods = sprintf("%.2f", 
                                            Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    Productivity_Index_26_Periods = sprintf("%.2f", 
                                            Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(`Department CODE`, starts_with("Productivity_Index"))

# FTE Variance Average: Worked FTE - Target FTE
FTE_Variance_avg <- Worked_FTE_avg %>%
  inner_join(Target_Worked_FTE_avg, by = "Department CODE") %>%
  mutate(
    FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`Department CODE`, starts_with("FTE_Variance"))

# LE Index Average: Target LE / Worked LE
LE_Index_avg <- Target_LE_avg %>%
  inner_join(Worked_LE_avg, by = "Department CODE") %>%
  mutate(
    LE_Index_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    LE_Index_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    LE_Index_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(`Department CODE`, starts_with("LE_Index"))

# LE Variance Average: Worked LE - Target LE
LE_Variance_avg <- Worked_LE_avg %>%
  inner_join(Target_LE_avg, by = "Department CODE") %>%
  mutate(
    LE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    LE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    LE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`Department CODE`, starts_with("LE_Variance"))

# Correlation Coefficient (Staffing to Volume)
correlation_result <- calculate_metric_correlation(data, "Actual Measure Amount", "Actual Worked Hours")
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



# Visualization -----------------------------------------------------------
# How the data will be plotted or how the data table will look including axis
# titles, scales, and color schemes of graphs or data tables.

# List of relevant data frames to combine
dfs <- list(
  Worked_FTE_avg, Paid_LE_avg, Target_Worked_FTE_avg, Target_LE_avg,
  Productivity_Index_avg, FTE_Variance_avg, LE_Index_avg, LE_Variance_avg,
  Premium_Pay_avg, Premium_Pay_pct_Worked_LE_avg, OT_expense_avg, 
  OT_Pay_pct_Worked_LE_avg, Productivity_Index_med, FTE_Variance_med, 
  LE_Index_med, LE_Variance_med, Premium_Pay_med, 
  Premium_Pay_pct_Worked_LE_med, OT_expense_med, OT_Pay_pct_Worked_LE_med, 
  correlation_result
)

# Metric names for column renaming
metric_names <- c(
  "Worked_FTE", "Paid_LE", "Target_Worked_FTE", "Target_LE", 
  "Productivity_Index", "FTE_Variance", "LE_Index", "LE_Variance", 
  "Premium_Pay", "Premium_Pay_pct_Worked_LE", "OT_expense", 
  "OT_Pay_pct_Worked_LE", "Productivity_Index_med", "FTE_Variance_med", 
  "LE_Index_med", "LE_Variance_med", "Premium_Pay_med", 
  "Premium_Pay_pct_Worked_LE_med", "OT_expense_med", "OT_Pay_pct_Worked_LE_med", 
  "correlation_result"
)

# Function to rename columns for each metric data frame
rename_columns <- function(df, metric) {
  # Ensure the data frame has enough columns
  if (ncol(df) >= 4) {
    colnames(df)[2:4] <- paste(metric, c("3_Periods", "13_Periods", "26_Periods"), sep = "_")
  }
  return(df)
}

# Apply renaming function to all data frames
renamed_dfs <- mapply(rename_columns, dfs, metric_names, SIMPLIFY = FALSE)

# Combine data frames using full join by 'Department CODE'
combined_df <- reduce(renamed_dfs, full_join, by = "Department CODE")

# Desired column order: Group by pay periods
new_column_order <- c(
  "Department CODE", 
  paste0(metric_names, "_3_Periods"), 
  paste0(metric_names, "_13_Periods"), 
  paste0(metric_names, "_26_Periods")
)

# Reorder columns
combined_df <- combined_df %>% select(all_of(new_column_order))

# View the final combined data frame
print(combined_df)

# File Saving -------------------------------------------------------------
# Writing files or data for storage
df <- combined_df  

# List of metric df names
metrics <- c(
  "Worked_FTE_avg", "Paid_LE_avg", "Target_Worked_FTE_avg", "Target_LE_avg", 
  "Productivity_Index_avg", "FTE_Variance_avg", "LE_Index_avg", "LE_Variance_avg", 
  "Premium_Pay_avg", "Premium_Pay_pct_Worked_LE_avg", "OT_expense_avg", 
  "OT_Pay_pct_Worked_LE_avg", "Productivity_Index_med", "FTE_Variance_med", 
  "LE_Index_med", "LE_Variance_med", "Premium_Pay_med", 
  "Premium_Pay_pct_Worked_LE_med", "OT_expense_med", "OT_Pay_pct_Worked_LE_med", 
  "correlation_result"
)

#Testing Percent Differences
percent_diff_df <- data.frame(Department_CODE = df$`Department CODE`)

# Function to calculate percentage differences
calc_percent_diff <- function(x, y) {
  # Convert to numeric and handle non-numeric values
  x <- as.numeric(as.character(x))
  y <- as.numeric(as.character(y))
  
  ifelse(is.na(x) | is.na(y) | y == 0, NA, ((x - y) / abs(y)) * 100)
}

# Loop through each metric to calculate percentage differences
for (metric in metrics) {
  # Define column names for the 3, 13, and 26 period data
  col_3 <- paste0(metric, "_3_Periods")
  col_13 <- paste0(metric, "_13_Periods")
  col_26 <- paste0(metric, "_26_Periods")
  
  # Check if all necessary columns exist in the data frame
  if (all(c(col_3, col_13, col_26) %in% names(df))) 
    # Calculate percentage differences and add to the new data frame
    percent_diff_df[[paste0(metric, "_3_vs_13_Periods")]] <- 
      calc_percent_diff(df[[col_3]], df[[col_13]])
    
    percent_diff_df[[paste0(metric, "_13_vs_26_Periods")]] <- 
      calc_percent_diff(df[[col_13]], df[[col_26]])
  } else {
    # Warn if the metric columns are missing
    warning(paste("Missing columns for metric:", metric))
  }
}

# View the final data frame with percentage differences
print(percent_diff_df)
# Script End --------------------------------------------------------------
