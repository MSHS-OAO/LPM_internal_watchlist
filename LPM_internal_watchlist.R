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
library(readxl)
library(writexl)
library(DBI)
library(odbc)
# Assigning Directory(ies) ------------------------------------------------
#Read in Files-----------------------------------------------------------------
dir_testing <- paste0("/SharedDrive/deans/Presidents/SixSigma/MSHS Productivity/",
                        "Productivity/Analysis/Labor Metric Expansion and In-house Watchlist")
setwd(dir_testing)
# Constants ---------------------------------------------------------------

# Data Import -------------------------------------------------------------
#Reporting definitions from DB
oao_con <- dbConnect(odbc(), "OAO Cloud DB Production")
rep_def <- tbl(oao_con, "LPM_MAPPING_REPDEF") %>%
  collect()
key_vol <- tbl(oao_con, "LPM_MAPPING_KEY_VOLUME") %>%
  collect()

# Read in the static,entity volume, LPM service line and labor standard Excel files
static_vol_deps <- read_excel(file.path(dir_testing, "/Mappings/Static Volume Departments.xlsx"))
entity_vol_deps <- read_excel(file.path(dir_testing, "/Mappings/Entity Volume Departments.xlsx"))
LPM_custom_service_line_mapping <- read_excel(file.path(dir_testing, "/Mappings/LPM Custom Service Line Mapping.xlsx"))
labor_standards <- read_excel(file.path(dir_testing, "/Mappings/LaborStandards.xlsx"))
# Read in raw data 
data <- read.csv(file.choose(), check.names = FALSE)

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

# Create a new data frame with only the Department DESC and Department CODE columns
department_data <- cleaned_data[, c("Department CODE  ", "Department DESC  ")]

# Replace the original data with the cleaned data
data <- cleaned_data

# Rollup Data Creation
rollup_data <- data %>%
  left_join(key_vol %>% select(DEFINITION_CODE, KEY_VOLUME), 
          by = c("Department CODE  " = "DEFINITION_CODE")) %>%
  left_join(rep_def %>% select(DEFINITION_CODE, SITE, CORPORATE_SERVICE_LINE, VP, DEPARTMENT_BREAKDOWN), 
            by = c("Department CODE  " = "DEFINITION_CODE")) %>%
  filter(DEPARTMENT_BREAKDOWN != 0) %>%
  select(-DEPARTMENT_BREAKDOWN, -`Facility DESC  `, -`Facility CODE  `, -`Corp Time Period Time Period End Date Measure DESC`) %>%
  left_join(LPM_custom_service_line_mapping, by = "CORPORATE_SERVICE_LINE") %>%
  relocate(SITE, CORPORATE_SERVICE_LINE, LPM_SERVICE_LINE, VP, `Department CODE  `, `Department DESC  `, KEY_VOLUME)

# Creating rollup reports
# Rollup groups definition
rollup_groups <- list(
  rollup_site = c("SITE"),
  rollup_vp = c("VP"),
  rollup_site_corp = c("SITE", "CORPORATE_SERVICE_LINE"),
  rollup_site_lpm = c("SITE", "LPM_SERVICE_LINE"),
  rollup_corp = c("CORPORATE_SERVICE_LINE"),
  rollup_lpm = c("LPM_SERVICE_LINE")
)

# Function to summarize the dataframe
summarize_rollup <- function(df, group_columns) {
  # Get the columns that start with a number (i.e., metric columns)
  metric_columns <- names(df)[grepl("^\\d", names(df))]
  
  # Ensure the selected columns are numeric (convert if needed)
  df[metric_columns] <- lapply(df[metric_columns], function(x) as.numeric(as.character(x)))
  
  # Group by the specified columns and summarize, only summing numeric values
  df %>%
    group_by(across(all_of(group_columns))) %>%
    summarise(across(all_of(metric_columns), ~ sum(.x, na.rm = TRUE)), .groups = "drop")
}

# List to store the rollup dataframes
rollups <- list()

# Generate rollups for each group
for (group_name in names(rollup_groups)) {
  rollups[[group_name]] <- summarize_rollup(rollup_data, rollup_groups[[group_name]])
}

library(dplyr)
library(stringr)

# Function to compute Target to Worked FTE ratio and multiply by 100
# Function to compute Target to Worked FTE ratio and Target to Worked LE ratio, and multiply by 100
compute_ratio <- function(df) {
  # Extract the columns that contain 'Target Worked FTE' and 'Actual Worked FTE'
  target_fte_cols <- grep("Total Target Wrked FTE", colnames(df), value = TRUE)
  actual_fte_cols <- grep("Actual Worked FTE", colnames(df), value = TRUE)
  
  # Extract the columns that contain 'Target Labor Expense' and 'Actual Paid Labor Expense'
  target_le_cols <- grep("Target Labor Expense", colnames(df), value = TRUE)
  actual_le_cols <- grep("Actual Paid Labor Expense", colnames(df), value = TRUE)
  
  # Extract the unique dates based on the column names
  dates <- unique(str_extract(target_fte_cols, "^\\S+"))  # Extract date prefix (e.g., "1/28/2023")
  
  # Loop through each date
  for (date in dates) {
    # Get the corresponding Target and Actual FTE columns for the date
    target_fte_col <- target_fte_cols[grepl(date, target_fte_cols)]
    actual_fte_col <- actual_fte_cols[grepl(date, actual_fte_cols)]
    
    # Get the corresponding Target and Actual LE columns for the date
    target_le_col <- target_le_cols[grepl(date, target_le_cols)]
    actual_le_col <- actual_le_cols[grepl(date, actual_le_cols)]
    
    # Create new column names for the ratios
    ratio_fte_col <- paste0(date, " Target to Worked FTE ratio")
    ratio_le_col <- paste0(date, " Target to Worked LE ratio")
    
    # If both the target and actual FTE columns exist, calculate the FTE ratio and create a new column
    if (length(target_fte_col) > 0 && length(actual_fte_col) > 0) {
      df[[ratio_fte_col]] <- (df[[target_fte_col]] / df[[actual_fte_col]]) * 100  # Multiply by 100
    }
    
    # If both the target and actual LE columns exist, calculate the LE ratio and create a new column
    if (length(target_le_col) > 0 && length(actual_le_col) > 0) {
      df[[ratio_le_col]] <- (df[[target_le_col]] / df[[actual_le_col]]) * 100  # Multiply by 100
    }
  }
  
  return(df)
}

# Apply the function to each data frame in the list 'rollups' and preserve original names
rollups <- lapply(rollups, function(df) compute_ratio(df))


# Data References ---------------------------------------------------------

# Creation of Functions --------------------------------------------------
#Function to calculate average of last 3, 13 and 26 pay periods. User specifies the metric.
calculate_metric_summary <- function(data, metric, summary_type = "mean", group_by_column = "Department CODE") {
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
  data_grouped <- data %>% group_by(!!sym(group_by_column))
  
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
calculate_metric_correlation <- function(data, metric1, metric2, group_by_column = "Department CODE") {
  
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
    group_by(!!sym(group_by_column)) %>%
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

# Function to calculate the linear regression slope of Worked Hours Productivity Index
calculate_slope <- function(data, metric, group_by_column = "Department CODE") {
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
  
  # Function to calculate slope
  calculate_slope <- function(x) {
    valid_values <- x[!is.na(x) & x != 0]
    if (length(valid_values) < 2) return(NA) # Not enough data to calculate slope
    return(lm(valid_values ~ seq_along(valid_values))$coefficients[2]) # Slope calculation
  }
  
  # Group by Department
  data_grouped <- data %>% group_by(!!sym(group_by_column))
  
  # Calculate slopes for each department based on the chosen metric
  slopes <- data_grouped %>% summarise(
    Slope_Last_3_Periods = calculate_slope(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3))))),
    Slope_Last_13_Periods = calculate_slope(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13))))),
    Slope_Last_26_Periods = calculate_slope(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26))))),
    .groups = "drop"
  )
  
  return(slopes)
}

calculate_intercept <- function(data, metric, group_by_column = "Department CODE") {
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
  
  # Function to calculate y-intercept
  calculate_intercept <- function(x) {
    valid_values <- x[!is.na(x) & x != 0]
    if (length(valid_values) < 2) return(NA) # Not enough data to calculate intercept
    model <- lm(valid_values ~ seq_along(valid_values))
    return(model$coefficients[1]) # Intercept calculation
  }
  
  # Group by Department
  data_grouped <- data %>% group_by(!!sym(group_by_column))
  
  # Calculate intercepts for each department based on the chosen metric
  intercepts <- data_grouped %>% summarise(
    Intercept_Last_3_Periods = calculate_intercept(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3))))),
    Intercept_Last_13_Periods = calculate_intercept(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13))))),
    Intercept_Last_26_Periods = calculate_intercept(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26))))),
    .groups = "drop"
  )
  
  return(intercepts)
}

#Function to calculate standard deviation
calculate_metric_sd <- function(data, metric, group_by_column = "Department CODE") {
  
  colnames(data) <- trimws(colnames(data))
  
  # Find columns for the specified metric
  metric_columns <- names(data)[grepl(paste0(" ", metric, "$"), names(data))]
  
  # Replace blanks with NA
  data[data == ""] <- NA
  
  # Group by Department CODE and calculate the standard deviation for the last 3, 13, and 26 periods
  sd_result <- data %>%
    group_by(!!sym(group_by_column)) %>%
    summarise(
      SD_Last_3_Periods = sd(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3)))), na.rm = TRUE),
      SD_Last_13_Periods = sd(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13)))), na.rm = TRUE),
      SD_Last_26_Periods = sd(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26)))), na.rm = TRUE),
      .groups = "drop"
    )
  
  return(sd_result)
}

#Min Max and Range function
calculate_metric_min_max_range <- function(data, metric, group_by_column = "Department CODE") {
  
  colnames(data) <- trimws(colnames(data))
  
  # Find columns for the specified metric
  metric_columns <- names(data)[grepl(paste0(" ", metric, "$"), names(data))]
  
  # Replace blanks with NA
  data[data == ""] <- NA
  
  # Group by Department CODE and calculate the min, max, and range for the last 3, 13, and 26 periods
  min_max_range_result <- data %>%
    group_by(!!sym(group_by_column)) %>%
    summarise(
      Min_Last_3_Periods = min(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3)))), na.rm = TRUE),
      Max_Last_3_Periods = max(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3)))), na.rm = TRUE),
      Range_Last_3_Periods = Max_Last_3_Periods - Min_Last_3_Periods,
      
      Min_Last_13_Periods = min(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13)))), na.rm = TRUE),
      Max_Last_13_Periods = max(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13)))), na.rm = TRUE),
      Range_Last_13_Periods = Max_Last_13_Periods - Min_Last_13_Periods,
      
      Min_Last_26_Periods = min(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26)))), na.rm = TRUE),
      Max_Last_26_Periods = max(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26)))), na.rm = TRUE),
      Range_Last_26_Periods = Max_Last_26_Periods - Min_Last_26_Periods,
      
      .groups = "drop"
    )
  
  return(min_max_range_result)
}

# Function to rename columns for each metric data frame
rename_columns <- function(df, metric) {
  if (grepl("min_max_range", metric)) {
    # Rename for metrics with min, max, and range components (including percentiles)
    colnames(df)[2:10] <- paste(metric, c("Min_3_Periods", "Max_3_Periods", "Range_3_Periods", 
                                          "Min_13_Periods", "Max_13_Periods", "Range_13_Periods",
                                          "Min_26_Periods", "Max_26_Periods", "Range_26_Periods"), sep = "_")
  } else if (grepl("percentiles", metric)) {
    # Rename for percentiles metrics to include Min, Max, and Range for each period
    colnames(df)[2:10] <- paste(metric, c("Percentile_Lower_3_Periods", "Percentile_Upper_3_Periods", "Spread_3_Periods", 
                                          "Percentile_Lower_13_Periods", "Percentile_Upper_13_Periods", "Spread_13_Periods",
                                          "Percentile_Lower_26_Periods", "Percentile_Upper_26_Periods", "Spread_26_Periods"), sep = "_")
  } else {
    # Rename for regular metrics
    colnames(df)[2:4] <- paste(metric, c("3_Periods", "13_Periods", "26_Periods"), sep = "_")
  }
  return(df)
}

#---------Applying function to calculate individual metrics---------------
# Premium Pay Spend average (OT + Agency)
Premium_Pay_avg <- calculate_metric_summary(data, "Premium Pay Expense", summary_type = "mean")

# Premium Pay Spend median (OT + Agency)
Premium_Pay_med <- calculate_metric_summary(data, "Premium Pay Expense", summary_type = "median")

# Premium Pay Hours average (OT + Agency)
Premium_Pay_hours_avg <- calculate_metric_summary(data, "Premium Pay Hours", summary_type = "mean")

# Premium Pay Hours median (OT + Agency)
Premium_Pay_hours_med <- calculate_metric_summary(data, "Premium Pay Hours", summary_type = "median")

# Hourly Rate Average
Hourly_rate_avg <- calculate_metric_summary(data, "Average Hourly Rate", summary_type = "mean")

# Worked Hours average
Worked_hours_avg <- calculate_metric_summary(data, "Actual Worked Hours", summary_type = "mean")

# Overtime Pay Spend average
OT_expense_avg <- calculate_metric_summary(data, "Overtime Labor Expense", summary_type = "mean")

# Overtime Pay Spend median
OT_expense_med <- calculate_metric_summary(data, "Overtime Labor Expense", summary_type = "median")

# Paid LE Average
Paid_LE_avg <- calculate_metric_summary(data, "Actual Paid Labor Expense", summary_type = "mean")

# Worked LE average (sub-calculation)
Worked_LE_avg <- calculate_metric_summary(data, "Worked Expenses", summary_type = "mean")

# Premium Pay % of Worked LE Average
Premium_Pay_pct_Worked_LE_avg <- Premium_Pay_avg %>%
  inner_join(Worked_LE_avg, by = "Department CODE") %>%
  mutate(
    Premium_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`Department CODE`, starts_with("Premium_Pay_Percentage"))

# Premium Pay % of Worked LE Median
Premium_Pay_pct_Worked_LE_med <- calculate_metric_summary(data, "Premium Pay % of Worked LE", summary_type = "median")

# Premium Hours % of Worked Hours Average
Premium_Hours_pct_Worked_hours <- Premium_Pay_hours_avg %>%
  inner_join(Worked_hours_avg, by = "Department CODE") %>%
  mutate(
    Premium_Hours_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Hours_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Hours_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`Department CODE`, starts_with("Premium_Hours_Percentage"))


# Premium Pay FTE Variance from Target Average
#sub calculations
Actual_Premium_Pay_FTEs <- calculate_metric_summary(data, "Actual Premium Pay FTEs", summary_type = "mean")
Premium_Pay_FTEs_Target <- calculate_metric_summary(data, "Premium Pay FTEs Target", summary_type = "mean")
# Premium Pay FTE Variance from Target: Actual Premium Pay FTEs - Premium Pay FTEs Target
Premium_Pay_FTE_Variance_calc <- Actual_Premium_Pay_FTEs %>%
  inner_join(Premium_Pay_FTEs_Target, by = "Department CODE") %>%
  mutate(
    Premium_Pay_FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    Premium_Pay_FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    Premium_Pay_FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`Department CODE`, starts_with("Premium_Pay_FTE_Variance"))

# OT Pay % of Worked LE Average (NOT PAID)
OT_Pay_pct_Worked_LE_avg <- OT_expense_avg %>%
  inner_join(Worked_LE_avg, by = "Department CODE") %>%
  mutate(
    OT_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    OT_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    OT_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
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

#Premium Pay Expense Variance Median
Premium_Pay_Variance_med <- calculate_metric_summary(data, "Premium Pay Expense Variance", summary_type = "median")

# Premium Pay FTE Variance Median
Premium_Pay_FTE_Variance_med <- calculate_metric_summary(data, "Premium Pay FTE Variance", summary_type = "median")

#Premium Pay Expense Variance
Premium_Pay_Variance <- calculate_metric_summary(data, "Premium Pay Expense Variance", summary_type = "mean")

# Premium Pay FTE Variance
Premium_Pay_FTE_Variance <- calculate_metric_summary(data, "Premium Pay FTE Variance", summary_type = "mean")

#Worked FTE Average
Worked_FTE_avg <- calculate_metric_summary(data, "Worked FTE", summary_type = "mean")

#Target Worked FTE Average
Target_Worked_FTE_avg <- calculate_metric_summary(data, "Total Target Wrked FTE", summary_type = "mean")

#Target Labor Expense Average
Target_LE_avg <- calculate_metric_summary(data, "Target Labor Expense", summary_type = "mean")

#Productivity Index Average
Productivity_Index_avg <- Target_Worked_FTE_avg %>%
  inner_join(Worked_FTE_avg, by = "Department CODE") %>%
  mutate(
    Productivity_Index_3_Periods = sprintf("%.4f", 
                                           Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    Productivity_Index_13_Periods = sprintf("%.4f", 
                                            Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    Productivity_Index_26_Periods = sprintf("%.4f", 
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
    LE_Index_3_Periods = sprintf("%.4f", Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    LE_Index_13_Periods = sprintf("%.4f", Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    LE_Index_26_Periods = sprintf("%.4f", Average_Last_26_Periods.x / Average_Last_26_Periods.y)
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

# Apply function to calculate the slope for Worked Hours Productivity Index
Worked_Hours_Prod_Slope <- calculate_slope(data, "Worked Hours Productivity Index")
LE_Index_Slope <- calculate_slope(data, "Labor Expense Index")
Worked_Hours_FTE_Variance_Slope <- calculate_slope(data, "Worked FTE Variance")
LE_Variance_Slope <- calculate_slope(data, "Labor Expense Variance")
Premium_Hours_pct_Worked_hours_Slope <- calculate_slope(data, "Premium Hours % of Worked Hours")

# Apply y intercept function
Worked_Hours_PI_Intercepts <- calculate_intercept(data, "Worked Hours Productivity Index")
LE_PI_Intercepts <- calculate_intercept(data, "Labor Expense Index")
Premium_Hours_pct_Worked_hours_Intercepts <- calculate_intercept(data, "Premium Hours % of Worked Hours")

#Applying standard deviation function
PI_stdv <- calculate_metric_sd(data, "Worked Hours Productivity Index")
LE_stdv <- calculate_metric_sd(data, "Labor Expense Index")
FTE_Variance_stdv <- calculate_metric_sd(data, "FTE Variance")
LE_Variance_stdv <- calculate_metric_sd(data, "Labor Expense Variance")

#Applying min max and range function
PI_min_max_range <- calculate_metric_min_max_range(data, "Worked Hours Productivity Index")
LE_min_max_range <- calculate_metric_min_max_range(data, "Labor Expense Index")
FTE_Variance_min_max_range <- calculate_metric_min_max_range(data, "FTE Variance")
LE_Variance_min_max_range <- calculate_metric_min_max_range(data, "Labor Expense Variance")

# ------------------------Rollup Calculations-------------------------------
#---------Applying function to calculate rollup metrics---------------
#-----------SITE ROLLUP------------------------------------------------
# Premium Pay Spend average (OT + Agency)
Premium_Pay_avg_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Premium Pay Expense", summary_type = "mean", "SITE")

# Premium Pay Spend median (OT + Agency)
Premium_Pay_med_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Premium Pay Expense", summary_type = "median", "SITE")

# Premium Pay Hours average (OT + Agency)
Premium_Pay_hours_avg_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Premium Pay Hours", summary_type = "mean", "SITE")

# Premium Pay Hours median (OT + Agency)
Premium_Pay_hours_med_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Premium Pay Hours", summary_type = "median", "SITE")

# Worked Hours average
Worked_hours_avg_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Actual Worked Hours", summary_type = "mean", "SITE")

# Overtime Pay Spend average
OT_expense_avg_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Overtime Labor Expense", summary_type = "mean", "SITE")

# Overtime Pay Spend median
OT_expense_med_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Overtime Labor Expense", summary_type = "median", "SITE")

# Paid LE Average
Paid_LE_avg_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Actual Paid Labor Expense", summary_type = "mean", "SITE")

# Worked LE average (sub-calculation)
Worked_LE_avg_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Worked Expenses", summary_type = "mean", "SITE")

# Hourly Rate Average 
Hourly_rate_avg_rollup_site <- Worked_LE_avg_rollup_site %>%
  inner_join(Worked_hours_avg_rollup_site, by = "SITE") %>%
  mutate(
    Hourly_rate_avg_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y), 2),
    Hourly_rate_avg_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y), 2),
    Hourly_rate_avg_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y), 2)
  ) %>%
  select(`SITE`, starts_with("Hourly_rate_avg"))

# Premium Pay % of Worked LE Average
Premium_Pay_pct_Worked_LE_avg_rollup_site <- Premium_Pay_avg_rollup_site %>%
  inner_join(Worked_LE_avg_rollup_site, by = "SITE") %>%
  mutate(
    Premium_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`SITE`, starts_with("Premium_Pay_Percentage"))

# Premium Pay % of Worked LE Median
Premium_Pay_pct_Worked_LE_med_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Premium Pay % of Worked LE", summary_type = "median", "SITE")

# Premium Hours % of Worked Hours Average
Premium_Hours_pct_Worked_hours_rollup_site <- Premium_Pay_hours_avg_rollup_site %>%
  inner_join(Worked_hours_avg_rollup_site, by = "SITE") %>%
  mutate(
    Premium_Hours_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Hours_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Hours_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`SITE`, starts_with("Premium_Hours_Percentage"))


# Premium Pay FTE Variance from Target Average
#sub calculations
Actual_Premium_Pay_FTEs_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Actual Premium Pay FTEs", summary_type = "mean", "SITE")
Premium_Pay_FTEs_Target_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Premium Pay FTEs Target", summary_type = "mean", "SITE")
# Premium Pay FTE Variance from Target: Actual Premium Pay FTEs - Premium Pay FTEs Target
Premium_Pay_FTE_Variance_calc_rollup_site <- Actual_Premium_Pay_FTEs_rollup_site %>%
  inner_join(Premium_Pay_FTEs_Target_rollup_site, by = "SITE") %>%
  mutate(
    Premium_Pay_FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    Premium_Pay_FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    Premium_Pay_FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`SITE`, starts_with("Premium_Pay_FTE_Variance"))

# OT Pay % of Worked LE Average (NOT PAID)
OT_Pay_pct_Worked_LE_avg_rollup_site <- OT_expense_avg_rollup_site %>%
  inner_join(Worked_LE_avg_rollup_site, by = "SITE") %>%
  mutate(
    OT_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    OT_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    OT_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`SITE`, starts_with("OT_Pay_Percentage"))

# OT Pay % of Worked LE Median (NOT PAID)
OT_Pay_pct_Worked_LE_med_rollup_site <- calculate_metric_summary(rollups$rollup_site, "OT Pay % of Worked LE", summary_type = "median", "SITE")

# Productivity Index Median
Productivity_Index_med_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Worked Hours Productivity Index", summary_type = "median", "SITE")

# Labor Expense Index Median
LE_Index_med_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Labor Expense Index", summary_type = "median", "SITE")

#Labor Expense Variance Median
LE_Variance_med_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Labor Expense Variance", summary_type = "median", "SITE")

#FTE Variance Median
FTE_Variance_med_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Worked FTE Variance", summary_type = "median", "SITE")

#Premium Pay Expense Variance Median
Premium_Pay_Variance_med_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Premium Pay Expense Variance", summary_type = "median", "SITE")

# Premium Pay FTE Variance Median
Premium_Pay_FTE_Variance_med_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Premium Pay FTE Variance", summary_type = "median", "SITE")

#Premium Pay Expense Variance
Premium_Pay_Variance_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Premium Pay Expense Variance", summary_type = "mean", "SITE")

# Premium Pay FTE Variance
Premium_Pay_FTE_Variance_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Premium Pay FTE Variance", summary_type = "mean", "SITE")

#Worked FTE Average
Worked_FTE_avg_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Worked FTE", summary_type = "mean", "SITE")

#Target Worked FTE Average
Target_Worked_FTE_avg_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Total Target Wrked FTE", summary_type = "mean", "SITE")

#Target Labor Expense Average
Target_LE_avg_rollup_site <- calculate_metric_summary(rollups$rollup_site, "Target Labor Expense", summary_type = "mean", "SITE")

#Productivity Index Average
Productivity_Index_avg_rollup_site <- Target_Worked_FTE_avg_rollup_site %>%
  inner_join(Worked_FTE_avg_rollup_site, by = "SITE") %>%
  mutate(
    Productivity_Index_3_Periods = sprintf("%.4f", 
                                           Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    Productivity_Index_13_Periods = sprintf("%.4f", 
                                            Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    Productivity_Index_26_Periods = sprintf("%.4f", 
                                            Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(`SITE`, starts_with("Productivity_Index"))

# FTE Variance Average: Worked FTE - Target FTE
FTE_Variance_avg_rollup_site <- Worked_FTE_avg_rollup_site %>%
  inner_join(Target_Worked_FTE_avg_rollup_site, by = "SITE") %>%
  mutate(
    FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`SITE`, starts_with("FTE_Variance"))

# LE Index Average: Target LE / Worked LE
LE_Index_avg_rollup_site <- Target_LE_avg_rollup_site %>%
  inner_join(Worked_LE_avg_rollup_site, by = "SITE") %>%
  mutate(
    LE_Index_3_Periods = sprintf("%.4f", Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    LE_Index_13_Periods = sprintf("%.4f", Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    LE_Index_26_Periods = sprintf("%.4f", Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(`SITE`, starts_with("LE_Index"))

# LE Variance Average: Worked LE - Target LE
LE_Variance_avg_rollup_site <- Worked_LE_avg_rollup_site %>%
  inner_join(Target_LE_avg_rollup_site, by = "SITE") %>%
  mutate(
    LE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    LE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    LE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`SITE`, starts_with("LE_Variance"))

# Correlation Coefficient (Staffing to Volume)
correlation_result_rollup_site <- calculate_metric_correlation(rollups$rollup_site, "Total Target Wrked FTE", "Actual Worked FTE", "SITE")

# Apply function to calculate the slope for Worked Hours Productivity Index
Worked_Hours_Prod_Slope_rollup_site <- calculate_slope(rollups$rollup_site, "Target to Worked FTE ratio", "SITE")
LE_Index_Slope_rollup_site <- calculate_slope(rollups$rollup_site, "Target to Worked LE ratio", "SITE")
Worked_Hours_FTE_Variance_Slope_rollup_site <- calculate_slope(rollups$rollup_site, "Worked FTE Variance", "SITE")
LE_Variance_Slope_rollup_site <- calculate_slope(rollups$rollup_site, "Labor Expense Variance", "SITE")
Premium_Hours_pct_Worked_hours_Slope_rollup_site <- calculate_slope(rollups$rollup_site, "Premium Hours % of Worked Hours", "SITE")

# Apply y intercept function
Worked_Hours_PI_Intercepts_rollup_site <- calculate_intercept(rollups$rollup_site, "Target to Worked FTE ratio", "SITE")
LE_PI_Intercepts_rollup_site <- calculate_intercept(rollups$rollup_site, "Target to Worked LE ratio", "SITE")
Premium_Hours_pct_Worked_hours_Intercepts_rollup_site <- calculate_intercept(rollups$rollup_site, "Premium Hours % of Worked Hours", "SITE")

#Applying standard deviation function
PI_stdv_rollup_site <- calculate_metric_sd(rollups$rollup_site, "Target to Worked FTE ratio", "SITE")
LE_stdv_rollup_site <- calculate_metric_sd(rollups$rollup_site, "Target to Worked LE ratio", "SITE")
FTE_Variance_stdv_rollup_site <- calculate_metric_sd(rollups$rollup_site, "FTE Variance", "SITE")
LE_Variance_stdv_rollup_site <- calculate_metric_sd(rollups$rollup_site, "Labor Expense Variance", "SITE")

#Applying min max and range function
PI_min_max_range_rollup_site <- calculate_metric_min_max_range(rollups$rollup_site, "Target to Worked FTE ratio", "SITE")
LE_min_max_range_rollup_site <- calculate_metric_min_max_range(rollups$rollup_site, "Target to Worked LE ratio", "SITE")
FTE_Variance_min_max_range_rollup_site <- calculate_metric_min_max_range(rollups$rollup_site, "FTE Variance", "SITE")
LE_Variance_min_max_range_rollup_site <- calculate_metric_min_max_range(rollups$rollup_site, "Labor Expense Variance", "SITE")

#-----------VP ROLLUP------------------------------------------------
# Premium Pay Spend average (OT + Agency)
Premium_Pay_avg_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Premium Pay Expense", summary_type = "mean", "VP")

# Premium Pay Spend median (OT + Agency)
Premium_Pay_med_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Premium Pay Expense", summary_type = "median", "VP")

# Premium Pay Hours average (OT + Agency)
Premium_Pay_hours_avg_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Premium Pay Hours", summary_type = "mean", "VP")

# Premium Pay Hours median (OT + Agency)
Premium_Pay_hours_med_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Premium Pay Hours", summary_type = "median", "VP")

# Worked Hours average
Worked_hours_avg_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Actual Worked Hours", summary_type = "mean", "VP")

# Overtime Pay Spend average
OT_expense_avg_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Overtime Labor Expense", summary_type = "mean", "VP")

# Overtime Pay Spend median
OT_expense_med_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Overtime Labor Expense", summary_type = "median", "VP")

# Paid LE Average
Paid_LE_avg_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Actual Paid Labor Expense", summary_type = "mean", "VP")

# Worked LE average (sub-calculation)
Worked_LE_avg_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Worked Expenses", summary_type = "mean", "VP")

# Hourly Rate Average 
Hourly_rate_avg_rollup_vp <- Worked_LE_avg_rollup_vp %>%
  inner_join(Worked_hours_avg_rollup_vp, by = "VP") %>%
  mutate(
    Hourly_rate_avg_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y), 2),
    Hourly_rate_avg_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y), 2),
    Hourly_rate_avg_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y), 2)
  ) %>%
  select(`VP`, starts_with("Hourly_rate_avg"))

# Premium Pay % of Worked LE Average
Premium_Pay_pct_Worked_LE_avg_rollup_vp <- Premium_Pay_avg_rollup_vp %>%
  inner_join(Worked_LE_avg_rollup_vp, by = "VP") %>%
  mutate(
    Premium_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`VP`, starts_with("Premium_Pay_Percentage"))

# Premium Pay % of Worked LE Median
Premium_Pay_pct_Worked_LE_med_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Premium Pay % of Worked LE", summary_type = "median", "VP")

# Premium Hours % of Worked Hours Average
Premium_Hours_pct_Worked_hours_rollup_vp <- Premium_Pay_hours_avg_rollup_vp %>%
  inner_join(Worked_hours_avg_rollup_vp, by = "VP") %>%
  mutate(
    Premium_Hours_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Hours_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Hours_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`VP`, starts_with("Premium_Hours_Percentage"))


# Premium Pay FTE Variance from Target Average
#sub calculations
Actual_Premium_Pay_FTEs_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Actual Premium Pay FTEs", summary_type = "mean", "VP")
Premium_Pay_FTEs_Target_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Premium Pay FTEs Target", summary_type = "mean", "VP")
# Premium Pay FTE Variance from Target: Actual Premium Pay FTEs - Premium Pay FTEs Target
Premium_Pay_FTE_Variance_calc_rollup_vp <- Actual_Premium_Pay_FTEs_rollup_vp %>%
  inner_join(Premium_Pay_FTEs_Target_rollup_vp, by = "VP") %>%
  mutate(
    Premium_Pay_FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    Premium_Pay_FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    Premium_Pay_FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`VP`, starts_with("Premium_Pay_FTE_Variance"))

# OT Pay % of Worked LE Average (NOT PAID)
OT_Pay_pct_Worked_LE_avg_rollup_vp <- OT_expense_avg_rollup_vp %>%
  inner_join(Worked_LE_avg_rollup_vp, by = "VP") %>%
  mutate(
    OT_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    OT_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    OT_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`VP`, starts_with("OT_Pay_Percentage"))

# OT Pay % of Worked LE Median (NOT PAID)
OT_Pay_pct_Worked_LE_med_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "OT Pay % of Worked LE", summary_type = "median", "VP")

# Productivity Index Median
Productivity_Index_med_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Worked Hours Productivity Index", summary_type = "median", "VP")

# Labor Expense Index Median
LE_Index_med_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Labor Expense Index", summary_type = "median", "VP")

#Labor Expense Variance Median
LE_Variance_med_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Labor Expense Variance", summary_type = "median", "VP")

#FTE Variance Median
FTE_Variance_med_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Worked FTE Variance", summary_type = "median", "VP")

#Premium Pay Expense Variance Median
Premium_Pay_Variance_med_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Premium Pay Expense Variance", summary_type = "median", "VP")

# Premium Pay FTE Variance Median
Premium_Pay_FTE_Variance_med_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Premium Pay FTE Variance", summary_type = "median", "VP")

#Premium Pay Expense Variance
Premium_Pay_Variance_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Premium Pay Expense Variance", summary_type = "mean", "VP")

# Premium Pay FTE Variance
Premium_Pay_FTE_Variance_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Premium Pay FTE Variance", summary_type = "mean", "VP")

#Worked FTE Average
Worked_FTE_avg_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Worked FTE", summary_type = "mean", "VP")

#Target Worked FTE Average
Target_Worked_FTE_avg_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Total Target Wrked FTE", summary_type = "mean", "VP")

#Target Labor Expense Average
Target_LE_avg_rollup_vp <- calculate_metric_summary(rollups$rollup_vp, "Target Labor Expense", summary_type = "mean", "VP")

#Productivity Index Average
Productivity_Index_avg_rollup_vp <- Target_Worked_FTE_avg_rollup_vp %>%
  inner_join(Worked_FTE_avg_rollup_vp, by = "VP") %>%
  mutate(
    Productivity_Index_3_Periods = sprintf("%.4f", 
                                           Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    Productivity_Index_13_Periods = sprintf("%.4f", 
                                            Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    Productivity_Index_26_Periods = sprintf("%.4f", 
                                            Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(`VP`, starts_with("Productivity_Index"))

# FTE Variance Average: Worked FTE - Target FTE
FTE_Variance_avg_rollup_vp <- Worked_FTE_avg_rollup_vp %>%
  inner_join(Target_Worked_FTE_avg_rollup_vp, by = "VP") %>%
  mutate(
    FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`VP`, starts_with("FTE_Variance"))

# LE Index Average: Target LE / Worked LE
LE_Index_avg_rollup_vp <- Target_LE_avg_rollup_vp %>%
  inner_join(Worked_LE_avg_rollup_vp, by = "VP") %>%
  mutate(
    LE_Index_3_Periods = sprintf("%.4f", Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    LE_Index_13_Periods = sprintf("%.4f", Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    LE_Index_26_Periods = sprintf("%.4f", Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(`VP`, starts_with("LE_Index"))

# LE Variance Average: Worked LE - Target LE
LE_Variance_avg_rollup_vp <- Worked_LE_avg_rollup_vp %>%
  inner_join(Target_LE_avg_rollup_vp, by = "VP") %>%
  mutate(
    LE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    LE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    LE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`VP`, starts_with("LE_Variance"))

# Correlation Coefficient (Staffing to Volume)
correlation_result_rollup_vp <- calculate_metric_correlation(rollups$rollup_vp, "Total Target Wrked FTE", "Actual Worked FTE", "VP")

# Apply function to calculate the slope for Worked Hours Productivity Index
Worked_Hours_Prod_Slope_rollup_vp <- calculate_slope(rollups$rollup_vp, "Target to Worked FTE ratio", "VP")
LE_Index_Slope_rollup_vp <- calculate_slope(rollups$rollup_vp, "Target to Worked LE ratio", "VP")
Worked_Hours_FTE_Variance_Slope_rollup_vp <- calculate_slope(rollups$rollup_vp, "Worked FTE Variance", "VP")
LE_Variance_Slope_rollup_vp <- calculate_slope(rollups$rollup_vp, "Labor Expense Variance", "VP")
Premium_Hours_pct_Worked_hours_Slope_rollup_vp <- calculate_slope(rollups$rollup_vp, "Premium Hours % of Worked Hours", "VP")

# Apply y intercept function
Worked_Hours_PI_Intercepts_rollup_vp <- calculate_intercept(rollups$rollup_vp, "Target to Worked FTE ratio", "VP")
LE_PI_Intercepts_rollup_vp <- calculate_intercept(rollups$rollup_vp, "Target to Worked LE ratio", "VP")
Premium_Hours_pct_Worked_hours_Intercepts_rollup_vp <- calculate_intercept(rollups$rollup_vp, "Premium Hours % of Worked Hours", "VP")

#Applying standard deviation function
PI_stdv_rollup_vp <- calculate_metric_sd(rollups$rollup_vp, "Target to Worked FTE ratio", "VP")
LE_stdv_rollup_vp <- calculate_metric_sd(rollups$rollup_vp, "Target to Worked LE ratio", "VP")
FTE_Variance_stdv_rollup_vp <- calculate_metric_sd(rollups$rollup_vp, "FTE Variance", "VP")
LE_Variance_stdv_rollup_vp <- calculate_metric_sd(rollups$rollup_vp, "Labor Expense Variance", "VP")

#Applying min max and range function
PI_min_max_range_rollup_vp <- calculate_metric_min_max_range(rollups$rollup_vp, "Target to Worked FTE ratio", "VP")
LE_min_max_range_rollup_vp <- calculate_metric_min_max_range(rollups$rollup_vp, "Target to Worked LE ratio", "VP")
FTE_Variance_min_max_range_rollup_vp <- calculate_metric_min_max_range(rollups$rollup_vp, "FTE Variance", "VP")
LE_Variance_min_max_range_rollup_vp <- calculate_metric_min_max_range(rollups$rollup_vp, "Labor Expense Variance", "VP")

#-----------CORPORATE_SERVICE_LINE ROLLUP------------------------------------------------
# Premium Pay Spend average (OT + Agency)
Premium_Pay_avg_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Premium Pay Expense", summary_type = "mean", "CORPORATE_SERVICE_LINE")

# Premium Pay Spend median (OT + Agency)
Premium_Pay_med_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Premium Pay Expense", summary_type = "median", "CORPORATE_SERVICE_LINE")

# Premium Pay Hours average (OT + Agency)
Premium_Pay_hours_avg_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Premium Pay Hours", summary_type = "mean", "CORPORATE_SERVICE_LINE")

# Premium Pay Hours median (OT + Agency)
Premium_Pay_hours_med_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Premium Pay Hours", summary_type = "median", "CORPORATE_SERVICE_LINE")

# Worked Hours average
Worked_hours_avg_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Actual Worked Hours", summary_type = "mean", "CORPORATE_SERVICE_LINE")

# Overtime Pay Spend average
OT_expense_avg_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Overtime Labor Expense", summary_type = "mean", "CORPORATE_SERVICE_LINE")

# Overtime Pay Spend median
OT_expense_med_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Overtime Labor Expense", summary_type = "median", "CORPORATE_SERVICE_LINE")

# Paid LE Average
Paid_LE_avg_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Actual Paid Labor Expense", summary_type = "mean", "CORPORATE_SERVICE_LINE")

# Worked LE average (sub-calculation)
Worked_LE_avg_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Worked Expenses", summary_type = "mean", "CORPORATE_SERVICE_LINE")

# Hourly Rate Average 
Hourly_rate_avg_rollup_corp <- Worked_LE_avg_rollup_corp %>%
  inner_join(Worked_hours_avg_rollup_corp, by = "CORPORATE_SERVICE_LINE") %>%
  mutate(
    Hourly_rate_avg_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y), 2),
    Hourly_rate_avg_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y), 2),
    Hourly_rate_avg_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y), 2)
  ) %>%
  select(`CORPORATE_SERVICE_LINE`, starts_with("Hourly_rate_avg"))

# Premium Pay % of Worked LE Average
Premium_Pay_pct_Worked_LE_avg_rollup_corp <- Premium_Pay_avg_rollup_corp %>%
  inner_join(Worked_LE_avg_rollup_corp, by = "CORPORATE_SERVICE_LINE") %>%
  mutate(
    Premium_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`CORPORATE_SERVICE_LINE`, starts_with("Premium_Pay_Percentage"))

# Premium Pay % of Worked LE Median
Premium_Pay_pct_Worked_LE_med_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Premium Pay % of Worked LE", summary_type = "median", "CORPORATE_SERVICE_LINE")

# Premium Hours % of Worked Hours Average
Premium_Hours_pct_Worked_hours_rollup_corp <- Premium_Pay_hours_avg_rollup_corp %>%
  inner_join(Worked_hours_avg_rollup_corp, by = "CORPORATE_SERVICE_LINE") %>%
  mutate(
    Premium_Hours_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Hours_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Hours_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`CORPORATE_SERVICE_LINE`, starts_with("Premium_Hours_Percentage"))


# Premium Pay FTE Variance from Target Average
#sub calculations
Actual_Premium_Pay_FTEs_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Actual Premium Pay FTEs", summary_type = "mean", "CORPORATE_SERVICE_LINE")
Premium_Pay_FTEs_Target_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Premium Pay FTEs Target", summary_type = "mean", "CORPORATE_SERVICE_LINE")
# Premium Pay FTE Variance from Target: Actual Premium Pay FTEs - Premium Pay FTEs Target
Premium_Pay_FTE_Variance_calc_rollup_corp <- Actual_Premium_Pay_FTEs_rollup_corp %>%
  inner_join(Premium_Pay_FTEs_Target_rollup_corp, by = "CORPORATE_SERVICE_LINE") %>%
  mutate(
    Premium_Pay_FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    Premium_Pay_FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    Premium_Pay_FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`CORPORATE_SERVICE_LINE`, starts_with("Premium_Pay_FTE_Variance"))

# OT Pay % of Worked LE Average (NOT PAID)
OT_Pay_pct_Worked_LE_avg_rollup_corp <- OT_expense_avg_rollup_corp %>%
  inner_join(Worked_LE_avg_rollup_corp, by = "CORPORATE_SERVICE_LINE") %>%
  mutate(
    OT_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    OT_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    OT_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`CORPORATE_SERVICE_LINE`, starts_with("OT_Pay_Percentage"))

# OT Pay % of Worked LE Median (NOT PAID)
OT_Pay_pct_Worked_LE_med_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "OT Pay % of Worked LE", summary_type = "median", "CORPORATE_SERVICE_LINE")

# Productivity Index Median
Productivity_Index_med_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Worked Hours Productivity Index", summary_type = "median", "CORPORATE_SERVICE_LINE")

# Labor Expense Index Median
LE_Index_med_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Labor Expense Index", summary_type = "median", "CORPORATE_SERVICE_LINE")

#Labor Expense Variance Median
LE_Variance_med_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Labor Expense Variance", summary_type = "median", "CORPORATE_SERVICE_LINE")

#FTE Variance Median
FTE_Variance_med_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Worked FTE Variance", summary_type = "median", "CORPORATE_SERVICE_LINE")

#Premium Pay Expense Variance Median
Premium_Pay_Variance_med_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Premium Pay Expense Variance", summary_type = "median", "CORPORATE_SERVICE_LINE")

# Premium Pay FTE Variance Median
Premium_Pay_FTE_Variance_med_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Premium Pay FTE Variance", summary_type = "median", "CORPORATE_SERVICE_LINE")

#Premium Pay Expense Variance
Premium_Pay_Variance_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Premium Pay Expense Variance", summary_type = "mean", "CORPORATE_SERVICE_LINE")

# Premium Pay FTE Variance
Premium_Pay_FTE_Variance_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Premium Pay FTE Variance", summary_type = "mean", "CORPORATE_SERVICE_LINE")

#Worked FTE Average
Worked_FTE_avg_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Worked FTE", summary_type = "mean", "CORPORATE_SERVICE_LINE")

#Target Worked FTE Average
Target_Worked_FTE_avg_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Total Target Wrked FTE", summary_type = "mean", "CORPORATE_SERVICE_LINE")

#Target Labor Expense Average
Target_LE_avg_rollup_corp <- calculate_metric_summary(rollups$rollup_corp, "Target Labor Expense", summary_type = "mean", "CORPORATE_SERVICE_LINE")

#Productivity Index Average
Productivity_Index_avg_rollup_corp <- Target_Worked_FTE_avg_rollup_corp %>%
  inner_join(Worked_FTE_avg_rollup_corp, by = "CORPORATE_SERVICE_LINE") %>%
  mutate(
    Productivity_Index_3_Periods = sprintf("%.4f", 
                                           Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    Productivity_Index_13_Periods = sprintf("%.4f", 
                                            Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    Productivity_Index_26_Periods = sprintf("%.4f", 
                                            Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(`CORPORATE_SERVICE_LINE`, starts_with("Productivity_Index"))

# FTE Variance Average: Worked FTE - Target FTE
FTE_Variance_avg_rollup_corp <- Worked_FTE_avg_rollup_corp %>%
  inner_join(Target_Worked_FTE_avg_rollup_corp, by = "CORPORATE_SERVICE_LINE") %>%
  mutate(
    FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`CORPORATE_SERVICE_LINE`, starts_with("FTE_Variance"))

# LE Index Average: Target LE / Worked LE
LE_Index_avg_rollup_corp <- Target_LE_avg_rollup_corp %>%
  inner_join(Worked_LE_avg_rollup_corp, by = "CORPORATE_SERVICE_LINE") %>%
  mutate(
    LE_Index_3_Periods = sprintf("%.4f", Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    LE_Index_13_Periods = sprintf("%.4f", Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    LE_Index_26_Periods = sprintf("%.4f", Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(`CORPORATE_SERVICE_LINE`, starts_with("LE_Index"))

# LE Variance Average: Worked LE - Target LE
LE_Variance_avg_rollup_corp <- Worked_LE_avg_rollup_corp %>%
  inner_join(Target_LE_avg_rollup_corp, by = "CORPORATE_SERVICE_LINE") %>%
  mutate(
    LE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    LE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    LE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`CORPORATE_SERVICE_LINE`, starts_with("LE_Variance"))

# Correlation Coefficient (Staffing to Volume)
correlation_result_rollup_corp <- calculate_metric_correlation(rollups$rollup_corp, "Total Target Wrked FTE", "Actual Worked FTE", "CORPORATE_SERVICE_LINE")

# Apply function to calculate the slope for Worked Hours Productivity Index
Worked_Hours_Prod_Slope_rollup_corp <- calculate_slope(rollups$rollup_corp, "Target to Worked FTE ratio", "CORPORATE_SERVICE_LINE")
LE_Index_Slope_rollup_corp <- calculate_slope(rollups$rollup_corp, "Target to Worked LE ratio", "CORPORATE_SERVICE_LINE")
Worked_Hours_FTE_Variance_Slope_rollup_corp <- calculate_slope(rollups$rollup_corp, "Worked FTE Variance", "CORPORATE_SERVICE_LINE")
LE_Variance_Slope_rollup_corp <- calculate_slope(rollups$rollup_corp, "Labor Expense Variance", "CORPORATE_SERVICE_LINE")
Premium_Hours_pct_Worked_hours_Slope_rollup_corp <- calculate_slope(rollups$rollup_corp, "Premium Hours % of Worked Hours", "CORPORATE_SERVICE_LINE")

# Apply y intercept function
Worked_Hours_PI_Intercepts_rollup_corp <- calculate_intercept(rollups$rollup_corp, "Target to Worked FTE ratio", "CORPORATE_SERVICE_LINE")
LE_PI_Intercepts_rollup_corp <- calculate_intercept(rollups$rollup_corp, "Target to Worked LE ratio", "CORPORATE_SERVICE_LINE")
Premium_Hours_pct_Worked_hours_Intercepts_rollup_corp <- calculate_intercept(rollups$rollup_corp, "Premium Hours % of Worked Hours", "CORPORATE_SERVICE_LINE")

#Applying standard deviation function
PI_stdv_rollup_corp <- calculate_metric_sd(rollups$rollup_corp, "Target to Worked FTE ratio", "CORPORATE_SERVICE_LINE")
LE_stdv_rollup_corp <- calculate_metric_sd(rollups$rollup_corp, "Target to Worked LE ratio", "CORPORATE_SERVICE_LINE")
FTE_Variance_stdv_rollup_corp <- calculate_metric_sd(rollups$rollup_corp, "FTE Variance", "CORPORATE_SERVICE_LINE")
LE_Variance_stdv_rollup_corp <- calculate_metric_sd(rollups$rollup_corp, "Labor Expense Variance", "CORPORATE_SERVICE_LINE")

#Applying min max and range function
PI_min_max_range_rollup_corp <- calculate_metric_min_max_range(rollups$rollup_corp, "Target to Worked FTE ratio", "CORPORATE_SERVICE_LINE")
LE_min_max_range_rollup_corp <- calculate_metric_min_max_range(rollups$rollup_corp, "Target to Worked LE ratio", "CORPORATE_SERVICE_LINE")
FTE_Variance_min_max_range_rollup_corp <- calculate_metric_min_max_range(rollups$rollup_corp, "FTE Variance", "CORPORATE_SERVICE_LINE")
LE_Variance_min_max_range_rollup_corp <- calculate_metric_min_max_range(rollups$rollup_corp, "Labor Expense Variance", "CORPORATE_SERVICE_LINE")


#-----------LPM_SERVICE_LINE ROLLUP------------------------------------------------
# Premium Pay Spend average (OT + Agency)
Premium_Pay_avg_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Premium Pay Expense", summary_type = "mean", "LPM_SERVICE_LINE")

# Premium Pay Spend median (OT + Agency)
Premium_Pay_med_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Premium Pay Expense", summary_type = "median", "LPM_SERVICE_LINE")

# Premium Pay Hours average (OT + Agency)
Premium_Pay_hours_avg_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Premium Pay Hours", summary_type = "mean", "LPM_SERVICE_LINE")

# Premium Pay Hours median (OT + Agency)
Premium_Pay_hours_med_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Premium Pay Hours", summary_type = "median", "LPM_SERVICE_LINE")

# Worked Hours average
Worked_hours_avg_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Actual Worked Hours", summary_type = "mean", "LPM_SERVICE_LINE")

# Overtime Pay Spend average
OT_expense_avg_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Overtime Labor Expense", summary_type = "mean", "LPM_SERVICE_LINE")

# Overtime Pay Spend median
OT_expense_med_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Overtime Labor Expense", summary_type = "median", "LPM_SERVICE_LINE")

# Paid LE Average
Paid_LE_avg_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Actual Paid Labor Expense", summary_type = "mean", "LPM_SERVICE_LINE")

# Worked LE average (sub-calculation)
Worked_LE_avg_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Worked Expenses", summary_type = "mean", "LPM_SERVICE_LINE")

# Hourly Rate Average 
Hourly_rate_avg_rollup_lpm <- Worked_LE_avg_rollup_lpm %>%
  inner_join(Worked_hours_avg_rollup_lpm, by = "LPM_SERVICE_LINE") %>%
  mutate(
    Hourly_rate_avg_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y), 2),
    Hourly_rate_avg_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y), 2),
    Hourly_rate_avg_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y), 2)
  ) %>%
  select(`LPM_SERVICE_LINE`, starts_with("Hourly_rate_avg"))

# Premium Pay % of Worked LE Average
Premium_Pay_pct_Worked_LE_avg_rollup_lpm <- Premium_Pay_avg_rollup_lpm %>%
  inner_join(Worked_LE_avg_rollup_lpm, by = "LPM_SERVICE_LINE") %>%
  mutate(
    Premium_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`LPM_SERVICE_LINE`, starts_with("Premium_Pay_Percentage"))

# Premium Pay % of Worked LE Median
Premium_Pay_pct_Worked_LE_med_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Premium Pay % of Worked LE", summary_type = "median", "LPM_SERVICE_LINE")

# Premium Hours % of Worked Hours Average
Premium_Hours_pct_Worked_hours_rollup_lpm <- Premium_Pay_hours_avg_rollup_lpm %>%
  inner_join(Worked_hours_avg_rollup_lpm, by = "LPM_SERVICE_LINE") %>%
  mutate(
    Premium_Hours_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Hours_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Hours_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`LPM_SERVICE_LINE`, starts_with("Premium_Hours_Percentage"))


# Premium Pay FTE Variance from Target Average
#sub calculations
Actual_Premium_Pay_FTEs_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Actual Premium Pay FTEs", summary_type = "mean", "LPM_SERVICE_LINE")
Premium_Pay_FTEs_Target_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Premium Pay FTEs Target", summary_type = "mean", "LPM_SERVICE_LINE")
# Premium Pay FTE Variance from Target: Actual Premium Pay FTEs - Premium Pay FTEs Target
Premium_Pay_FTE_Variance_calc_rollup_lpm <- Actual_Premium_Pay_FTEs_rollup_lpm %>%
  inner_join(Premium_Pay_FTEs_Target_rollup_lpm, by = "LPM_SERVICE_LINE") %>%
  mutate(
    Premium_Pay_FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    Premium_Pay_FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    Premium_Pay_FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`LPM_SERVICE_LINE`, starts_with("Premium_Pay_FTE_Variance"))

# OT Pay % of Worked LE Average (NOT PAID)
OT_Pay_pct_Worked_LE_avg_rollup_lpm <- OT_expense_avg_rollup_lpm %>%
  inner_join(Worked_LE_avg_rollup_lpm, by = "LPM_SERVICE_LINE") %>%
  mutate(
    OT_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    OT_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    OT_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(`LPM_SERVICE_LINE`, starts_with("OT_Pay_Percentage"))

# OT Pay % of Worked LE Median (NOT PAID)
OT_Pay_pct_Worked_LE_med_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "OT Pay % of Worked LE", summary_type = "median", "LPM_SERVICE_LINE")

# Productivity Index Median
Productivity_Index_med_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Worked Hours Productivity Index", summary_type = "median", "LPM_SERVICE_LINE")

# Labor Expense Index Median
LE_Index_med_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Labor Expense Index", summary_type = "median", "LPM_SERVICE_LINE")

#Labor Expense Variance Median
LE_Variance_med_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Labor Expense Variance", summary_type = "median", "LPM_SERVICE_LINE")

#FTE Variance Median
FTE_Variance_med_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Worked FTE Variance", summary_type = "median", "LPM_SERVICE_LINE")

#Premium Pay Expense Variance Median
Premium_Pay_Variance_med_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Premium Pay Expense Variance", summary_type = "median", "LPM_SERVICE_LINE")

# Premium Pay FTE Variance Median
Premium_Pay_FTE_Variance_med_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Premium Pay FTE Variance", summary_type = "median", "LPM_SERVICE_LINE")

#Premium Pay Expense Variance
Premium_Pay_Variance_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Premium Pay Expense Variance", summary_type = "mean", "LPM_SERVICE_LINE")

# Premium Pay FTE Variance
Premium_Pay_FTE_Variance_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Premium Pay FTE Variance", summary_type = "mean", "LPM_SERVICE_LINE")

#Worked FTE Average
Worked_FTE_avg_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Worked FTE", summary_type = "mean", "LPM_SERVICE_LINE")

#Target Worked FTE Average
Target_Worked_FTE_avg_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Total Target Wrked FTE", summary_type = "mean", "LPM_SERVICE_LINE")

#Target Labor Expense Average
Target_LE_avg_rollup_lpm <- calculate_metric_summary(rollups$rollup_lpm, "Target Labor Expense", summary_type = "mean", "LPM_SERVICE_LINE")

#Productivity Index Average
Productivity_Index_avg_rollup_lpm <- Target_Worked_FTE_avg_rollup_lpm %>%
  inner_join(Worked_FTE_avg_rollup_lpm, by = "LPM_SERVICE_LINE") %>%
  mutate(
    Productivity_Index_3_Periods = sprintf("%.4f", 
                                           Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    Productivity_Index_13_Periods = sprintf("%.4f", 
                                            Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    Productivity_Index_26_Periods = sprintf("%.4f", 
                                            Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(`LPM_SERVICE_LINE`, starts_with("Productivity_Index"))

# FTE Variance Average: Worked FTE - Target FTE
FTE_Variance_avg_rollup_lpm <- Worked_FTE_avg_rollup_lpm %>%
  inner_join(Target_Worked_FTE_avg_rollup_lpm, by = "LPM_SERVICE_LINE") %>%
  mutate(
    FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`LPM_SERVICE_LINE`, starts_with("FTE_Variance"))

# LE Index Average: Target LE / Worked LE
LE_Index_avg_rollup_lpm <- Target_LE_avg_rollup_lpm %>%
  inner_join(Worked_LE_avg_rollup_lpm, by = "LPM_SERVICE_LINE") %>%
  mutate(
    LE_Index_3_Periods = sprintf("%.4f", Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    LE_Index_13_Periods = sprintf("%.4f", Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    LE_Index_26_Periods = sprintf("%.4f", Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(`LPM_SERVICE_LINE`, starts_with("LE_Index"))

# LE Variance Average: Worked LE - Target LE
LE_Variance_avg_rollup_lpm <- Worked_LE_avg_rollup_lpm %>%
  inner_join(Target_LE_avg_rollup_lpm, by = "LPM_SERVICE_LINE") %>%
  mutate(
    LE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    LE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    LE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(`LPM_SERVICE_LINE`, starts_with("LE_Variance"))

# Correlation Coefficient (Staffing to Volume)
correlation_result_rollup_lpm <- calculate_metric_correlation(rollups$rollup_lpm, "Total Target Wrked FTE", "Actual Worked FTE", "LPM_SERVICE_LINE")

# Apply function to calculate the slope for Worked Hours Productivity Index
Worked_Hours_Prod_Slope_rollup_lpm <- calculate_slope(rollups$rollup_lpm, "Target to Worked FTE ratio", "LPM_SERVICE_LINE")
LE_Index_Slope_rollup_lpm <- calculate_slope(rollups$rollup_lpm, "Target to Worked LE ratio", "LPM_SERVICE_LINE")
Worked_Hours_FTE_Variance_Slope_rollup_lpm <- calculate_slope(rollups$rollup_lpm, "Worked FTE Variance", "LPM_SERVICE_LINE")
LE_Variance_Slope_rollup_lpm <- calculate_slope(rollups$rollup_lpm, "Labor Expense Variance", "LPM_SERVICE_LINE")
Premium_Hours_pct_Worked_hours_Slope_rollup_lpm <- calculate_slope(rollups$rollup_lpm, "Premium Hours % of Worked Hours", "LPM_SERVICE_LINE")

# Apply y intercept function
Worked_Hours_PI_Intercepts_rollup_lpm <- calculate_intercept(rollups$rollup_lpm, "Target to Worked FTE ratio", "LPM_SERVICE_LINE")
LE_PI_Intercepts_rollup_lpm <- calculate_intercept(rollups$rollup_lpm, "Target to Worked LE ratio", "LPM_SERVICE_LINE")
Premium_Hours_pct_Worked_hours_Intercepts_rollup_lpm <- calculate_intercept(rollups$rollup_lpm, "Premium Hours % of Worked Hours", "LPM_SERVICE_LINE")

#Applying standard deviation function
PI_stdv_rollup_lpm <- calculate_metric_sd(rollups$rollup_lpm, "Target to Worked FTE ratio", "LPM_SERVICE_LINE")
LE_stdv_rollup_lpm <- calculate_metric_sd(rollups$rollup_lpm, "Target to Worked LE ratio", "LPM_SERVICE_LINE")
FTE_Variance_stdv_rollup_lpm <- calculate_metric_sd(rollups$rollup_lpm, "FTE Variance", "LPM_SERVICE_LINE")
LE_Variance_stdv_rollup_lpm <- calculate_metric_sd(rollups$rollup_lpm, "Labor Expense Variance", "LPM_SERVICE_LINE")

#Applying min max and range function
PI_min_max_range_rollup_lpm <- calculate_metric_min_max_range(rollups$rollup_lpm, "Target to Worked FTE ratio", "LPM_SERVICE_LINE")
LE_min_max_range_rollup_lpm <- calculate_metric_min_max_range(rollups$rollup_lpm, "Target to Worked LE ratio", "LPM_SERVICE_LINE")
FTE_Variance_min_max_range_rollup_lpm <- calculate_metric_min_max_range(rollups$rollup_lpm, "FTE Variance", "LPM_SERVICE_LINE")
LE_Variance_min_max_range_rollup_lpm <- calculate_metric_min_max_range(rollups$rollup_lpm, "Labor Expense Variance", "LPM_SERVICE_LINE")


#-----Multiple grouping column functions-----------------------------
#Function to calculate average of last 3, 13 and 26 pay periods. User specifies the metric.
calculate_metric_summary_2 <- function(data, metric, summary_type = "mean", group_by_columns = c("Department CODE")) {
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
  
  # Group by the two columns
  data_grouped <- data %>% group_by(across(all_of(group_by_columns)))
  
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
calculate_metric_correlation_2 <- function(data, metric1, metric2, group_by_columns = c("Department CODE")) {
  
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
    group_by(across(all_of(group_by_columns))) %>%
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

# Function to calculate the linear regression slope of Worked Hours Productivity Index
calculate_slope_2 <- function(data, metric, group_by_columns = c("Department CODE")) {
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
  
  # Function to calculate slope
  calculate_slope <- function(x) {
    valid_values <- x[!is.na(x) & x != 0]
    if (length(valid_values) < 2) return(NA) # Not enough data to calculate slope
    return(lm(valid_values ~ seq_along(valid_values))$coefficients[2]) # Slope calculation
  }
  
  # Group by the two columns
  data_grouped <- data %>% group_by(across(all_of(group_by_columns)))
  
  # Calculate slopes for each department based on the chosen metric
  slopes <- data_grouped %>% summarise(
    Slope_Last_3_Periods = calculate_slope(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3))))),
    Slope_Last_13_Periods = calculate_slope(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13))))),
    Slope_Last_26_Periods = calculate_slope(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26))))),
    .groups = "drop"
  )
  
  return(slopes)
}

calculate_intercept_2 <- function(data, metric, group_by_columns = c("Department CODE")) {
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
  
  # Function to calculate y-intercept
  calculate_intercept <- function(x) {
    valid_values <- x[!is.na(x) & x != 0]
    if (length(valid_values) < 2) return(NA) # Not enough data to calculate intercept
    model <- lm(valid_values ~ seq_along(valid_values))
    return(model$coefficients[1]) # Intercept calculation
  }
  
  # Group by the two columns
  data_grouped <- data %>% group_by(across(all_of(group_by_columns)))
  
  # Calculate intercepts for each department based on the chosen metric
  intercepts <- data_grouped %>% summarise(
    Intercept_Last_3_Periods = calculate_intercept(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3))))),
    Intercept_Last_13_Periods = calculate_intercept(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13))))),
    Intercept_Last_26_Periods = calculate_intercept(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26))))),
    .groups = "drop"
  )
  
  return(intercepts)
}

#Function to calculate standard deviation
calculate_metric_sd_2 <- function(data, metric, group_by_columns = c("Department CODE")) {
  
  colnames(data) <- trimws(colnames(data))
  
  # Find columns for the specified metric
  metric_columns <- names(data)[grepl(paste0(" ", metric, "$"), names(data))]
  
  # Replace blanks with NA
  data[data == ""] <- NA
  
  # Group by Department CODE and calculate the standard deviation for the last 3, 13, and 26 periods
  sd_result <- data %>%
    group_by(across(all_of(group_by_columns))) %>%
    summarise(
      SD_Last_3_Periods = sd(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3)))), na.rm = TRUE),
      SD_Last_13_Periods = sd(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13)))), na.rm = TRUE),
      SD_Last_26_Periods = sd(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26)))), na.rm = TRUE),
      .groups = "drop"
    )
  
  return(sd_result)
}

#Min Max and Range function
calculate_metric_min_max_range_2 <- function(data, metric, group_by_columns = c("Department CODE")) {
  
  colnames(data) <- trimws(colnames(data))
  
  # Find columns for the specified metric
  metric_columns <- names(data)[grepl(paste0(" ", metric, "$"), names(data))]
  
  # Replace blanks with NA
  data[data == ""] <- NA
  
  # Group by Department CODE and calculate the min, max, and range for the last 3, 13, and 26 periods
  min_max_range_result <- data %>%
    group_by(across(all_of(group_by_columns))) %>%
    summarise(
      Min_Last_3_Periods = min(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3)))), na.rm = TRUE),
      Max_Last_3_Periods = max(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3)))), na.rm = TRUE),
      Range_Last_3_Periods = Max_Last_3_Periods - Min_Last_3_Periods,
      
      Min_Last_13_Periods = min(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13)))), na.rm = TRUE),
      Max_Last_13_Periods = max(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13)))), na.rm = TRUE),
      Range_Last_13_Periods = Max_Last_13_Periods - Min_Last_13_Periods,
      
      Min_Last_26_Periods = min(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26)))), na.rm = TRUE),
      Max_Last_26_Periods = max(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26)))), na.rm = TRUE),
      Range_Last_26_Periods = Max_Last_26_Periods - Min_Last_26_Periods,
      
      .groups = "drop"
    )
  
  return(min_max_range_result)
}

#---------Applying 2 column functions to the 2 rollups-----------------
#-----------SITE and CORPORATE_SERVICE_LINE ROLLUP------------------------------------------------
# Premium Pay Spend average (OT + Agency)
Premium_Pay_avg_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Premium Pay Expense", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))

# Premium Pay Spend median (OT + Agency)
Premium_Pay_med_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Premium Pay Expense", summary_type = "median", c("SITE", "CORPORATE_SERVICE_LINE"))

# Premium Pay Hours average (OT + Agency)
Premium_Pay_hours_avg_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Premium Pay Hours", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))

# Premium Pay Hours median (OT + Agency)
Premium_Pay_hours_med_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Premium Pay Hours", summary_type = "median", c("SITE", "CORPORATE_SERVICE_LINE"))

# Worked Hours average
Worked_hours_avg_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Actual Worked Hours", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))

# Overtime Pay Spend average
OT_expense_avg_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Overtime Labor Expense", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))

# Overtime Pay Spend median
OT_expense_med_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Overtime Labor Expense", summary_type = "median", c("SITE", "CORPORATE_SERVICE_LINE"))

# Paid LE Average
Paid_LE_avg_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Actual Paid Labor Expense", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))

# Worked LE average (sub-calculation)
Worked_LE_avg_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Worked Expenses", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))

# Hourly Rate Average 
Hourly_rate_avg_rollup_site_corp <- Worked_LE_avg_rollup_site_corp %>%
  inner_join(Worked_hours_avg_rollup_site_corp, by = c("SITE", "CORPORATE_SERVICE_LINE")) %>%
  mutate(
    Hourly_rate_avg_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y), 2),
    Hourly_rate_avg_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y), 2),
    Hourly_rate_avg_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y), 2)
  ) %>%
  select(SITE, CORPORATE_SERVICE_LINE, starts_with("Hourly_rate_avg"))

# Premium Pay % of Worked LE Average
Premium_Pay_pct_Worked_LE_avg_rollup_site_corp <- Premium_Pay_avg_rollup_site_corp %>%
  inner_join(Worked_LE_avg_rollup_site_corp, by = c("SITE", "CORPORATE_SERVICE_LINE")) %>%
  mutate(
    Premium_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(SITE, CORPORATE_SERVICE_LINE, starts_with("Premium_Pay_Percentage"))

# Premium Pay % of Worked LE Median
Premium_Pay_pct_Worked_LE_med_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Premium Pay % of Worked LE", summary_type = "median", c("SITE", "CORPORATE_SERVICE_LINE"))

# Premium Hours % of Worked Hours Average
Premium_Hours_pct_Worked_hours_rollup_site_corp <- Premium_Pay_hours_avg_rollup_site_corp %>%
  inner_join(Worked_hours_avg_rollup_site_corp, by = c("SITE", "CORPORATE_SERVICE_LINE")) %>%
  mutate(
    Premium_Hours_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Hours_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Hours_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(SITE, CORPORATE_SERVICE_LINE, starts_with("Premium_Hours_Percentage"))


# Premium Pay FTE Variance from Target Average
#sub calculations
Actual_Premium_Pay_FTEs_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Actual Premium Pay FTEs", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))
Premium_Pay_FTEs_Target_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Premium Pay FTEs Target", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))
# Premium Pay FTE Variance from Target: Actual Premium Pay FTEs - Premium Pay FTEs Target
Premium_Pay_FTE_Variance_calc_rollup_site_corp <- Actual_Premium_Pay_FTEs_rollup_site_corp %>%
  inner_join(Premium_Pay_FTEs_Target_rollup_site_corp, by = c("SITE", "CORPORATE_SERVICE_LINE")) %>%
  mutate(
    Premium_Pay_FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    Premium_Pay_FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    Premium_Pay_FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(SITE, CORPORATE_SERVICE_LINE, starts_with("Premium_Pay_FTE_Variance"))

# OT Pay % of Worked LE Average (NOT PAID)
OT_Pay_pct_Worked_LE_avg_rollup_site_corp <- OT_expense_avg_rollup_site_corp %>%
  inner_join(Worked_LE_avg_rollup_site_corp, by = c("SITE", "CORPORATE_SERVICE_LINE")) %>%
  mutate(
    OT_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    OT_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    OT_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(SITE, CORPORATE_SERVICE_LINE, starts_with("OT_Pay_Percentage"))

# OT Pay % of Worked LE Median (NOT PAID)
OT_Pay_pct_Worked_LE_med_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "OT Pay % of Worked LE", summary_type = "median", c("SITE", "CORPORATE_SERVICE_LINE"))

# Productivity Index Median
Productivity_Index_med_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Worked Hours Productivity Index", summary_type = "median", c("SITE", "CORPORATE_SERVICE_LINE"))

# Labor Expense Index Median
LE_Index_med_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Labor Expense Index", summary_type = "median", c("SITE", "CORPORATE_SERVICE_LINE"))

#Labor Expense Variance Median
LE_Variance_med_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Labor Expense Variance", summary_type = "median", c("SITE", "CORPORATE_SERVICE_LINE"))

#FTE Variance Median
FTE_Variance_med_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Worked FTE Variance", summary_type = "median", c("SITE", "CORPORATE_SERVICE_LINE"))

#Premium Pay Expense Variance Median
Premium_Pay_Variance_med_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Premium Pay Expense Variance", summary_type = "median", c("SITE", "CORPORATE_SERVICE_LINE"))

# Premium Pay FTE Variance Median
Premium_Pay_FTE_Variance_med_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Premium Pay FTE Variance", summary_type = "median", c("SITE", "CORPORATE_SERVICE_LINE"))

#Premium Pay Expense Variance
Premium_Pay_Variance_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Premium Pay Expense Variance", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))

# Premium Pay FTE Variance
Premium_Pay_FTE_Variance_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Premium Pay FTE Variance", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))

#Worked FTE Average
Worked_FTE_avg_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Worked FTE", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))

#Target Worked FTE Average
Target_Worked_FTE_avg_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Total Target Wrked FTE", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))

#Target Labor Expense Average
Target_LE_avg_rollup_site_corp <- calculate_metric_summary_2(rollups$rollup_site_corp, "Target Labor Expense", summary_type = "mean", c("SITE", "CORPORATE_SERVICE_LINE"))

#Productivity Index Average
Productivity_Index_avg_rollup_site_corp <- Target_Worked_FTE_avg_rollup_site_corp %>%
  inner_join(Worked_FTE_avg_rollup_site_corp, by = c("SITE", "CORPORATE_SERVICE_LINE")) %>%
  mutate(
    Productivity_Index_3_Periods = sprintf("%.4f", 
                                           Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    Productivity_Index_13_Periods = sprintf("%.4f", 
                                            Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    Productivity_Index_26_Periods = sprintf("%.4f", 
                                            Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(SITE, CORPORATE_SERVICE_LINE, starts_with("Productivity_Index"))

# FTE Variance Average: Worked FTE - Target FTE
FTE_Variance_avg_rollup_site_corp <- Worked_FTE_avg_rollup_site_corp %>%
  inner_join(Target_Worked_FTE_avg_rollup_site_corp, by = c("SITE", "CORPORATE_SERVICE_LINE")) %>%
  mutate(
    FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(SITE, CORPORATE_SERVICE_LINE, starts_with("FTE_Variance"))

# LE Index Average: Target LE / Worked LE
LE_Index_avg_rollup_site_corp <- Target_LE_avg_rollup_site_corp %>%
  inner_join(Worked_LE_avg_rollup_site_corp, by = c("SITE", "CORPORATE_SERVICE_LINE")) %>%
  mutate(
    LE_Index_3_Periods = sprintf("%.4f", Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    LE_Index_13_Periods = sprintf("%.4f", Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    LE_Index_26_Periods = sprintf("%.4f", Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(SITE, CORPORATE_SERVICE_LINE, starts_with("LE_Index"))

# LE Variance Average: Worked LE - Target LE
LE_Variance_avg_rollup_site_corp <- Worked_LE_avg_rollup_site_corp %>%
  inner_join(Target_LE_avg_rollup_site_corp, by = c("SITE", "CORPORATE_SERVICE_LINE")) %>%
  mutate(
    LE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    LE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    LE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(SITE, CORPORATE_SERVICE_LINE, starts_with("LE_Variance"))

# Correlation Coefficient (Staffing to Volume)
correlation_result_rollup_site_corp <- calculate_metric_correlation_2(rollups$rollup_site_corp, "Total Target Wrked FTE", "Actual Worked FTE", c("SITE", "CORPORATE_SERVICE_LINE"))

rollups$rollup_site_corp <- drop_na(rollups$rollup_site_corp)

# Apply function to calculate the slope for Worked Hours Productivity Index
Worked_Hours_Prod_Slope_rollup_site_corp <- calculate_slope_2(rollups$rollup_site_corp, "Target to Worked FTE ratio", c("SITE", "CORPORATE_SERVICE_LINE"))
LE_Index_Slope_rollup_site_corp <- calculate_slope_2(rollups$rollup_site_corp, "Target to Worked LE ratio", c("SITE", "CORPORATE_SERVICE_LINE"))
Worked_Hours_FTE_Variance_Slope_rollup_site_corp <- calculate_slope_2(rollups$rollup_site_corp, "Worked FTE Variance", c("SITE", "CORPORATE_SERVICE_LINE"))
LE_Variance_Slope_rollup_site_corp <- calculate_slope_2(rollups$rollup_site_corp, "Labor Expense Variance", c("SITE", "CORPORATE_SERVICE_LINE"))
Premium_Hours_pct_Worked_hours_Slope_rollup_site_corp <- calculate_slope_2(rollups$rollup_site_corp, "Premium Hours % of Worked Hours", c("SITE", "CORPORATE_SERVICE_LINE"))

# Apply y intercept function
Worked_Hours_PI_Intercepts_rollup_site_corp <- calculate_intercept_2(rollups$rollup_site_corp, "Target to Worked FTE ratio", c("SITE", "CORPORATE_SERVICE_LINE"))
LE_PI_Intercepts_rollup_site_corp <- calculate_intercept_2(rollups$rollup_site_corp, "Target to Worked LE ratio", c("SITE", "CORPORATE_SERVICE_LINE"))
Premium_Hours_pct_Worked_hours_Intercepts_rollup_site_corp <- calculate_intercept_2(rollups$rollup_site_corp, "Premium Hours % of Worked Hours", c("SITE", "CORPORATE_SERVICE_LINE"))

#Applying standard deviation function
PI_stdv_rollup_site_corp <- calculate_metric_sd_2(rollups$rollup_site_corp, "Target to Worked FTE ratio", c("SITE", "CORPORATE_SERVICE_LINE"))
LE_stdv_rollup_site_corp <- calculate_metric_sd_2(rollups$rollup_site_corp, "Target to Worked LE ratio", c("SITE", "CORPORATE_SERVICE_LINE"))
FTE_Variance_stdv_rollup_site_corp <- calculate_metric_sd_2(rollups$rollup_site_corp, "FTE Variance", c("SITE", "CORPORATE_SERVICE_LINE"))
LE_Variance_stdv_rollup_site_corp <- calculate_metric_sd_2(rollups$rollup_site_corp, "Labor Expense Variance", c("SITE", "CORPORATE_SERVICE_LINE"))

#Applying min max and range function
PI_min_max_range_rollup_site_corp <- calculate_metric_min_max_range_2(rollups$rollup_site_corp, "Target to Worked FTE ratio", c("SITE", "CORPORATE_SERVICE_LINE"))
LE_min_max_range_rollup_site_corp <- calculate_metric_min_max_range_2(rollups$rollup_site_corp, "Target to Worked LE ratio", c("SITE", "CORPORATE_SERVICE_LINE"))
FTE_Variance_min_max_range_rollup_site_corp <- calculate_metric_min_max_range_2(rollups$rollup_site_corp, "FTE Variance", c("SITE", "CORPORATE_SERVICE_LINE"))
LE_Variance_min_max_range_rollup_site_corp <- calculate_metric_min_max_range_2(rollups$rollup_site_corp, "Labor Expense Variance", c("SITE", "CORPORATE_SERVICE_LINE"))

#-----------SITE and LPM_SERVICE_LINE ROLLUP------------------------------------------------
# Premium Pay Spend average (OT + Agency)
Premium_Pay_avg_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Premium Pay Expense", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))

# Premium Pay Spend median (OT + Agency)
Premium_Pay_med_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Premium Pay Expense", summary_type = "median", c("SITE", "LPM_SERVICE_LINE"))

# Premium Pay Hours average (OT + Agency)
Premium_Pay_hours_avg_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Premium Pay Hours", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))

# Premium Pay Hours median (OT + Agency)
Premium_Pay_hours_med_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Premium Pay Hours", summary_type = "median", c("SITE", "LPM_SERVICE_LINE"))

# Worked Hours average
Worked_hours_avg_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Actual Worked Hours", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))

# Overtime Pay Spend average
OT_expense_avg_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Overtime Labor Expense", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))

# Overtime Pay Spend median
OT_expense_med_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Overtime Labor Expense", summary_type = "median", c("SITE", "LPM_SERVICE_LINE"))

# Paid LE Average
Paid_LE_avg_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Actual Paid Labor Expense", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))

# Worked LE average (sub-calculation)
Worked_LE_avg_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Worked Expenses", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))

# Hourly Rate Average 
Hourly_rate_avg_rollup_site_lpm <- Worked_LE_avg_rollup_site_lpm %>%
  inner_join(Worked_hours_avg_rollup_site_lpm, by = c("SITE", "LPM_SERVICE_LINE")) %>%
  mutate(
    Hourly_rate_avg_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y), 2),
    Hourly_rate_avg_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y), 2),
    Hourly_rate_avg_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y), 2)
  ) %>%
  select(SITE, LPM_SERVICE_LINE, starts_with("Hourly_rate_avg"))

# Premium Pay % of Worked LE Average
Premium_Pay_pct_Worked_LE_avg_rollup_site_lpm <- Premium_Pay_avg_rollup_site_lpm %>%
  inner_join(Worked_LE_avg_rollup_site_lpm, by = c("SITE", "LPM_SERVICE_LINE")) %>%
  mutate(
    Premium_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(SITE, LPM_SERVICE_LINE, starts_with("Premium_Pay_Percentage"))

# Premium Pay % of Worked LE Median
Premium_Pay_pct_Worked_LE_med_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Premium Pay % of Worked LE", summary_type = "median", c("SITE", "LPM_SERVICE_LINE"))

# Premium Hours % of Worked Hours Average
Premium_Hours_pct_Worked_hours_rollup_site_lpm <- Premium_Pay_hours_avg_rollup_site_lpm %>%
  inner_join(Worked_hours_avg_rollup_site_lpm, by = c("SITE", "LPM_SERVICE_LINE")) %>%
  mutate(
    Premium_Hours_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    Premium_Hours_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    Premium_Hours_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(SITE, LPM_SERVICE_LINE, starts_with("Premium_Hours_Percentage"))


# Premium Pay FTE Variance from Target Average
#sub calculations
Actual_Premium_Pay_FTEs_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Actual Premium Pay FTEs", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))
Premium_Pay_FTEs_Target_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Premium Pay FTEs Target", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))
# Premium Pay FTE Variance from Target: Actual Premium Pay FTEs - Premium Pay FTEs Target
Premium_Pay_FTE_Variance_calc_rollup_site_lpm <- Actual_Premium_Pay_FTEs_rollup_site_lpm %>%
  inner_join(Premium_Pay_FTEs_Target_rollup_site_lpm, by = c("SITE", "LPM_SERVICE_LINE")) %>%
  mutate(
    Premium_Pay_FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    Premium_Pay_FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    Premium_Pay_FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(SITE, LPM_SERVICE_LINE, starts_with("Premium_Pay_FTE_Variance"))

# OT Pay % of Worked LE Average (NOT PAID)
OT_Pay_pct_Worked_LE_avg_rollup_site_lpm <- OT_expense_avg_rollup_site_lpm %>%
  inner_join(Worked_LE_avg_rollup_site_lpm, by = c("SITE", "LPM_SERVICE_LINE")) %>%
  mutate(
    OT_Pay_Percentage_3_Periods = round((Average_Last_3_Periods.x / Average_Last_3_Periods.y) * 100, 2),
    OT_Pay_Percentage_13_Periods = round((Average_Last_13_Periods.x / Average_Last_13_Periods.y) * 100, 2),
    OT_Pay_Percentage_26_Periods = round((Average_Last_26_Periods.x / Average_Last_26_Periods.y) * 100, 2)
  ) %>%
  select(SITE, LPM_SERVICE_LINE, starts_with("OT_Pay_Percentage"))

# OT Pay % of Worked LE Median (NOT PAID)
OT_Pay_pct_Worked_LE_med_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "OT Pay % of Worked LE", summary_type = "median", c("SITE", "LPM_SERVICE_LINE"))

# Productivity Index Median
Productivity_Index_med_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Worked Hours Productivity Index", summary_type = "median", c("SITE", "LPM_SERVICE_LINE"))

# Labor Expense Index Median
LE_Index_med_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Labor Expense Index", summary_type = "median", c("SITE", "LPM_SERVICE_LINE"))

#Labor Expense Variance Median
LE_Variance_med_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Labor Expense Variance", summary_type = "median", c("SITE", "LPM_SERVICE_LINE"))

#FTE Variance Median
FTE_Variance_med_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Worked FTE Variance", summary_type = "median", c("SITE", "LPM_SERVICE_LINE"))

#Premium Pay Expense Variance Median
Premium_Pay_Variance_med_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Premium Pay Expense Variance", summary_type = "median", c("SITE", "LPM_SERVICE_LINE"))

# Premium Pay FTE Variance Median
Premium_Pay_FTE_Variance_med_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Premium Pay FTE Variance", summary_type = "median", c("SITE", "LPM_SERVICE_LINE"))

#Premium Pay Expense Variance
Premium_Pay_Variance_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Premium Pay Expense Variance", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))

# Premium Pay FTE Variance
Premium_Pay_FTE_Variance_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Premium Pay FTE Variance", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))

#Worked FTE Average
Worked_FTE_avg_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Worked FTE", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))

#Target Worked FTE Average
Target_Worked_FTE_avg_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Total Target Wrked FTE", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))

#Target Labor Expense Average
Target_LE_avg_rollup_site_lpm <- calculate_metric_summary_2(rollups$rollup_site_lpm, "Target Labor Expense", summary_type = "mean", c("SITE", "LPM_SERVICE_LINE"))

#Productivity Index Average
Productivity_Index_avg_rollup_site_lpm <- Target_Worked_FTE_avg_rollup_site_lpm %>%
  inner_join(Worked_FTE_avg_rollup_site_lpm, by = c("SITE", "LPM_SERVICE_LINE")) %>%
  mutate(
    Productivity_Index_3_Periods = sprintf("%.4f", 
                                           Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    Productivity_Index_13_Periods = sprintf("%.4f", 
                                            Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    Productivity_Index_26_Periods = sprintf("%.4f", 
                                            Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(SITE, LPM_SERVICE_LINE, starts_with("Productivity_Index"))

# FTE Variance Average: Worked FTE - Target FTE
FTE_Variance_avg_rollup_site_lpm <- Worked_FTE_avg_rollup_site_lpm %>%
  inner_join(Target_Worked_FTE_avg_rollup_site_lpm, by = c("SITE", "LPM_SERVICE_LINE")) %>%
  mutate(
    FTE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    FTE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    FTE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(SITE, LPM_SERVICE_LINE, starts_with("FTE_Variance"))

# LE Index Average: Target LE / Worked LE
LE_Index_avg_rollup_site_lpm <- Target_LE_avg_rollup_site_lpm %>%
  inner_join(Worked_LE_avg_rollup_site_lpm, by = c("SITE", "LPM_SERVICE_LINE")) %>%
  mutate(
    LE_Index_3_Periods = sprintf("%.4f", Average_Last_3_Periods.x / Average_Last_3_Periods.y),
    LE_Index_13_Periods = sprintf("%.4f", Average_Last_13_Periods.x / Average_Last_13_Periods.y),
    LE_Index_26_Periods = sprintf("%.4f", Average_Last_26_Periods.x / Average_Last_26_Periods.y)
  ) %>%
  select(SITE, LPM_SERVICE_LINE, starts_with("LE_Index"))

# LE Variance Average: Worked LE - Target LE
LE_Variance_avg_rollup_site_lpm <- Worked_LE_avg_rollup_site_lpm %>%
  inner_join(Target_LE_avg_rollup_site_lpm, by = c("SITE", "LPM_SERVICE_LINE")) %>%
  mutate(
    LE_Variance_3_Periods = sprintf("%.2f", Average_Last_3_Periods.x - Average_Last_3_Periods.y),
    LE_Variance_13_Periods = sprintf("%.2f", Average_Last_13_Periods.x - Average_Last_13_Periods.y),
    LE_Variance_26_Periods = sprintf("%.2f", Average_Last_26_Periods.x - Average_Last_26_Periods.y)
  ) %>%
  select(SITE, LPM_SERVICE_LINE, starts_with("LE_Variance"))

# Correlation Coefficient (Staffing to Volume)
correlation_result_rollup_site_lpm <- calculate_metric_correlation_2(rollups$rollup_site_lpm, "Total Target Wrked FTE", "Actual Worked FTE", c("SITE", "LPM_SERVICE_LINE"))

# Apply function to calculate the slope for Worked Hours Productivity Index
Worked_Hours_Prod_Slope_rollup_site_lpm <- calculate_slope_2(rollups$rollup_site_lpm, "Target to Worked FTE ratio", c("SITE", "LPM_SERVICE_LINE"))
LE_Index_Slope_rollup_site_lpm <- calculate_slope_2(rollups$rollup_site_lpm, "Target to Worked LE ratio", c("SITE", "LPM_SERVICE_LINE"))
Worked_Hours_FTE_Variance_Slope_rollup_site_lpm <- calculate_slope_2(rollups$rollup_site_lpm, "Worked FTE Variance", c("SITE", "LPM_SERVICE_LINE"))
LE_Variance_Slope_rollup_site_lpm <- calculate_slope_2(rollups$rollup_site_lpm, "Labor Expense Variance", c("SITE", "LPM_SERVICE_LINE"))
Premium_Hours_pct_Worked_hours_Slope_rollup_site_lpm <- calculate_slope_2(rollups$rollup_site_lpm, "Premium Hours % of Worked Hours", c("SITE", "LPM_SERVICE_LINE"))

# Apply y intercept function
Worked_Hours_PI_Intercepts_rollup_site_lpm <- calculate_intercept_2(rollups$rollup_site_lpm, "Target to Worked FTE ratio", c("SITE", "LPM_SERVICE_LINE"))
LE_PI_Intercepts_rollup_site_lpm <- calculate_intercept_2(rollups$rollup_site_lpm, "Target to Worked LE ratio", c("SITE", "LPM_SERVICE_LINE"))
Premium_Hours_pct_Worked_hours_Intercepts_rollup_site_lpm <- calculate_intercept_2(rollups$rollup_site_lpm, "Premium Hours % of Worked Hours", c("SITE", "LPM_SERVICE_LINE"))

#Applying standard deviation function
PI_stdv_rollup_site_lpm <- calculate_metric_sd_2(rollups$rollup_site_lpm, "Target to Worked FTE ratio", c("SITE", "LPM_SERVICE_LINE"))
LE_stdv_rollup_site_lpm <- calculate_metric_sd_2(rollups$rollup_site_lpm, "Target to Worked LE ratio", c("SITE", "LPM_SERVICE_LINE"))
FTE_Variance_stdv_rollup_site_lpm <- calculate_metric_sd_2(rollups$rollup_site_lpm, "FTE Variance", c("SITE", "LPM_SERVICE_LINE"))
LE_Variance_stdv_rollup_site_lpm <- calculate_metric_sd_2(rollups$rollup_site_lpm, "Labor Expense Variance", c("SITE", "LPM_SERVICE_LINE"))

#Applying min max and range function
PI_min_max_range_rollup_site_lpm <- calculate_metric_min_max_range_2(rollups$rollup_site_lpm, "Target to Worked FTE ratio", c("SITE", "LPM_SERVICE_LINE"))
LE_min_max_range_rollup_site_lpm <- calculate_metric_min_max_range_2(rollups$rollup_site_lpm, "Target to Worked LE ratio", c("SITE", "LPM_SERVICE_LINE"))
FTE_Variance_min_max_range_rollup_site_lpm <- calculate_metric_min_max_range_2(rollups$rollup_site_lpm, "FTE Variance", c("SITE", "LPM_SERVICE_LINE"))
LE_Variance_min_max_range_rollup_site_lpm <- calculate_metric_min_max_range_2(rollups$rollup_site_lpm, "Labor Expense Variance", c("SITE", "LPM_SERVICE_LINE"))
#--------------

# Data Formatting ---------------------------------------------------------
# How the data will look during the output of the script.
# For example, if you have a data table that needs the numbers to show up as
# green or red depending on whether they meet a certain threshold.
# Function to calculate metrics for each rollup dataframe
# Assuming 'rollups$rollup_site' is the dataframe you want to work with


# Quality Checks ----------------------------------------------------------
# Checks that are performed on the output to confirm data consistency and


# Visualization -----------------------------------------------------------
# How the data will be plotted or how the data table will look including axis
# titles, scales, and color schemes of graphs or data tables.

# Updated list of relevant data frames to combine, including new data frames
dfs <- list(
  Worked_FTE_avg, Paid_LE_avg, Target_Worked_FTE_avg, Target_LE_avg,
  Productivity_Index_avg, FTE_Variance_avg, LE_Index_avg, LE_Variance_avg,
  Premium_Pay_avg, Premium_Pay_pct_Worked_LE_avg, OT_expense_avg, 
  OT_Pay_pct_Worked_LE_avg, Productivity_Index_med, FTE_Variance_med,
  Premium_Pay_Variance_med, LE_Index_med, LE_Variance_med, Premium_Pay_med, 
  Premium_Pay_pct_Worked_LE_med, OT_expense_med, OT_Pay_pct_Worked_LE_med, 
  correlation_result, Worked_Hours_Prod_Slope, LE_Index_Slope,
  Worked_Hours_PI_Intercepts, LE_PI_Intercepts, PI_stdv, LE_stdv, 
  FTE_Variance_stdv, LE_Variance_stdv, PI_min_max_range, LE_min_max_range, 
  FTE_Variance_min_max_range, LE_Variance_min_max_range, Premium_Hours_pct_Worked_hours,
  Actual_Premium_Pay_FTEs, Premium_Pay_FTE_Variance_calc, Premium_Pay_Variance,
  Worked_Hours_FTE_Variance_Slope, LE_Variance_Slope, Hourly_rate_avg, 
  Premium_Hours_pct_Worked_hours_Slope, Premium_Hours_pct_Worked_hours_Intercepts
)

# Updated metric names to match additional data frames
metric_names <- c(
  "Worked_FTE", "Paid_LE", "Target_Worked_FTE", "Target_LE", 
  "Productivity_Index", "FTE_Variance", "LE_Index", "LE_Variance", 
  "Premium_Pay", "Premium_Pay_pct_Worked_LE", "OT_expense", 
  "OT_Pay_pct_Worked_LE", "Productivity_Index_med", "FTE_Variance_med",
  "Premium_Pay_Variance_med", "LE_Index_med", "LE_Variance_med", 
  "Premium_Pay_med", "Premium_Pay_pct_Worked_LE_med", "OT_expense_med", 
  "OT_Pay_pct_Worked_LE_med", 
  "correlation_result", "Worked_Hours_Prod_Slope", "LE_Index_Slope",
  "Worked_Hours_PI_Intercepts", "LE_PI_Intercepts", "PI_stdv", "LE_stdv", 
  "FTE_Variance_stdv", "LE_Variance_stdv", "PI_min_max_range", 
  "LE_min_max_range", "FTE_Variance_min_max_range", "LE_Variance_min_max_range",
  "Premium_Hours_pct_Worked_Hours", 
  "Actual_Premium_Pay_FTEs", "Premium_Pay_FTE_Variance_calc", "Premium_Pay_Variance",
  "Worked_Hours_FTE_Variance_Slope", "LE_Variance_Slope", "Hourly_rate_avg",
  "Premium_Hours_pct_Worked_hours_Slope", "Premium_Hours_pct_Worked_hours_Intercepts"
)

# Apply renaming function to all data frames
renamed_dfs <- mapply(rename_columns, dfs, metric_names, SIMPLIFY = FALSE)

# Combine data frames using full join by 'Department CODE'
combined_df <- reduce(renamed_dfs, full_join, by = "Department CODE")

# Reorder columns to group by pay periods
new_column_order <- c(
  "Department CODE",
  grep("_3_Periods$", names(combined_df), value = TRUE),
  grep("_13_Periods$", names(combined_df), value = TRUE),
  grep("_26_Periods$", names(combined_df), value = TRUE)
)

combined_df <- combined_df %>% select(all_of(new_column_order))
#---------------TESTING---------------------------------------------
#-----------Updated Ranking dataframe with corrected rankings------------------
# Filter out rows where Worked_FTE_3_Periods is NA or 0
cleaned_df <- combined_df %>%
  filter(!is.na(Worked_FTE_3_Periods) & Worked_FTE_3_Periods != 0)

#New
cleaned_df <- combined_df %>%
  filter(
    !is.na(Worked_FTE_3_Periods) & Worked_FTE_3_Periods != 0,
    !is.na(Productivity_Index_3_Periods) & 
      !is.nan(Productivity_Index_3_Periods) & 
      Productivity_Index_3_Periods != 0
  )
# Create a dataframe of departments with NA or 0 in Worked_FTE_3_Periods
na_departments <- combined_df %>%
  filter(is.na(Worked_FTE_3_Periods) | Worked_FTE_3_Periods == 0) %>%
  select(`Department CODE`) %>%
  distinct()

# NA Department List
list(
  Cleaned_Data = cleaned_df,
  NA_Departments = na_departments
)

# Initialize ranked_df from combined_df
ranked_df <- cleaned_df

# List of metrics where lower values are better (DOUBLE CHECK)
lower_is_better_metrics <- c(
  "Worked_FTE", "Paid_LE", "FTE_Variance", "LE_Variance", "Premium_Pay", 
  "Premium_Pay_pct_Worked_LE", "OT_expense", "OT_Pay_pct_Worked_LE",
  "FTE_Variance_med", "Premium_Pay_Variance_med",
  "LE_Variance_med", "Premium_Pay_med", "OT_expense_med",
  "Premium_Pay_pct_Worked_LE_med", "OT_Pay_pct_Worked_LE_med", "PI_stdv",
  "LE_stdv", "FTE_Variance_stdv", "LE_Variance_stdv", "PI_min_max_range_Range", 
  "LE_min_max_range_Range", "FTE_Variance_min_max_range_Range", 
  "LE_Variance_min_max_range_Range", "PI_percentiles_Spread", 
  "LE_percentiles_Spread", "Premium_Hours_pct_Worked_Hours", 
  "Premium_Pay_FTE_Variance_calc", "Premium_Pay_Variance", 
  "Actual_Premium_Pay_FTEs", "Worked_Hours_FTE_Variance_Slope", 
  "LE_Variance_Slope", "Hourly_rate_avg", "Premium_Hours_pct_Worked_hours_Slope",
  "Premium_Hours_pct_Worked_hours_Intercepts")

# Adjust column names for 3, 13, and 26 periods
metrics_columns <- grep("_3_Periods$|_13_Periods$|_26_Periods$", names(ranked_df), value = TRUE)
lower_is_better_columns <- unlist(lapply(lower_is_better_metrics, function(metric) {
  grep(paste0("^", metric, "_(3|13|26)_Periods$"), names(ranked_df), value = TRUE)
}))

# Rank each metric column
for (metric in metrics_columns) {
  if (all(is.na(ranked_df[[metric]]))) {
    # Skip columns with all NA values
    ranked_df[[paste0(metric, "_rank")]] <- NA
  } else if (metric %in% lower_is_better_columns) {
    # Reverse ranking for lower-is-better metrics
    ranked_df[[paste0(metric, "_rank")]] <- rank(as.numeric(ranked_df[[metric]]), ties.method = "average", na.last = "keep")
  } else {
    # Normal ranking for higher-is-better metrics
    ranked_df[[paste0(metric, "_rank")]] <- rank(-as.numeric(ranked_df[[metric]]), ties.method = "average", na.last = "keep")
  }
}

# Update total_rank column
ranked_df$total_rank <- rowSums(ranked_df[, paste0(metrics_columns, "_rank")], na.rm = TRUE)

# Scale total_rank based on the number of metrics
num_metrics <- length(metrics_columns) # Number of metrics
ranked_df$scaled_rank <- ranked_df$total_rank / num_metrics

# Remove the original metric columns and keep only the rank columns
ranked_df <- ranked_df[, grep("_rank$", names(ranked_df))]
ranked_df <- cbind(`Department CODE` = cleaned_df$`Department CODE`, ranked_df)

# Update total_rank column for the 13 pay period columns only
ranked_df$total_rank_13 <- rowSums(ranked_df[, grep("_13_Periods_rank$", names(ranked_df))], na.rm = TRUE)

# Calculate the number of non-NA metrics for 13 pay periods
ranked_df$num_non_na_13 <- rowSums(!is.na(ranked_df[, grep("_13_Periods_rank$", names(ranked_df))]))

# Scale total_rank for the 13 pay period columns using non-NA count
ranked_df$scaled_rank_13 <- ranked_df$total_rank_13 / ranked_df$num_non_na_13

# Define subsets of metrics
productivity_metrics <- c("Productivity_Index", "FTE_Variance", "LE_Index", "LE_Variance")

premium_pay_metrics <- c("Premium_Hours_pct_Worked_Hours", 
                         "Premium_Pay_pct_Worked_LE",
                         "Premium_Pay_FTE_Variance_calc", 
                         "Premium_Pay_Variance",
                         "Premium_Hours_pct_Worked_hours_Slope")

spread_metrics <- c("PI_stdv", "LE_stdv", "PI_min_max_range_Range", 
                    "LE_min_max_range_Range", "correlation_result", 
                    "FTE_Variance_stdv", "LE_Variance_stdv", 
                    "FTE_Variance_min_max_range_Range", 
                    "LE_Variance_min_max_range_Range")

productivity_trend_metrics <- c("Worked_Hours_Prod_Slope", "LE_Index_Slope",
                               "Worked_Hours_FTE_Variance_Slope", 
                               "LE_Variance_Slope")

productivity_premium_pay_metrics <- c("Productivity_Index", "Premium_Hours_pct_Worked_Hours")

add_total_rank <- function(metrics, df, subset_name, periods = c(3, 13, 26)) {
  # Generate rank column names based on selected periods
  rank_columns <- paste0(metrics, "_", periods, "_Periods_rank")
  
  # Calculate the total rank by summing the ranks for the selected periods
  df[[paste0(subset_name, "_total_rank")]] <- rowSums(df[, rank_columns], na.rm = TRUE)
  
  # Calculate the number of non-NA metrics for each row
  df[[paste0(subset_name, "_non_na_count")]] <- rowSums(!is.na(df[, rank_columns]))
  
  # Scale the total rank by dividing by the number of non-NA metrics
  df[[paste0(subset_name, "_scaled_rank")]] <- df[[paste0(subset_name, "_total_rank")]] / 
    df[[paste0(subset_name, "_non_na_count")]]
  
  return(df)
}

# Apply the function to each subset of metrics with the user-defined periods
ranked_df <- add_total_rank(productivity_metrics, ranked_df, "productivity", periods = c(13)) # Adjust the periods as needed
ranked_df <- add_total_rank(premium_pay_metrics, ranked_df, "premium_pay", periods = c(13)) # Adjust the periods as needed
ranked_df <- add_total_rank(spread_metrics, ranked_df, "spread", periods = c(13)) # Adjust the periods as needed
ranked_df <- add_total_rank(productivity_trend_metrics, ranked_df, "productivity_trend", periods = c(13)) # Adjust the periods as needed
ranked_df <- add_total_rank(productivity_premium_pay_metrics, ranked_df, "productivity_premium_pay", periods = c(13)) # Adjust the periods as needed

# Rank the scaled ranks
ranked_df$composite_rank_13 <- rank(ranked_df$scaled_rank_13, ties.method = "min")
ranked_df$productivity_composite_rank <- rank(ranked_df$productivity_scaled_rank, ties.method = "min")
ranked_df$premium_pay_composite_rank <- rank(ranked_df$premium_pay_scaled_rank, ties.method = "min")
ranked_df$spread_composite_rank <- rank(ranked_df$spread_scaled_rank, ties.method = "min")
ranked_df$productivity_trend_composite_rank <- rank(ranked_df$productivity_trend_scaled_rank, ties.method = "min")
ranked_df$productivity_premium_pay_composite_rank <- rank(ranked_df$productivity_premium_pay_scaled_rank, ties.method = "min")
#-------Combining ranked dataframe and metric dataframe-----------------------
# Perform a left join to append ranked_df to cleaned_df
final_df <- merge(cleaned_df, ranked_df, by = "Department CODE", all.x = TRUE)
colnames(final_df) <- trimws(colnames(final_df))
colnames(department_data) <- trimws(colnames(department_data))
# Left join to add 'Department DESC' to final_df
final_df <- left_join(final_df, department_data, by = "Department CODE")
# Reorder columns to ensure 'Department DESC' is placed after 'Department CODE'
final_df <- final_df %>%
  select(`Department CODE`, `Department DESC`, everything())

#------------Adding in binary static and entity volume columns-------------
static_vol_departments <- static_vol_deps$`Department Definition Code`
entity_vol_departments <- entity_vol_deps$`Department Definition Code`

# Add the 'Entity Volume' and 'Static Volume' columns to final_df based on conditions
final_df$Entity_Volume <- ifelse(final_df$`Department CODE` %in% entity_vol_departments, 1, 0)
final_df$Static_Volume <- ifelse(final_df$`Department CODE` %in% static_vol_departments, 1, 0)

final_df <- final_df %>%
  left_join(key_vol %>% select(DEFINITION_CODE, KEY_VOLUME), 
            by = c("Department CODE" = "DEFINITION_CODE")) %>%
  left_join(rep_def %>% select(DEFINITION_CODE, SITE, CORPORATE_SERVICE_LINE, VP, DEPARTMENT_BREAKDOWN), 
            by = c("Department CODE" = "DEFINITION_CODE")) %>%
  filter(DEPARTMENT_BREAKDOWN != 0) %>%
  select(-DEPARTMENT_BREAKDOWN)  # Remove the extra column if not needed


# Define the desired column order
# Define the new desired column order
col_order <- c(
  "SITE",
  "CORPORATE_SERVICE_LINE",
  "VP",
  "Department CODE", 
  "Department DESC",
  "Entity_Volume", 
  "Static_Volume",
  "composite_rank_13",
  "scaled_rank_13",
  "productivity_composite_rank",
  "productivity_scaled_rank", 
  "premium_pay_composite_rank",
  "premium_pay_scaled_rank", 
  "spread_composite_rank",
  "spread_scaled_rank",
  "productivity_trend_composite_rank",
  "productivity_trend_scaled_rank",
  "productivity_premium_pay_composite_rank",
  "productivity_premium_pay_scaled_rank",
  setdiff(names(final_df), c(
    "SITE",
    "CORPORATE_SERVICE_LINE",
    "VP",
    "Department CODE", 
    "Department DESC",
    "Entity_Volume", 
    "Static_Volume",
    "composite_rank_13",
    "scaled_rank_13",
    "productivity_composite_rank",
    "productivity_scaled_rank", 
    "premium_pay_composite_rank",
    "premium_pay_scaled_rank", 
    "spread_composite_rank",
    "spread_scaled_rank",
    "productivity_trend_composite_rank",
    "productivity_trend_scaled_rank",
    "productivity_premium_pay_composite_rank",
    "productivity_premium_pay_scaled_rank"
  ))
)


# Reorder the columns in final_df
# Remove columns where the name contains ".1"
final_df <- final_df[, !grepl("\\.1$", colnames(final_df))]
final_df <- final_df[, col_order]

#Formatting shortlist view
# Define the exact column names with "rank" in lowercase
selected_columns <- c("SITE", "CORPORATE_SERVICE_LINE", "VP", "Department CODE", "Department DESC", 
                      "Entity_Volume", "Static_Volume", "composite_rank_13", "scaled_rank_13", "Hourly_rate_avg_13_Periods",
                      "Worked_FTE_13_Periods", "productivity_composite_rank",
                      "productivity_scaled_rank", 
                      "Productivity_Index_13_Periods_rank", "FTE_Variance_13_Periods_rank", "LE_Index_13_Periods_rank", 
                      "LE_Variance_13_Periods_rank", 
                      "Productivity_Index_13_Periods", "FTE_Variance_13_Periods", 
                      "LE_Index_13_Periods", "LE_Variance_13_Periods", "premium_pay_composite_rank",
                      "premium_pay_scaled_rank", "Premium_Hours_pct_Worked_Hours_13_Periods_rank", "Premium_Pay_pct_Worked_LE_13_Periods_rank", 
                      "Premium_Pay_FTE_Variance_calc_13_Periods_rank", "Premium_Pay_Variance_13_Periods_rank", 
                      "Premium_Hours_pct_Worked_Hours_13_Periods", 
                      "Premium_Pay_pct_Worked_LE_13_Periods", "Premium_Pay_FTE_Variance_calc_13_Periods", "Premium_Pay_Variance_13_Periods", 
                      "Premium_Hours_pct_Worked_hours_Slope_13_Periods_rank",
                      "spread_composite_rank", "spread_scaled_rank", "PI_stdv_13_Periods_rank", "LE_stdv_13_Periods_rank", "PI_min_max_range_Range_13_Periods_rank", 
                      "LE_min_max_range_Range_13_Periods_rank", "correlation_result_13_Periods_rank", "FTE_Variance_stdv_13_Periods_rank", 
                      "LE_Variance_stdv_13_Periods_rank", "FTE_Variance_min_max_range_Range_13_Periods_rank", "LE_Variance_min_max_range_Range_13_Periods_rank", 
                      "PI_stdv_13_Periods", "LE_stdv_13_Periods", "PI_min_max_range_Range_13_Periods", "LE_min_max_range_Range_13_Periods", 
                      "correlation_result_13_Periods", 
                      "FTE_Variance_stdv_13_Periods", "LE_Variance_stdv_13_Periods", "FTE_Variance_min_max_range_Range_13_Periods", 
                      "LE_Variance_min_max_range_Range_13_Periods", "productivity_trend_composite_rank", 
                      "productivity_trend_scaled_rank", "Worked_Hours_Prod_Slope_13_Periods_rank", "LE_Index_Slope_13_Periods_rank", 
                      "Worked_Hours_FTE_Variance_Slope_13_Periods_rank", "LE_Variance_Slope_13_Periods_rank", 
                      "Worked_Hours_Prod_Slope_13_Periods", "LE_Index_Slope_13_Periods", "Worked_Hours_FTE_Variance_Slope_13_Periods", 
                      "LE_Variance_Slope_13_Periods", "productivity_premium_pay_composite_rank",
                      "productivity_premium_pay_scaled_rank", "Premium_Hours_pct_Worked_hours_Intercepts_13_Periods")



# Create the new dataframe with columns in the specified order
shortlist_view <- final_df %>%
  select(all_of(selected_columns)) %>%
  left_join(key_vol %>% select(DEFINITION_CODE, KEY_VOLUME),
            by = c("Department CODE" = "DEFINITION_CODE")) %>%
  relocate(KEY_VOLUME, .after = `Department DESC`) %>%
  left_join(
    labor_standards %>%
      select(`Department Definition Code`,
             `Primary Worked Hours per Unit Target`,
             `Primary Paid Labor Expense per Unit Target`,
             `Standard Type`),
    by = c("Department CODE" = "Department Definition Code")) %>%
      relocate(`Primary Worked Hours per Unit Target`,
               `Primary Paid Labor Expense per Unit Target`,
               `Standard Type`,
               .after = Static_Volume)
  
# Function to add standard deviation columns for each metric subset
add_subset_sd <- function(df, metrics, subset_name, period = 13) {
  # Build expected rank column names
  rank_cols <- paste0(metrics, "_", period, "_Periods_rank")
  
  # Keep only rank columns that exist in df
  rank_cols <- rank_cols[rank_cols %in% names(df)]
  
  # Calculate SD across subset ranks
  sd_col <- paste0(subset_name, "_rank_sd_", period)
  df[[sd_col]] <- apply(df[, rank_cols, drop = FALSE], 1, function(x) sd(x, na.rm = TRUE))
  
  # Determine the column to place it after
  target_col <- paste0(subset_name, "_scaled_rank")
  
  # Reorder so SD column appears right after its scaled_rank column (if it exists)
  if (target_col %in% names(df)) {
    df <- df %>%
      relocate(all_of(sd_col), .after = all_of(target_col))
  }
  
  return(df)
}

# Apply the SD function and reposition columns
shortlist_view <- shortlist_view %>%
  add_subset_sd("productivity_metrics" |> get(), "productivity", 13) %>%
  add_subset_sd("premium_pay_metrics" |> get(), "premium_pay", 13) %>%
  add_subset_sd("spread_metrics" |> get(), "spread", 13) %>%
  add_subset_sd("productivity_trend_metrics" |> get(), "productivity_trend", 13) %>%
  add_subset_sd("productivity_premium_pay_metrics" |> get(), "productivity_premium_pay", 13)

# Combine into a named list for multiple sheets
output_list <- list(
  "Shortlist_View" = shortlist_view,
  "NA_Departments" = na_departments
)

# Export to Excel with multiple sheets
write_xlsx(output_list, path = "Appendix.xlsx")
# Rollups----------------------------------------------------------
#------------Renaming columns for rollups------------------------
# List of suffixes for the different sets of data frames
suffixes <- c("rollup_site", "rollup_vp", "rollup_site_corp", "rollup_site_lpm", "rollup_corp", "rollup_lpm")

# Descriptive column names for each set of data frames
rollup_groups <- list(
  rollup_site = c("SITE"),
  rollup_vp = c("VP"),
  rollup_site_corp = c("SITE", "CORPORATE_SERVICE_LINE"),
  rollup_site_lpm = c("SITE", "LPM_SERVICE_LINE"),
  rollup_corp = c("CORPORATE_SERVICE_LINE"),
  rollup_lpm = c("LPM_SERVICE_LINE")
)

rename_columns <- function(df_name, suffix) {
  # Get the base name of the data frame (before the suffix)
  base_name <- gsub(paste0("_", suffix), "", df_name)
  
  # Get the descriptive columns for the current suffix
  descriptive_columns <- rollup_groups[[suffix]]
  
  # Define the new column names with correct ordering
  new_column_names <- c(paste(base_name, "3_Periods", sep = "_"), 
                        paste(base_name, "13_Periods", sep = "_"), 
                        paste(base_name, "26_Periods", sep = "_"))
  
  # Get the data frame by name
  df <- get(df_name)
  
  # Retain the descriptive columns and rename the metric columns
  colnames(df) <- c(descriptive_columns, 
                    grep("_3_Periods$", new_column_names, value = TRUE),
                    grep("_13_Periods$", new_column_names, value = TRUE),
                    grep("_26_Periods$", new_column_names, value = TRUE))
  
  # Return the updated data frame
  assign(df_name, df, envir = .GlobalEnv)
}

# Function to rename columns for all sets
rename_all_sets <- function() {
  # Loop through each suffix and rename columns for the corresponding data frames
  for (suffix in suffixes) {
    # List of data frame names for the current suffix
    df_names <- paste(c(
      "Worked_FTE_avg", "Paid_LE_avg", "Target_Worked_FTE_avg", "Target_LE_avg",
      "Productivity_Index_avg", "FTE_Variance_avg", "LE_Index_avg", "LE_Variance_avg",
      "Premium_Pay_avg", "Premium_Pay_pct_Worked_LE_avg", "OT_expense_avg", 
      "OT_Pay_pct_Worked_LE_avg", "Productivity_Index_med", "FTE_Variance_med",
      "Premium_Pay_Variance_med", "LE_Index_med", "LE_Variance_med", "Premium_Pay_med", 
      "Premium_Pay_pct_Worked_LE_med", "OT_expense_med", "OT_Pay_pct_Worked_LE_med", 
      "correlation_result", "Worked_Hours_Prod_Slope", "LE_Index_Slope",
      "Worked_Hours_PI_Intercepts", "LE_PI_Intercepts", "PI_stdv", "LE_stdv", 
      "FTE_Variance_stdv", "LE_Variance_stdv", "PI_min_max_range", "LE_min_max_range", 
      "FTE_Variance_min_max_range", "LE_Variance_min_max_range", "Premium_Hours_pct_Worked_hours",
      "Actual_Premium_Pay_FTEs", "Premium_Pay_FTE_Variance_calc", "Premium_Pay_Variance",
      "Worked_Hours_FTE_Variance_Slope", "LE_Variance_Slope", "Hourly_rate_avg", 
      "Premium_Hours_pct_Worked_hours_Slope", "Premium_Hours_pct_Worked_hours_Intercepts"
    ), 
                      suffix, sep = "_")
    
    # Apply the renaming function to each data frame
    lapply(df_names, rename_columns, suffix = suffix)
  }
}

# Call the function to rename columns for all sets
rename_all_sets()

#---------Combining rollups--------------------------------
# List of data frames 
#--------Rollup Site---------------------------------------
dfs_rollup_site <- list(
  Worked_FTE_avg_rollup_site, Paid_LE_avg_rollup_site, Target_Worked_FTE_avg_rollup_site, Target_LE_avg_rollup_site,
  Productivity_Index_avg_rollup_site, FTE_Variance_avg_rollup_site, LE_Index_avg_rollup_site, LE_Variance_avg_rollup_site,
  Premium_Pay_avg_rollup_site, Premium_Pay_pct_Worked_LE_avg_rollup_site, OT_expense_avg_rollup_site, 
  OT_Pay_pct_Worked_LE_avg_rollup_site, Productivity_Index_med_rollup_site, FTE_Variance_med_rollup_site,
  Premium_Pay_Variance_med_rollup_site, LE_Index_med_rollup_site, LE_Variance_med_rollup_site, Premium_Pay_med_rollup_site, 
  Premium_Pay_pct_Worked_LE_med_rollup_site, OT_expense_med_rollup_site, OT_Pay_pct_Worked_LE_med_rollup_site, 
  correlation_result_rollup_site, Worked_Hours_Prod_Slope_rollup_site, LE_Index_Slope_rollup_site,
  Worked_Hours_PI_Intercepts_rollup_site, LE_PI_Intercepts_rollup_site, PI_stdv_rollup_site, LE_stdv_rollup_site, 
  FTE_Variance_stdv_rollup_site, LE_Variance_stdv_rollup_site, PI_min_max_range_rollup_site, LE_min_max_range_rollup_site, 
  FTE_Variance_min_max_range_rollup_site, LE_Variance_min_max_range_rollup_site, Premium_Hours_pct_Worked_hours_rollup_site,
  Actual_Premium_Pay_FTEs_rollup_site, Premium_Pay_FTE_Variance_calc_rollup_site, Premium_Pay_Variance_rollup_site,
  Worked_Hours_FTE_Variance_Slope_rollup_site, LE_Variance_Slope_rollup_site, Hourly_rate_avg_rollup_site, 
  Premium_Hours_pct_Worked_hours_Slope_rollup_site, Premium_Hours_pct_Worked_hours_Intercepts_rollup_site
)

combined_rollup_site <- Reduce(function(x, y) merge(x, y, by = "SITE", all.x = TRUE), dfs_rollup_site)[, c("SITE", grep("13_Periods", colnames(Reduce(function(x, y) merge(x, y, by = "SITE", all.x = TRUE), dfs_rollup_site)), value = TRUE))]

#---------------Rollup VP--------------------------------------------------
dfs_rollup_vp <- list(
  Worked_FTE_avg_rollup_vp, Paid_LE_avg_rollup_vp, Target_Worked_FTE_avg_rollup_vp, Target_LE_avg_rollup_vp,
  Productivity_Index_avg_rollup_vp, FTE_Variance_avg_rollup_vp, LE_Index_avg_rollup_vp, LE_Variance_avg_rollup_vp,
  Premium_Pay_avg_rollup_vp, Premium_Pay_pct_Worked_LE_avg_rollup_vp, OT_expense_avg_rollup_vp, 
  OT_Pay_pct_Worked_LE_avg_rollup_vp, Productivity_Index_med_rollup_vp, FTE_Variance_med_rollup_vp,
  Premium_Pay_Variance_med_rollup_vp, LE_Index_med_rollup_vp, LE_Variance_med_rollup_vp, Premium_Pay_med_rollup_vp, 
  Premium_Pay_pct_Worked_LE_med_rollup_vp, OT_expense_med_rollup_vp, OT_Pay_pct_Worked_LE_med_rollup_vp, 
  correlation_result_rollup_vp, Worked_Hours_Prod_Slope_rollup_vp, LE_Index_Slope_rollup_vp,
  Worked_Hours_PI_Intercepts_rollup_vp, LE_PI_Intercepts_rollup_vp, PI_stdv_rollup_vp, LE_stdv_rollup_vp, 
  FTE_Variance_stdv_rollup_vp, LE_Variance_stdv_rollup_vp, PI_min_max_range_rollup_vp, LE_min_max_range_rollup_vp, 
  FTE_Variance_min_max_range_rollup_vp, LE_Variance_min_max_range_rollup_vp, Premium_Hours_pct_Worked_hours_rollup_vp,
  Actual_Premium_Pay_FTEs_rollup_vp, Premium_Pay_FTE_Variance_calc_rollup_vp, Premium_Pay_Variance_rollup_vp,
  Worked_Hours_FTE_Variance_Slope_rollup_vp, LE_Variance_Slope_rollup_vp, Hourly_rate_avg_rollup_vp, 
  Premium_Hours_pct_Worked_hours_Slope_rollup_vp, Premium_Hours_pct_Worked_hours_Intercepts_rollup_vp
)

combined_rollup_vp <- Reduce(function(x, y) merge(x, y, by = "VP", all.x = TRUE), dfs_rollup_vp)[, c("VP", grep("13_Periods", colnames(Reduce(function(x, y) merge(x, y, by = "VP", all.x = TRUE), dfs_rollup_vp)), value = TRUE))]

#------------Rollup Corporate Service Line----------------------------------------------
dfs_rollup_corp <- list(
  Worked_FTE_avg_rollup_corp, Paid_LE_avg_rollup_corp, Target_Worked_FTE_avg_rollup_corp, Target_LE_avg_rollup_corp,
  Productivity_Index_avg_rollup_corp, FTE_Variance_avg_rollup_corp, LE_Index_avg_rollup_corp, LE_Variance_avg_rollup_corp,
  Premium_Pay_avg_rollup_corp, Premium_Pay_pct_Worked_LE_avg_rollup_corp, OT_expense_avg_rollup_corp, 
  OT_Pay_pct_Worked_LE_avg_rollup_corp, Productivity_Index_med_rollup_corp, FTE_Variance_med_rollup_corp,
  Premium_Pay_Variance_med_rollup_corp, LE_Index_med_rollup_corp, LE_Variance_med_rollup_corp, Premium_Pay_med_rollup_corp, 
  Premium_Pay_pct_Worked_LE_med_rollup_corp, OT_expense_med_rollup_corp, OT_Pay_pct_Worked_LE_med_rollup_corp, 
  correlation_result_rollup_corp, Worked_Hours_Prod_Slope_rollup_corp, LE_Index_Slope_rollup_corp,
  Worked_Hours_PI_Intercepts_rollup_corp, LE_PI_Intercepts_rollup_corp, PI_stdv_rollup_corp, LE_stdv_rollup_corp, 
  FTE_Variance_stdv_rollup_corp, LE_Variance_stdv_rollup_corp, PI_min_max_range_rollup_corp, LE_min_max_range_rollup_corp, 
  FTE_Variance_min_max_range_rollup_corp, LE_Variance_min_max_range_rollup_corp, Premium_Hours_pct_Worked_hours_rollup_corp,
  Actual_Premium_Pay_FTEs_rollup_corp, Premium_Pay_FTE_Variance_calc_rollup_corp, Premium_Pay_Variance_rollup_corp,
  Worked_Hours_FTE_Variance_Slope_rollup_corp, LE_Variance_Slope_rollup_corp, Hourly_rate_avg_rollup_corp, 
  Premium_Hours_pct_Worked_hours_Slope_rollup_corp, Premium_Hours_pct_Worked_hours_Intercepts_rollup_corp
)

combined_rollup_corp <- Reduce(function(x, y) merge(x, y, by = "CORPORATE_SERVICE_LINE", all.x = TRUE), dfs_rollup_corp)[, c("CORPORATE_SERVICE_LINE", grep("13_Periods", colnames(Reduce(function(x, y) merge(x, y, by = "CORPORATE_SERVICE_LINE", all.x = TRUE), dfs_rollup_corp)), value = TRUE))]

#-----------Rollup LPM SERVICE LINE-------------------------------------------------------
dfs_rollup_lpm <- list(
  Worked_FTE_avg_rollup_lpm, Paid_LE_avg_rollup_lpm, Target_Worked_FTE_avg_rollup_lpm, Target_LE_avg_rollup_lpm,
  Productivity_Index_avg_rollup_lpm, FTE_Variance_avg_rollup_lpm, LE_Index_avg_rollup_lpm, LE_Variance_avg_rollup_lpm,
  Premium_Pay_avg_rollup_lpm, Premium_Pay_pct_Worked_LE_avg_rollup_lpm, OT_expense_avg_rollup_lpm, 
  OT_Pay_pct_Worked_LE_avg_rollup_lpm, Productivity_Index_med_rollup_lpm, FTE_Variance_med_rollup_lpm,
  Premium_Pay_Variance_med_rollup_lpm, LE_Index_med_rollup_lpm, LE_Variance_med_rollup_lpm, Premium_Pay_med_rollup_lpm, 
  Premium_Pay_pct_Worked_LE_med_rollup_lpm, OT_expense_med_rollup_lpm, OT_Pay_pct_Worked_LE_med_rollup_lpm, 
  correlation_result_rollup_lpm, Worked_Hours_Prod_Slope_rollup_lpm, LE_Index_Slope_rollup_lpm,
  Worked_Hours_PI_Intercepts_rollup_lpm, LE_PI_Intercepts_rollup_lpm, PI_stdv_rollup_lpm, LE_stdv_rollup_lpm, 
  FTE_Variance_stdv_rollup_lpm, LE_Variance_stdv_rollup_lpm, PI_min_max_range_rollup_lpm, LE_min_max_range_rollup_lpm, 
  FTE_Variance_min_max_range_rollup_lpm, LE_Variance_min_max_range_rollup_lpm, Premium_Hours_pct_Worked_hours_rollup_lpm,
  Actual_Premium_Pay_FTEs_rollup_lpm, Premium_Pay_FTE_Variance_calc_rollup_lpm, Premium_Pay_Variance_rollup_lpm,
  Worked_Hours_FTE_Variance_Slope_rollup_lpm, LE_Variance_Slope_rollup_lpm, Hourly_rate_avg_rollup_lpm, 
  Premium_Hours_pct_Worked_hours_Slope_rollup_lpm, Premium_Hours_pct_Worked_hours_Intercepts_rollup_lpm
)

combined_rollup_lpm <- Reduce(function(x, y) merge(x, y, by = "LPM_SERVICE_LINE", all.x = TRUE), dfs_rollup_lpm)[, c("LPM_SERVICE_LINE", grep("13_Periods", colnames(Reduce(function(x, y) merge(x, y, by = "LPM_SERVICE_LINE", all.x = TRUE), dfs_rollup_lpm)), value = TRUE))]

#----------Rollup_Site_Corporate_service_line--------------------------
dfs_rollup_site_corp <- list(
  Worked_FTE_avg_rollup_site_corp, Paid_LE_avg_rollup_site_corp, Target_Worked_FTE_avg_rollup_site_corp, Target_LE_avg_rollup_site_corp,
  Productivity_Index_avg_rollup_site_corp, FTE_Variance_avg_rollup_site_corp, LE_Index_avg_rollup_site_corp, LE_Variance_avg_rollup_site_corp,
  Premium_Pay_avg_rollup_site_corp, Premium_Pay_pct_Worked_LE_avg_rollup_site_corp, OT_expense_avg_rollup_site_corp, 
  OT_Pay_pct_Worked_LE_avg_rollup_site_corp, Productivity_Index_med_rollup_site_corp, FTE_Variance_med_rollup_site_corp,
  Premium_Pay_Variance_med_rollup_site_corp, LE_Index_med_rollup_site_corp, LE_Variance_med_rollup_site_corp, Premium_Pay_med_rollup_site_corp, 
  Premium_Pay_pct_Worked_LE_med_rollup_site_corp, OT_expense_med_rollup_site_corp, OT_Pay_pct_Worked_LE_med_rollup_site_corp, 
  correlation_result_rollup_site_corp, Worked_Hours_Prod_Slope_rollup_site_corp, LE_Index_Slope_rollup_site_corp,
  Worked_Hours_PI_Intercepts_rollup_site_corp, LE_PI_Intercepts_rollup_site_corp, PI_stdv_rollup_site_corp, LE_stdv_rollup_site_corp, 
  FTE_Variance_stdv_rollup_site_corp, LE_Variance_stdv_rollup_site_corp, PI_min_max_range_rollup_site_corp, LE_min_max_range_rollup_site_corp, 
  FTE_Variance_min_max_range_rollup_site_corp, LE_Variance_min_max_range_rollup_site_corp, Premium_Hours_pct_Worked_hours_rollup_site_corp,
  Actual_Premium_Pay_FTEs_rollup_site_corp, Premium_Pay_FTE_Variance_calc_rollup_site_corp, Premium_Pay_Variance_rollup_site_corp,
  Worked_Hours_FTE_Variance_Slope_rollup_site_corp, LE_Variance_Slope_rollup_site_corp, Hourly_rate_avg_rollup_site_corp, 
  Premium_Hours_pct_Worked_hours_Slope_rollup_site_corp, Premium_Hours_pct_Worked_hours_Intercepts_rollup_site_corp
)

combined_rollup_site_corp <- Reduce(function(x, y) merge(x, y, by = c("SITE", "CORPORATE_SERVICE_LINE"), all.x = TRUE), dfs_rollup_site_corp)[, c("SITE", "CORPORATE_SERVICE_LINE", grep("13_Periods", colnames(Reduce(function(x, y) merge(x, y, by = c("SITE", "CORPORATE_SERVICE_LINE"), all.x = TRUE), dfs_rollup_site_corp)), value = TRUE))]

#---------Rollup Site LPM Service Line--------------------------
dfs_rollup_site_lpm <- list(
  Worked_FTE_avg_rollup_site_lpm, Paid_LE_avg_rollup_site_lpm, Target_Worked_FTE_avg_rollup_site_lpm, Target_LE_avg_rollup_site_lpm,
  Productivity_Index_avg_rollup_site_lpm, FTE_Variance_avg_rollup_site_lpm, LE_Index_avg_rollup_site_lpm, LE_Variance_avg_rollup_site_lpm,
  Premium_Pay_avg_rollup_site_lpm, Premium_Pay_pct_Worked_LE_avg_rollup_site_lpm, OT_expense_avg_rollup_site_lpm, 
  OT_Pay_pct_Worked_LE_avg_rollup_site_lpm, Productivity_Index_med_rollup_site_lpm, FTE_Variance_med_rollup_site_lpm,
  Premium_Pay_Variance_med_rollup_site_lpm, LE_Index_med_rollup_site_lpm, LE_Variance_med_rollup_site_lpm, Premium_Pay_med_rollup_site_lpm, 
  Premium_Pay_pct_Worked_LE_med_rollup_site_lpm, OT_expense_med_rollup_site_lpm, OT_Pay_pct_Worked_LE_med_rollup_site_lpm, 
  correlation_result_rollup_site_lpm, Worked_Hours_Prod_Slope_rollup_site_lpm, LE_Index_Slope_rollup_site_lpm,
  Worked_Hours_PI_Intercepts_rollup_site_lpm, LE_PI_Intercepts_rollup_site_lpm, PI_stdv_rollup_site_lpm, LE_stdv_rollup_site_lpm, 
  FTE_Variance_stdv_rollup_site_lpm, LE_Variance_stdv_rollup_site_lpm, PI_min_max_range_rollup_site_lpm, LE_min_max_range_rollup_site_lpm, 
  FTE_Variance_min_max_range_rollup_site_lpm, LE_Variance_min_max_range_rollup_site_lpm, Premium_Hours_pct_Worked_hours_rollup_site_lpm,
  Actual_Premium_Pay_FTEs_rollup_site_lpm, Premium_Pay_FTE_Variance_calc_rollup_site_lpm, Premium_Pay_Variance_rollup_site_lpm,
  Worked_Hours_FTE_Variance_Slope_rollup_site_lpm, LE_Variance_Slope_rollup_site_lpm, Hourly_rate_avg_rollup_site_lpm, 
  Premium_Hours_pct_Worked_hours_Slope_rollup_site_lpm, Premium_Hours_pct_Worked_hours_Intercepts_rollup_site_lpm
)

combined_rollup_site_lpm <- Reduce(function(x, y) merge(x, y, by = c("SITE", "LPM_SERVICE_LINE"), all.x = TRUE), dfs_rollup_site_lpm)[, c("SITE", "LPM_SERVICE_LINE", grep("13_Periods", colnames(Reduce(function(x, y) merge(x, y, by = c("SITE", "LPM_SERVICE_LINE"), all.x = TRUE), dfs_rollup_site_lpm)), value = TRUE))]



#------------Ranking Rollups---------------------
lower_is_better_metrics <- c(
  "Worked_FTE_avg_13_Periods", "Paid_LE_avg_13_Periods", "FTE_Variance_avg_13_Periods", "LE_Variance_avg_13_Periods", "Premium_Pay_avg_13_Periods", 
  "Premium_Pay_pct_Worked_LE_avg_13_Periods", "OT_expense_med_13_Periods", "OT_Pay_pct_Worked_LE_avg_13_Periods",
  "FTE_Variance_med_13_Periods", "Premium_Pay_Variance_med_13_Periods",
  "LE_Variance_med_13_Periods", "Premium_Pay_med_13_Periods", "OT_expense_med_13_Periods",
  "Premium_Pay_pct_Worked_LE_med_13_Periods", "OT_Pay_pct_Worked_LE_med_13_Periods", "PI_stdv_13_Periods",
  "LE_stdv_13_Periods", "FTE_Variance_stdv_13_Periods", "LE_Variance_stdv_13_Periods", "PI_min_max_range_Range_13_Periods", 
  "LE_min_max_range_Range_13_Periods", "FTE_Variance_min_max_range_Range_13_Periods", 
  "LE_Variance_min_max_range_Range_13_Periods", "PI_percentiles_Spread_13_Periods", 
  "LE_percentiles_Spread_13_Periods", "Premium_Hours_pct_Worked_hours_13_Periods", 
  "Premium_Pay_FTE_Variance_calc_13_Periods", "Premium_Pay_Variance_13_Periods", 
  "Actual_Premium_Pay_FTEs_13_Periods", "Worked_Hours_FTE_Variance_Slope_13_Periods", 
  "LE_Variance_Slope_13_Periods", "Hourly_rate_avg_13_Periods", "Premium_Hours_pct_Worked_hours_Slope_13_Periods",
  "Premium_Hours_pct_Worked_hours_Intercepts_13_Periods"
)

rank_dataframes <- function(dfs, lower_is_better_metrics) {
  # Iterate over each dataframe in the list
  for (i in seq_along(dfs)) {
    df <- dfs[[i]]
    
    # Iterate over each metric in the dataframe
    for (metric in colnames(df)) {
      if (metric %in% lower_is_better_metrics) {
        # Reverse ranking for lower-is-better metrics
        df[[paste0(metric, "_rank")]] <- rank(as.numeric(df[[metric]]), ties.method = "average", na.last = "keep")
      } else {
        # Normal ranking for higher-is-better metrics
        df[[paste0(metric, "_rank")]] <- rank(-as.numeric(df[[metric]]), ties.method = "average", na.last = "keep")
      }
    }
    
    # Save the modified dataframe back to the list
    dfs[[i]] <- df
  }
  
  return(dfs)
}

dfs <- list(combined_rollup_corp, combined_rollup_lpm, combined_rollup_site , combined_rollup_site_corp, 
            combined_rollup_site_lpm, combined_rollup_vp)

# Call the function to add ranking columns
dfs_ranked <- rank_dataframes(dfs, lower_is_better_metrics)


library(dplyr)

add_composite_ranks <- function(df, period = "13_Periods") {
  
  metric_groups <- list(
  productivity_metrics = c(
    "Productivity_Index_avg", 
    "FTE_Variance_avg", 
    "LE_Index_avg", 
    "LE_Variance_avg"
  ),
  
  premium_pay_metrics = c(
    "Premium_Hours_pct_Worked_hours", 
    "Premium_Pay_pct_Worked_LE_avg", 
    "Premium_Pay_FTE_Variance_calc", 
    "Premium_Pay_Variance"
  ),
  
  spread_metrics = c(
    "PI_stdv", 
    "LE_stdv", 
    "PI_min_max_range", 
    "LE_min_max_range", 
    "correlation_result", 
    "FTE_Variance_stdv", 
    "LE_Variance_stdv", 
    "FTE_Variance_min_max_range", 
    "LE_Variance_min_max_range"
  ),
  
  productivity_trend_metrics = c(
    "Worked_Hours_Prod_Slope", 
    "LE_Index_Slope", 
    "Worked_Hours_FTE_Variance_Slope", 
    "LE_Variance_Slope"
  ),
  
  productivity_premium_pay_metrics = c(
    "Productivity_Index_avg", 
    "Premium_Hours_pct_Worked_hours"
  )
)
  # Define metric groups
  metric_groups <- list(
    productivity_metrics = c("Productivity_Index", "FTE_Variance", "LE_Index", "LE_Variance"),
    premium_pay_metrics = c("Premium_Hours_pct_Worked_Hours", 
                            "Premium_Pay_pct_Worked_LE", "Premium_Pay_FTE_Variance_calc", "Premium_Pay_Variance"),
    spread_metrics = c("PI_stdv", "LE_stdv", "PI_min_max_range_Range", "LE_min_max_range_Range", "correlation_result", 
                       "FTE_Variance_stdv", "LE_Variance_stdv", "FTE_Variance_min_max_range_Range", "LE_Variance_min_max_range_Range"),
    productivity_trend_metrics = c("Worked_Hours_Prod_Slope", "LE_Index_Slope", "Worked_Hours_FTE_Variance_Slope", "LE_Variance_Slope"),
    productivity_premium_pay_metrics = c("Productivity_Index", "Premium_Hours_pct_Worked_Hours")
  )
  
  lower_is_better_metrics <- c(
    "Worked_FTE_avg", "Paid_LE_avg", "FTE_Variance_avg", "LE_Variance_avg", "Premium_Pay_avg", 
    "Premium_Pay_pct_Worked_LE_avg", "OT_expense_med", "OT_Pay_pct_Worked_LE_avg",
    "FTE_Variance_med", "Premium_Pay_Variance_med", "LE_Variance_med", "Premium_Pay_med", 
    "OT_expense_med", "Premium_Pay_pct_Worked_LE_med", "OT_Pay_pct_Worked_LE_med", "PI_stdv",
    "LE_stdv", "FTE_Variance_stdv", "LE_Variance_stdv", "PI_min_max_range_Range", "LE_min_max_range_Range", 
    "FTE_Variance_min_max_range_Range", "LE_Variance_min_max_range_Range", "PI_percentiles_Spread", 
    "LE_percentiles_Spread", "Premium_Hours_pct_Worked_Hours", "Premium_Pay_FTE_Variance_calc", 
    "Premium_Pay_Variance", "Actual_Premium_Pay_FTEs", "Worked_Hours_FTE_Variance_Slope", 
    "LE_Variance_Slope", "Hourly_rate_avg", "Premium_Hours_pct_Worked_hours_Slope", 
    "Premium_Hours_pct_Worked_hours_Intercepts"
  )
  
  for (group_name in names(metric_groups)) {
    # Construct full metric names with period suffix
    group_metrics <- paste0(metric_groups[[group_name]], "_", period)
    
    # Filter for existing columns in the dataframe
    group_metrics <- intersect(group_metrics, names(df))
    
    if (length(group_metrics) > 0) {
      # Rank calculation
      for (metric in group_metrics) {
        if (metric %in% paste0(lower_is_better_metrics, "_", period)) {
          df[[paste0(metric, "_Rank")]] <- rank(df[[metric]], na.last = "keep", ties.method = "average") # Lower is better
        } else {
          df[[paste0(metric, "_Rank")]] <- rank(-df[[metric]], na.last = "keep", ties.method = "average") # Higher is better
        }
      }
      
      # Extract rank columns
      rank_columns <- paste0(group_metrics, "_Rank")
      rank_df <- df %>% select(all_of(rank_columns))
      
      # Compute total rank
      df[[paste0(group_name, "_Total_Rank")]] <- rowSums(rank_df, na.rm = TRUE)
      
      # Compute scaled rank (total rank divided by non-NA count)
      non_na_counts <- rowSums(!is.na(rank_df))
      df[[paste0(group_name, "_Scaled_Rank")]] <- ifelse(non_na_counts > 0, 
                                                         df[[paste0(group_name, "_Total_Rank")]] / non_na_counts, 
                                                         NA)
    } else {
      message(paste("No valid metrics found for:", group_name))  # Debugging output
    }
  }
  
  return(df)
}

# Apply function to all six data frames
dataframes_list <- lapply(dfs_ranked, add_composite_ranks)


add_composite_ranks <- function(df, period = "13_Periods") {
  # Define metric groups
  metric_groups <- list(
    productivity_metrics = c("Productivity_Index_avg", "FTE_Variance_avg", "LE_Index_avg", "LE_Variance_avg"),
    premium_pay_metrics = c("Premium_Hours_pct_Worked_Hours", 
                          "Premium_Pay_pct_Worked_LE", "Premium_Pay_FTE_Variance_calc", "Premium_Pay_Variance"),
    spread_metrics = c("PI_stdv", "LE_stdv", "PI_min_max_range_Range", "LE_min_max_range_Range", "correlation_result", 
                       "FTE_Variance_stdv", "LE_Variance_stdv", "FTE_Variance_min_max_range_Range", "LE_Variance_min_max_range_Range"),
    productivity_trend_metrics = c("Worked_Hours_Prod_Slope", "LE_Index_Slope", "Worked_Hours_FTE_Variance_Slope", "LE_Variance_Slope"),
    productivity_premium_pay_metrics = c("Productivity_Index", "Premium_Hours_pct_Worked_Hours")
  )
  
  lower_is_better_metrics <- c(
    "Worked_FTE_avg", "Paid_LE_avg", "FTE_Variance_avg", "LE_Variance_avg", "Premium_Pay_avg", 
    "Premium_Pay_pct_Worked_LE_avg", "OT_expense_med", "OT_Pay_pct_Worked_LE_avg",
    "FTE_Variance_med", "Premium_Pay_Variance_med", "LE_Variance_med", "Premium_Pay_med", 
    "OT_expense_med", "Premium_Pay_pct_Worked_LE_med", "OT_Pay_pct_Worked_LE_med", "PI_stdv",
    "LE_stdv", "FTE_Variance_stdv", "LE_Variance_stdv", "PI_min_max_range_Range", "LE_min_max_range_Range", 
    "FTE_Variance_min_max_range_Range", "LE_Variance_min_max_range_Range", "PI_percentiles_Spread", 
    "LE_percentiles_Spread", "Premium_Hours_pct_Worked_Hours", "Premium_Pay_FTE_Variance_calc", 
    "Premium_Pay_Variance", "Actual_Premium_Pay_FTEs", "Worked_Hours_FTE_Variance_Slope", 
    "LE_Variance_Slope", "Hourly_rate_avg", "Premium_Hours_pct_Worked_hours_Slope", 
    "Premium_Hours_pct_Worked_hours_Intercepts"
  )
  
  for (group_name in names(metric_groups)) {
    # Construct full metric names with period suffix
    group_metrics <- paste0(metric_groups[[group_name]], "_", period)
    
    # Filter for existing columns in the dataframe
    group_metrics <- intersect(group_metrics, names(df))
    
    if (length(group_metrics) > 0) {
      # Rank calculation
      for (metric in group_metrics) {
        # Ensure the metric is numeric before applying rank
        if (is.numeric(df[[metric]])) {
          if (metric %in% paste0(lower_is_better_metrics, "_", period)) {
            df[[paste0(metric, "_rank")]] <- rank(df[[metric]], na.last = "keep", ties.method = "average") # Lower is better
          } else {
            df[[paste0(metric, "_rank")]] <- rank(-df[[metric]], na.last = "keep", ties.method = "average") # Higher is better
          }
        } else {
          message(paste("Skipping non-numeric metric:", metric))  # Debugging output for non-numeric columns
        }
      }
      
      # Extract rank columns
      rank_columns <- paste0(group_metrics, "_rank")
      rank_df <- df %>% select(all_of(rank_columns))
      
      # Compute total rank
      df[[paste0(group_name, "_Total_Rank")]] <- rowSums(rank_df, na.rm = TRUE)
      
      # Compute scaled rank (total rank divided by non-NA count)
      non_na_counts <- rowSums(!is.na(rank_df))
      df[[paste0(group_name, "_Scaled_Rank")]] <- ifelse(non_na_counts > 0, 
                                                         df[[paste0(group_name, "_Total_Rank")]] / non_na_counts, 
                                                         NA)
    } else {
      message(paste("No valid metrics found for:", group_name))  # Debugging output
    }
  }
  
  return(df)
}

# Apply function to all six data frames
dataframes_list <- lapply(dfs_ranked, add_composite_ranks)




add_composite_ranks <- function(df, period = "13_Periods") {
  # Define metric groups
  metric_groups <- list(
    productivity_metrics = c("Productivity_Index_avg", "FTE_Variance_avg", "LE_Index_avg", "LE_Variance_avg"),
    premium_pay_metrics = c("Premium_Hours_pct_Worked_Hours", 
                            "Premium_Pay_pct_Worked_LE", "Premium_Pay_FTE_Variance_calc", "Premium_Pay_Variance"),
    spread_metrics = c("PI_stdv", "LE_stdv", "PI_min_max_range_Range", "LE_min_max_range_Range", "correlation_result", 
                       "FTE_Variance_stdv", "LE_Variance_stdv", "FTE_Variance_min_max_range_Range", "LE_Variance_min_max_range_Range"),
    productivity_trend_metrics = c("Worked_Hours_Prod_Slope", "LE_Index_Slope", "Worked_Hours_FTE_Variance_Slope", "LE_Variance_Slope"),
    productivity_premium_pay_metrics = c("Productivity_Index_avg", "Premium_Hours_pct_Worked_Hours")
  )
  
  lower_is_better_metrics <- c(
    "Worked_FTE_avg", "Paid_LE_avg", "FTE_Variance_avg", "LE_Variance_avg", "Premium_Pay_avg", 
    "Premium_Pay_pct_Worked_LE_avg", "OT_expense_med", "OT_Pay_pct_Worked_LE_avg",
    "FTE_Variance_med", "Premium_Pay_Variance_med", "LE_Variance_med", "Premium_Pay_med", 
    "OT_expense_med", "Premium_Pay_pct_Worked_LE_med", "OT_Pay_pct_Worked_LE_med", "PI_stdv",
    "LE_stdv", "FTE_Variance_stdv", "LE_Variance_stdv", "PI_min_max_range_Range", "LE_min_max_range_Range", 
    "FTE_Variance_min_max_range_Range", "LE_Variance_min_max_range_Range", "PI_percentiles_Spread", 
    "LE_percentiles_Spread", "Premium_Hours_pct_Worked_Hours", "Premium_Pay_FTE_Variance_calc", 
    "Premium_Pay_Variance", "Actual_Premium_Pay_FTEs", "Worked_Hours_FTE_Variance_Slope", 
    "LE_Variance_Slope", "Hourly_rate_avg", "Premium_Hours_pct_Worked_hours_Slope", 
    "Premium_Hours_pct_Worked_hours_Intercepts"
  )
  
  for (group_name in names(metric_groups)) {
    group_metrics <- paste0(metric_groups[[group_name]], "_", period)
    group_metrics <- intersect(group_metrics, names(df))
    
    if (length(group_metrics) > 0) {
      for (metric in group_metrics) {
        df[[metric]] <- as.numeric(as.character(df[[metric]]))
        
        if (is.numeric(df[[metric]])) {
          if (metric %in% paste0(lower_is_better_metrics, "_", period)) {
            df[[paste0(metric, "_rank")]] <- rank(df[[metric]], na.last = "keep", ties.method = "average")
          } else {
            df[[paste0(metric, "_rank")]] <- rank(-df[[metric]], na.last = "keep", ties.method = "average")
          }
        } else {
          message(paste("Skipping non-numeric metric:", metric))
        }
      }
      
      rank_columns <- paste0(group_metrics, "_rank")
      rank_df <- df %>% select(all_of(rank_columns))
      
      df[[paste0(group_name, "_Total_Rank")]] <- rowSums(rank_df, na.rm = TRUE)
      non_na_counts <- rowSums(!is.na(rank_df))
      df[[paste0(group_name, "_Scaled_Rank")]] <- ifelse(non_na_counts > 0, 
                                                         df[[paste0(group_name, "_Total_Rank")]] / non_na_counts, 
                                                         NA)
    } else {
      message(paste("No valid metrics found for:", group_name))
    }
  }
  
  # Compute overall total rank and total scaled rank
  all_rank_columns <- grep("_rank$", names(df), value = TRUE)
  composite_rank_columns <- grep("_(Total|Scaled)_Rank$", names(df), value = TRUE)
  individual_rank_columns <- setdiff(all_rank_columns, composite_rank_columns)
  
  if (length(individual_rank_columns) > 0) {
    total_rank_df <- df %>% select(all_of(individual_rank_columns))
    df[["Total_Rank"]] <- rowSums(total_rank_df, na.rm = TRUE)
    total_non_na_counts <- rowSums(!is.na(total_rank_df))
    df[["Total_Scaled_Rank"]] <- ifelse(total_non_na_counts > 0, df[["Total_Rank"]] / total_non_na_counts, NA)
  }
  
  return(df)
}

# Apply function to all data frames
dataframes_list <- lapply(dfs_ranked, add_composite_ranks)

#-------------Composite ranking for rollups--------------------------
# Function to remove columns that contain only NA values
remove_na_columns <- function(df) {
  df[, colSums(!is.na(df)) > 0]
}

# Function to add total ranks and composite ranks for selected metrics
add_total_rank_rollup <- function(metric_prefixes, df, subset_name, periods = c(3, 13, 26)) {
  # Identify columns that match any of the metric patterns for the given periods
  rank_columns <- names(df)[grepl("_rank$", names(df)) & 
                              sapply(metric_prefixes, function(prefix) {
                                sapply(periods, function(p) {
                                  grepl(paste0("^", prefix, ".*_", p, "_Periods_rank$"), names(df))
                                })
                              }) |> rowSums() > 0]
  
  if (length(rank_columns) > 0) {
    total_rank_col <- paste0(subset_name, "_total_rank")
    non_na_col <- paste0(subset_name, "_non_na_count")
    scaled_rank_col <- paste0(subset_name, "_scaled_rank")
    
    df[[total_rank_col]] <- rowSums(df[, rank_columns, drop = FALSE], na.rm = TRUE)
    df[[non_na_col]] <- rowSums(!is.na(df[, rank_columns, drop = FALSE]))
    df[[scaled_rank_col]] <- ifelse(df[[non_na_col]] > 0, 
                                    df[[total_rank_col]] / df[[non_na_col]], 
                                    NA)
  }
  
  return(df)
}

# Apply NA column removal and ranking calculations to each dataframe in dfs_ranked
dfs_ranked <- lapply(dataframes_list, function(df) {
  df <- remove_na_columns(df)
  
  df <- add_total_rank_rollup(productivity_metrics, df, "productivity", periods = c(13))
  df <- add_total_rank_rollup(premium_pay_metrics, df, "premium_pay", periods = c(13))
  df <- add_total_rank_rollup(spread_metrics, df, "spread", periods = c(13))
  df <- add_total_rank_rollup(productivity_trend_metrics, df, "productivity_trend", periods = c(13))
  df <- add_total_rank_rollup(productivity_premium_pay_metrics, df, "productivity_premium_pay", periods = c(13))
  
  # Rank the scaled ranks safely
  rank_columns <- c("scaled_rank_13", "productivity_scaled_rank", "premium_pay_scaled_rank", 
                    "spread_scaled_rank", "productivity_trend_scaled_rank", "productivity_premium_pay_scaled_rank")
  
  rank_names <- c("composite_rank_13", "productivity_composite_rank", "premium_pay_composite_rank",
                  "spread_composite_rank", "productivity_trend_composite_rank", "productivity_premium_pay_composite_rank")
  
  for (i in seq_along(rank_columns)) {
    if (rank_columns[i] %in% names(df)) {
      df[[rank_names[i]]] <- rank(df[[rank_columns[i]]], ties.method = "min", na.last = "keep")
    }
  }
  
  # Also rank the Total_Scaled_Rank if it exists
  if ("Total_Scaled_Rank" %in% names(df)) {
    df[["Total_Composite_Rank"]] <- rank(df[["Total_Scaled_Rank"]], ties.method = "min", na.last = "keep")
  }
  
  return(df)
})



# Define export directory (update this path as needed)
export_dir <- dir_testing

# Export each dataframe in dfs_ranked
for (i in seq_along(dfs_ranked)) {
  file_name <- paste0(export_dir, "ranked_df_", i, ".csv")
  write.csv(dfs_ranked[[i]], file = file_name, row.names = FALSE)
}

for (i in 1:length(dfs_ranked)) {
  # Create a file name for each dataframe (optional, you can customize it)
  file_name <- paste0("dfs_ranked_", i, ".csv")
  
  # Define the full file path
  file_path <- file.path(dir_testing, file_name)
  
  # Export the dataframe to the file
  write.csv(dfs_ranked[[i]], file_path, row.names = FALSE)
}

#---------Cleaning up Rollups------------------------------
# Define yellow-highlighted columns (in desired order)
yellow_cols <- c(
  "Hourly_rate_avg_13_Periods",
  "Worked_FTE_avg_13_Periods",
  "Total_Composite_Rank",
  "Total_Scaled_Rank",
  "productivity_composite_rank",
  "productivity_scaled_rank",
  "Productivity_Index_avg_13_Periods",
  "FTE_Variance_avg_13_Periods",
  "LE_Index_avg_13_Periods",
  "LE_Variance_avg_13_Periods",
  "premium_pay_composite_rank",
  "premium_pay_scaled_rank",
  "Premium_Pay_avg_13_Periods",
  "Actual_Premium_Pay_FTEs_13_Periods",
  "Premium_Hours_pct_Worked_hours_13_Periods",
  "Premium_Pay_pct_Worked_LE_avg_13_Periods",
  "Premium_Pay_FTE_Variance_calc_13_Periods",
  "Premium_Pay_Variance_13_Periods",
  "spread_composite_rank",
  "spread_scaled_rank",
  "PI_stdv_13_Periods",
  "LE_stdv_13_Periods",
  "PI_min_max_range_13_Periods",
  "LE_min_max_range_13_Periods",
  "correlation_result_13_Periods",
  "FTE_Variance_stdv_13_Periods",
  "LE_Variance_stdv_13_Periods",
  "FTE_Variance_min_max_range_13_Periods",
  "LE_Variance_min_max_range_13_Periods",
  "productivity_trend_composite_rank",
  "productivity_trend_scaled_rank",
  "Worked_Hours_Prod_Slope_13_Periods",
  "LE_Index_Slope_13_Periods",
  "Worked_Hours_FTE_Variance_Slope_13_Periods",
  "LE_Variance_Slope_13_Periods",
  "productivity_premium_pay_composite_rank",
  "productivity_premium_pay_scaled_rank",
  "Productivity_Index_avg_13_Periods",
  "Premium_Hours_pct_Worked_hours_13_Periods"
)

# Function to subset and reorder columns
subset_df <- function(df) {
  # Find the index of the Worked_FTE_avg_13_Periods column
  worked_fte_index <- which(names(df) == "Worked_FTE_avg_13_Periods")
  
  # Descriptive columns = everything before that
  desc_cols <- names(df)[1:(worked_fte_index - 1)]
  
  # Final column order: descriptive columns first, then yellow columns (in specified order)
  keep_cols <- c(desc_cols, yellow_cols)
  
  # Subset the dataframe
  df_subset <- df[, keep_cols, drop = FALSE]
  
  return(df_subset)
}

# Apply to each dataframe in the dfs_ranked list
dfs_ranked_subset <- lapply(dfs_ranked, subset_df)

# Assign sheet names: Ranked_1 through Ranked_6, then NA_Departments
sheet_names <- c(paste0("Ranked_", 1:6), "NA_Departments")

# Combine the 6 ranked dataframes with na_departments into one list
all_dfs <- c(dfs_ranked_subset, list(na_departments))

# Name the list using the desired sheet names
named_dfs <- setNames(all_dfs, sheet_names)

# Write to Excel
write_xlsx(named_dfs, path = "ranked_output.xlsx")

#----------Correlation Coefficient Testing---------------
rollup_data_1 <- rollup_data[, 1:5]
rollup_data_1$`Department CODE` <- trimws(rollup_data_1$`Department CODE`)
correlation_df <- correlation_result %>%
  left_join(rollup_data_1, by = "Department CODE")

library(dplyr)

# Clean up column names (remove extra spaces)
colnames(correlation_df) <- trimws(colnames(correlation_df))

# Helper function to summarize correlation
summarize_correlation <- function(df, group_vars) {
  df %>%
    group_by(across(all_of(group_vars))) %>%
    summarise(
      Avg_Correlation = mean(Correlation_Last_13_Periods, na.rm = TRUE),
      Median_Correlation = median(Correlation_Last_13_Periods, na.rm = TRUE),
      Pct_Positive_Correlation = mean(Correlation_Last_13_Periods > 0, na.rm = TRUE) * 100,
      .groups = "drop"
    )
}
correlation_df <- correlation_df[, -ncol(correlation_df)]
# 1. Group by SITE, VP, CORPORATE_SERVICE_LINE, LPM_SERVICE_LINE
summary_site <- summarize_correlation(
  correlation_df,
  c("SITE")
)
summary_vp <- summarize_correlation(
  correlation_df,
  c("VP")
)

summary_corp <- summarize_correlation(
  correlation_df,
  c("CORPORATE_SERVICE_LINE")
)

summary_lpm <- summarize_correlation(
  correlation_df,
  c("LPM_SERVICE_LINE")
)

# 2. Group by SITE and CORPORATE_SERVICE_LINE
summary_site_corp <- summarize_correlation(
  correlation_df,
  c("SITE", "CORPORATE_SERVICE_LINE")
)

# 3. Group by SITE and LPM_SERVICE_LINE
summary_site_lpm <- summarize_correlation(
  correlation_df,
  c("SITE", "LPM_SERVICE_LINE")
)

library(openxlsx)

# Define full path
save_path <- file.path(dir_testing, "Work in Progress", "Correlation")

# Write each data frame to an Excel file
write.xlsx(correlation_result_rollup_site,      file = file.path(save_path, "correlation_result_rollup_site.xlsx"))
write.xlsx(correlation_result_rollup_vp,        file = file.path(save_path, "correlation_result_rollup_vp.xlsx"))
write.xlsx(correlation_result_rollup_lpm,       file = file.path(save_path, "correlation_result_rollup_lpm.xlsx"))
write.xlsx(correlation_result_rollup_corp,      file = file.path(save_path, "correlation_result_rollup_corp.xlsx"))
write.xlsx(correlation_result_rollup_site_corp, file = file.path(save_path, "correlation_result_rollup_site_corp.xlsx"))
write.xlsx(correlation_result_rollup_site_lpm,  file = file.path(save_path, "correlation_result_rollup_site_lpm.xlsx"))

# Write each summary data frame to an Excel file
write.xlsx(summary_site,      file = file.path(save_path, "summary_site.xlsx"))
write.xlsx(summary_vp,        file = file.path(save_path, "summary_vp.xlsx"))
write.xlsx(summary_lpm,       file = file.path(save_path, "summary_lpm.xlsx"))
write.xlsx(summary_corp,      file = file.path(save_path, "summary_corp.xlsx"))
write.xlsx(summary_site_corp, file = file.path(save_path, "summary_site_corp.xlsx"))
write.xlsx(summary_site_lpm,  file = file.path(save_path, "summary_site_lpm.xlsx"))
