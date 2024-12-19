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
# Read in the static and entity volume department Excel files
static_vol_deps <- read_excel(file.path(dir_testing, "Static Volume Departments.xlsx"))
entity_vol_deps <- read_excel(file.path(dir_testing, "Entity Volume Departments.xlsx"))

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
# Data References ---------------------------------------------------------
# (aka Mapping Tables)
# Files that need to be imported for mappings and look-up tables.
# (This section may be combined into the Data Import section.)

# Creation of Functions --------------------------------------------------
#Function to calculate average of last 3, 13 and 26 pay periods. User specifies the metric.
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

# Function to calculate the linear regression slope of Worked Hours Productivity Index
calculate_slope <- function(data, metric) {
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
  data_grouped <- data %>% group_by(`Department CODE`)
  
  # Calculate slopes for each department based on the chosen metric
  slopes <- data_grouped %>% summarise(
    Slope_Last_3_Periods = calculate_slope(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3))))),
    Slope_Last_13_Periods = calculate_slope(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13))))),
    Slope_Last_26_Periods = calculate_slope(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26))))),
    .groups = "drop"
  )
  
  return(slopes)
}

# Function to calculate the linear regression equation for any given metric
calculate_regression_equation <- function(data, metric_name = "Worked Hours Productivity Index") {
  # Trim whitespace in column names
  colnames(data) <- trimws(colnames(data))
  
  # Filter columns with the specified metric name at the end
  metric_columns <- names(data)[grepl(paste0(" ", metric_name, "$"), names(data))]
  
  # Sort metric columns by date to ensure chronological order
  metric_columns <- sort(metric_columns)
  
  # Group data by Department CODE and calculate slope and intercept for each time period
  regression_equations <- data %>% group_by(`Department CODE`) %>% summarise(
    Regression_Last_3_Periods = {
      y <- as.numeric(unlist(select(cur_data(), tail(metric_columns, 3))))
      x <- seq_along(y)
      if (length(na.omit(y)) >= 2) {
        lm_fit <- lm(y ~ x)
        intercept <- coef(lm_fit)[1]
        slope <- coef(lm_fit)[2]
        paste0("y = ", round(intercept, 3), " + ", round(slope, 3), " * x")
      } else {
        NA
      }
    },
    Regression_Last_13_Periods = {
      y <- as.numeric(unlist(select(cur_data(), tail(metric_columns, 13))))
      x <- seq_along(y)
      if (length(na.omit(y)) >= 2) {
        lm_fit <- lm(y ~ x)
        intercept <- coef(lm_fit)[1]
        slope <- coef(lm_fit)[2]
        paste0("y = ", round(intercept, 3), " + ", round(slope, 3), " * x")
      } else {
        NA
      }
    },
    Regression_Last_26_Periods = {
      y <- as.numeric(unlist(select(cur_data(), tail(metric_columns, 26))))
      x <- seq_along(y)
      if (length(na.omit(y)) >= 2) {
        lm_fit <- lm(y ~ x)
        intercept <- coef(lm_fit)[1]
        slope <- coef(lm_fit)[2]
        paste0("y = ", round(intercept, 3), " + ", round(slope, 3), " * x")
      } else {
        NA
      }
    },
    .groups = "drop"
  )
  
  return(regression_equations)
}

calculate_intercept <- function(data, metric) {
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
  data_grouped <- data %>% group_by(`Department CODE`)
  
  # Calculate intercepts for each department based on the chosen metric
  intercepts <- data_grouped %>% summarise(
    Intercept_Last_3_Periods = calculate_intercept(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3))))),
    Intercept_Last_13_Periods = calculate_intercept(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13))))),
    Intercept_Last_26_Periods = calculate_intercept(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26))))),
    .groups = "drop"
  )
  
  return(intercepts)
}

# Function to calculate only the y-intercept (OLD)
calculate_intercept_old <- function(data, metric_name = "Worked Hours Productivity Index") {
  # Trim whitespace in column names
  colnames(data) <- trimws(colnames(data))
  
  metric_columns <- names(data)[grepl(paste0(" ", metric_name, "$"), names(data))]
  
  # Sort metric columns by date to ensure chronological order
  metric_columns <- sort(metric_columns)
  
  # Group data by Department CODE and calculate intercept for each time period
  intercepts <- data %>% group_by(`Department CODE`) %>% summarise(
    Intercept_Last_3_Periods = {
      y <- as.numeric(unlist(select(cur_data(), tail(metric_columns, 3))))
      x <- seq_along(y)
      if (length(na.omit(y)) >= 2) coef(lm(y ~ x))[1] else NA
    },
    Intercept_Last_13_Periods = {
      y <- as.numeric(unlist(select(cur_data(), tail(metric_columns, 13))))
      x <- seq_along(y)
      if (length(na.omit(y)) >= 2) coef(lm(y ~ x))[1] else NA
    },
    Intercept_Last_26_Periods = {
      y <- as.numeric(unlist(select(cur_data(), tail(metric_columns, 26))))
      x <- seq_along(y)
      if (length(na.omit(y)) >= 2) coef(lm(y ~ x))[1] else NA
    },
    .groups = "drop"
  )
  
  return(intercepts)
}

#Function to calculate standard deviation
calculate_metric_sd <- function(data, metric) {
  
  colnames(data) <- trimws(colnames(data))
  
  # Find columns for the specified metric
  metric_columns <- names(data)[grepl(paste0(" ", metric, "$"), names(data))]
  
  # Replace blanks with NA
  data[data == ""] <- NA
  
  # Group by Department CODE and calculate the standard deviation for the last 3, 13, and 26 periods
  sd_result <- data %>%
    group_by(`Department CODE`) %>%
    summarise(
      SD_Last_3_Periods = sd(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3)))), na.rm = TRUE),
      SD_Last_13_Periods = sd(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13)))), na.rm = TRUE),
      SD_Last_26_Periods = sd(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26)))), na.rm = TRUE),
      .groups = "drop"
    )
  
  return(sd_result)
}

#Min Max and Range function
calculate_metric_min_max_range <- function(data, metric) {
  
  colnames(data) <- trimws(colnames(data))
  
  # Find columns for the specified metric
  metric_columns <- names(data)[grepl(paste0(" ", metric, "$"), names(data))]
  
  # Replace blanks with NA
  data[data == ""] <- NA
  
  # Group by Department CODE and calculate the min, max, and range for the last 3, 13, and 26 periods
  min_max_range_result <- data %>%
    group_by(`Department CODE`) %>%
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

#Percentile Function
calculate_metric_percentiles <- function(data, metric, lower_percentile = 0.25, upper_percentile = 0.75) {
  
  colnames(data) <- trimws(colnames(data))
  
  # Find columns for the specified metric
  metric_columns <- names(data)[grepl(paste0(" ", metric, "$"), names(data))]
  
  # Replace blanks with NA
  data[data == ""] <- NA
  
  # Group by Department CODE and calculate the percentiles and spread for the last 3, 13, and 26 periods
  percentile_result <- data %>% 
    group_by(`Department CODE`) %>% 
    summarise(
      Percentile_Last_3_Periods_Lower = quantile(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3)))), probs = lower_percentile, na.rm = TRUE),
      Percentile_Last_3_Periods_Upper = quantile(as.numeric(unlist(select(cur_data(), tail(metric_columns, 3)))), probs = upper_percentile, na.rm = TRUE),
      Spread_Last_3_Periods = Percentile_Last_3_Periods_Upper - Percentile_Last_3_Periods_Lower,
      
      Percentile_Last_13_Periods_Lower = quantile(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13)))), probs = lower_percentile, na.rm = TRUE),
      Percentile_Last_13_Periods_Upper = quantile(as.numeric(unlist(select(cur_data(), tail(metric_columns, 13)))), probs = upper_percentile, na.rm = TRUE),
      Spread_Last_13_Periods = Percentile_Last_13_Periods_Upper - Percentile_Last_13_Periods_Lower,
      
      Percentile_Last_26_Periods_Lower = quantile(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26)))), probs = lower_percentile, na.rm = TRUE),
      Percentile_Last_26_Periods_Upper = quantile(as.numeric(unlist(select(cur_data(), tail(metric_columns, 26)))), probs = upper_percentile, na.rm = TRUE),
      Spread_Last_26_Periods = Percentile_Last_26_Periods_Upper - Percentile_Last_26_Periods_Lower,
      
      .groups = "drop"
    )
  
  return(percentile_result)
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

# Apply function to calculate the slope for Worked Hours Productivity Index
Worked_Hours_Prod_Slope <- calculate_slope(data, "Worked Hours Productivity Index")
LE_Index_Slope <- calculate_slope(data, "Labor Expense Index")
Worked_Hours_FTE_Variance_Slope <- calculate_slope(data, "Worked FTE Variance")
LE_Variance_Slope <- calculate_slope(data, "Labor Expense Variance")

# Apply function to calculate the linear regression equations for Worked Hours Productivity Index
Worked_Hours_PI_Regressions <- calculate_regression_equation(data, "Worked Hours Productivity Index")
LE_PI_Regressions <- calculate_regression_equation(data, "Labor Expense Index")

# Apply y intercept function
Worked_Hours_PI_Intercepts <- calculate_intercept(data, "Worked Hours Productivity Index")
LE_PI_Intercepts <- calculate_intercept(data, "Labor Expense Index")

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

#Applying percentile function
PI_percentiles <- calculate_metric_percentiles(data, "Worked Hours Productivity Index", 0.10, 0.90) # Example for 10th and 90th percentiles
LE_percentiles <- calculate_metric_percentiles(data, "Labor Expense Index", 0.20, 0.80) # Example for 20th and 80th percentiles

# Data Formatting ---------------------------------------------------------
# How the data will look during the output of the script.
# For example, if you have a data table that needs the numbers to show up as
# green or red depending on whether they meet a certain threshold.


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
  FTE_Variance_min_max_range, LE_Variance_min_max_range,
  PI_percentiles, LE_percentiles, Premium_Hours_pct_Worked_hours,
  Premium_Pay_hours_avg, Premium_Pay_FTE_Variance_calc, Premium_Pay_Variance,
  Worked_Hours_FTE_Variance_Slope, LE_Variance_Slope
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
  "PI_percentiles", "LE_percentiles", "Premium_Hours_pct_Worked_Hours", 
  "Premium_Pay_Hours", "Premium_Pay_FTE_Variance_calc", "Premium_Pay_Variance",
  "Worked_Hours_FTE_Variance_Slope", "LE_Variance_Slope"
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
  "LE_percentiles_Spread", "Premium_Hours_pct_Worked_hours", 
  "Premium_Pay_FTE_Variance_calc", "Premium_Pay_Variance", "Premium_Pay_Hours")

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

# Update total_rank column for the 13 pay period columns only
ranked_df$total_rank <- rowSums(ranked_df[, grep("_13_Periods_rank$", names(ranked_df))], na.rm = TRUE)

# Remove the original metric columns and keep only the rank columns
ranked_df <- ranked_df[, grep("_rank$", names(ranked_df))]
ranked_df <- cbind(`Department CODE` = cleaned_df$`Department CODE`, ranked_df)

# Define subsets of metrics
productivity_metrics <- c("Productivity_Index", "FTE_Variance", "LE_Index", "LE_Variance")

premium_pay_metrics <- c("Premium_Pay", "Premium_Pay_Hours", 
                         "Premium_Hours_pct_Worked_Hours", 
                         "Premium_Pay_pct_Worked_LE",
                         "Premium_Pay_FTE_Variance_calc", 
                         "Premium_Pay_Variance")

spread_metrics <- c("PI_stdv", "LE_stdv", "PI_min_max_range_Range", 
                    "LE_min_max_range_Range", "correlation_result", 
                    "FTE_Variance_stdv", "LE_Variance_stdv", 
                    "FTE_Variance_min_max_range_Range", 
                    "LE_Variance_min_max_range_Range")

linear_regression_metrics <- c("Worked_Hours_Prod_Slope", "LE_Index_Slope",
                               "Worked_Hours_FTE_Variance_Slope", 
                               "LE_Variance_Slope")


# Add total rank columns for each subset of metrics with a control for pay period durations
add_total_rank <- function(metrics, df, subset_name, periods = c(3, 13, 26)) {
  # Generate rank columns based on selected periods
  rank_columns <- paste0(metrics, "_", periods, "_Periods_rank")
  
  # Calculate the total rank by summing the ranks for the selected periods
  df[[paste0(subset_name, "_total_rank")]] <- rowSums(df[, rank_columns], na.rm = TRUE)
  
  # Scale the total rank by dividing by the number of metrics
  num_metrics <- length(metrics)
  df[[paste0(subset_name, "_scaled_rank")]] <- df[[paste0(subset_name, "_total_rank")]] / num_metrics
  
  return(df)
}

# Apply the function to each subset of metrics with the user-defined periods
ranked_df <- add_total_rank(productivity_metrics, ranked_df, "productivity", periods = c(13)) # Adjust the periods as needed
ranked_df <- add_total_rank(premium_pay_metrics, ranked_df, "premium_pay", periods = c(13)) # Adjust the periods as needed
ranked_df <- add_total_rank(spread_metrics, ranked_df, "spread", periods = c(13)) # Adjust the periods as needed
ranked_df <- add_total_rank(linear_regression_metrics, ranked_df, "linear_regression", periods = c(13)) # Adjust the periods as needed

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
  left_join(rep_def %>% select(DEFINITION_CODE, SITE, CORPORATE_SERVICE_LINE, VP), 
            by = c("Department CODE" = "DEFINITION_CODE"))


# Define the desired column order
col_order <- c(
  "SITE",
  "CORPORATE_SERVICE_LINE",
  "VP",
  "Department CODE", 
  "Department DESC",
  "Entity_Volume", 
  "Static_Volume", 
  "total_rank", 
  "productivity_scaled_rank", 
  "premium_pay_scaled_rank", 
  "spread_scaled_rank",
  "linear_regression_scaled_rank",
  setdiff(names(final_df), c("Department CODE", "Department DESC", "Entity_Volume", "Static_Volume", "total_rank", "productivity_total_rank", "labor_total_rank", "spread_total_rank"))
)

# Reorder the columns in final_df
# Remove columns where the name contains ".1"
final_df <- final_df[, !grepl("\\.1$", colnames(final_df))]
final_df <- final_df[, col_order]
# Script End --------------------------------------------------------------
