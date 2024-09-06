# GENERAL REMINDERS -------------------------------------------------------

# 1. The outline can be customized based on project requirements.
# 2. Sections may be added, removed, reordered, renamed, etc. as needed.
# 3. Reference checklist for commenting and style guidelines
# 4. Create Sections using the menu Code > Insert Section
#    or the shortcut Ctrl+Shift+R
# 5. Create subsections by starting a Section with two # signs (e.g. ##).  
#    - See example within this section.


## subsection example ----------------------------------------------------
# The header for this section is an example of how to indent subsections by
# adding an additional # symbol.


# Libraries ---------------------------------------------------------------
# Include all the packages that will be used throughout the code.
# This is where packages can be installed if the user does not have them
# currently installed.

# Ensuring the appropriate package versions are used for the project based on
# renv usage
renv::restore()

# Common Packages
library(here)
library(rmarkdown)
library(shiny)

# tidyverse includes: dplyr, ggplot2, lubridate, purrr, readr, readxl,
# reprex, stringr, tidyr...and more
# See this link for full list: https://tidyverse.tidyverse.org/
library(tidyverse)


# Source Global Functions -------------------------------------------------
# Source scripts that house global functions used within this script.
# Global functions would include functions that are used regularly
# throughout the rest of the script and should make code more readable.

source()


# Assigning Directory(ies) ------------------------------------------------
# Define variables for frequently used root directories or full directories.
# (This section could be combined with the Constants section.)
# Reminder: avoid using setwd()

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

# an example project specific path that can be changed based on the project
project_path <- paste0("/Operations Analytics and Optimization/Projects/",
                       "Service Lines/Lab Kpi/Data/")

# Here is the final path
user_path <- paste0(user_directory, project_path,"*.*")


# Constants ---------------------------------------------------------------
# Define constants that will be used throughout the code. These are the
# variables that are calculated here and not changed in the rest of the code.


# Data Import -------------------------------------------------------------
# Importing data that is needed in the code whether it’s from the shared drive
# or OneDrive or some other location.


# Data References ---------------------------------------------------------
# (aka Mapping Tables)
# Files that need to be imported for mappings and look-up tables.
# (This section may be combined into the Data Import section.)


# Creation of Functions --------------------------------------------------
# These are functions that will be commonly used within the rest of the script.
# It might make sense to keep these files in a separate file that is sourced
# in the "Source Global Functions" section above.


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
