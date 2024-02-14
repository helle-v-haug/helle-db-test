# Databricks notebook source
# Load required libraries
library(tidyverse)
library(ggplot2)
library(sparklyr)
library(dplyr)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG poc_development;
# MAGIC USE SCHEMA hackathon;

# COMMAND ----------

my_prefix = "KFK_HVH"

# COMMAND ----------

# Read Spark table into Spark DataFrame
sc <- spark_connect(method = "databricks")
spark_df <- spark_read_table(sc, name = paste0(my_prefix, "_raw_abt"))

# Convert Spark DataFrame to Pandas DataFrame
df <- collect(spark_df)

# COMMAND ----------

# Set the data types for the columns
df <- df %>%
  mutate(
    person_age = as.integer(person_age),
    person_income = as.integer(person_income),
    person_home_ownership = as.character(person_home_ownership),
    person_emp_length = as.double(person_emp_length),
    loan_intent = as.character(loan_intent),
    loan_grade = as.character(loan_grade),
    loan_amnt = as.integer(loan_amnt),
    loan_int_rate = as.double(loan_int_rate),
    loan_status = as.integer(loan_status),
    loan_percent_income = as.double(loan_percent_income),
    cb_person_default_on_file = as.character(cb_person_default_on_file),
    cb_person_cred_hist_length = as.integer(cb_person_cred_hist_length)
  )


# COMMAND ----------

head(df)

# COMMAND ----------

grab_col_names <- function(dataframe, cat_th=10, car_th=20) {
  # Get all columns with character data type
  cat_cols <- names(dataframe)[sapply(dataframe, class) == "character"]
  
  # Get all columns with numeric data type that have less than 'cat_th' unique values
  num_but_cat <- names(Filter(function(x) length(unique(x)) < cat_th & !is.character(x), dataframe))
  
  # Get all columns with character data type that have more than 'car_th' unique values
  cat_but_car <- names(Filter(function(x) length(unique(x)) > car_th & is.character(x), dataframe))
  
  # Combine the categorical columns and the numeric columns that have less than 'cat_th' unique values
  cat_cols <- union(cat_cols, num_but_cat)
  
  # Remove the columns that are both categorical and have more than 'car_th' unique values
  cat_cols <- setdiff(cat_cols, cat_but_car)
  
  # Get all columns with numeric data type
  num_cols <- names(dataframe)[sapply(dataframe, is.numeric)]
  
  # Remove the numeric columns that are also considered as categorical due to having less than 'cat_th' unique values
  num_cols <- setdiff(num_cols, num_but_cat)
  
  # Print information about the dataset
  cat_cols_len <- length(cat_cols)
  num_cols_len <- length(num_cols)
  cat_but_car_len <- length(cat_but_car)
  num_but_cat_len <- length(num_but_cat)
  
  print(paste("Observations:", nrow(dataframe)))
  print(paste("Variables:", ncol(dataframe)))
  print(paste("cat_cols:", cat_cols_len))
  print(paste("num_cols:", num_cols_len))
  print(paste("cat_but_car:", cat_but_car_len))
  print(paste("num_but_cat:", num_but_cat_len))
  
  return(list(cat_cols=cat_cols, num_cols=num_cols, cat_but_car=cat_but_car, num_but_cat=num_but_cat))
}


# COMMAND ----------

#use AI-Assistant to explain code

# COMMAND ----------

# Call the grab_col_names function
columns <- grab_col_names(df)

# Store the results in variables
cat_cols <- columns$cat_cols
num_cols <- columns$num_cols
cat_but_car <- columns$cat_but_car
num_but_cat <- columns$num_but_cat

# COMMAND ----------

num_but_cat
#Its the dependent variable so we're gonna ignore that.

# COMMAND ----------

cat_cols
#there are 5 categorical variables.

# COMMAND ----------

num_cols

#there are 7 numerical variables.

# COMMAND ----------

# Define function for target summary with numerical columns
target_summary_with_num <- function(dataframe, target, numerical_col) {
  summary <- dataframe %>%
    group_by({{ target }}) %>%
    summarise(mean = mean(.data[[numerical_col]], na.rm = TRUE), .groups = "drop")
  
  print(summary)
  cat("\n\n")
}

# Iterate over numeric columns
for (col in num_cols) {
  target_summary_with_num(df, loan_status, col)
}


# COMMAND ----------

# Define outlier thresholds function
outlier_thresholds <- function(dataframe, col_name, q1=0.01, q3=0.99) {
  quartile1 <- quantile(dataframe[[col_name]], q1, na.rm = TRUE)
  quartile3 <- quantile(dataframe[[col_name]], q3, na.rm = TRUE)
  interquantile_range <- quartile3 - quartile1
  up_limit <- quartile3 + 1.5 * interquantile_range
  low_limit <- quartile1 - 1.5 * interquantile_range
  return(list(low_limit = low_limit, up_limit = up_limit))
}

# Define check_outlier function
check_outlier <- function(dataframe, col_name) {
  thresholds <- outlier_thresholds(dataframe, col_name)
  low_limit <- thresholds$low_limit
  up_limit <- thresholds$up_limit
  
  if (any(dataframe[[col_name]] > up_limit, na.rm = TRUE) | any(dataframe[[col_name]] < low_limit, na.rm = TRUE)) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}

# Define check_outlier function
check_outlier <- function(dataframe, col_name) {
  thresholds <- outlier_thresholds(dataframe, col_name)
  low_limit <- thresholds$low_limit
  up_limit <- thresholds$up_limit
  if (any(dataframe[[col_name]] > up_limit) | any(dataframe[[col_name]] < low_limit)) {
    return(TRUE)
  } else {
    return(FALSE)
  }
}


# COMMAND ----------

# Iterate over numeric columns and check for outliers
for (col in num_cols) {
  # Exclude missing values from the check
  outlier <- check_outlier(df[!is.na(df[[col]]), ], col)
  print(paste(col, outlier))
}

# Output message for columns with outliers
# Adjust the column names in the message as needed
cat("person_age, person_income, and person_emp_length have some outlier observations.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Remove outliers

# COMMAND ----------

# Iterate over numerical columns
for (col_name in num_cols) {
  # Calculate outlier thresholds for the current column
  thresholds <- outlier_thresholds(df, col_name)
  
  # Print the column name
  cat(paste("Column:", col_name, "\n"))
  
  # Print the outlier thresholds
  cat("Lower Limit:", thresholds$low_limit, "\n")
  cat("Upper Limit:", thresholds$up_limit, "\n\n")
}

# COMMAND ----------

# Get indices of rows where person_age is greater than 93
indices <- which(df$person_age > 93)

# Print the indices
print(indices)

# COMMAND ----------

replace_with_thresholds <- function(dataframe, variable) {
  thresholds <- outlier_thresholds(dataframe, variable)
  low_limit <- thresholds$low_limit
  up_limit <- thresholds$up_limit
  
  # Replace missing values with thresholds
  dataframe[[variable]][is.na(dataframe[[variable]])] <- low_limit
  
  # Replace outliers with threshold values
  dataframe[[variable]][dataframe[[variable]] < low_limit] <- low_limit
  dataframe[[variable]][dataframe[[variable]] > up_limit] <- up_limit
}

# COMMAND ----------

# Apply function to replace outliers for each numerical column
for (col_name in num_cols) {
  if (col_name %in% colnames(df)) {
    df[[col_name]] <- ifelse(!is.na(df[[col_name]]), replace_with_thresholds(df[[col_name]]), df[[col_name]])
  }
}

# Check for outliers after replacement
for (col_name in num_cols) {
  if (col_name %in% colnames(df) && !is.na(df[[col_name]])) {
    outlier <- check_outlier(df[[col_name]])
    print(paste(col_name, outlier))
  }
}
