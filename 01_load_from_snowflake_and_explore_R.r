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

# Specify which catalog, schema and table we want to load data from
catalog <- "temp_abt"
schema <- "temp_schema"
table <- "abt_loan_data"

# COMMAND ----------

# DID NOT WORK: Use spark to load a Dataframe from the specified table

# sc <- spark_connect(method = "databricks")
# df <- spark_read_table(sc, name = paste(catalog, schema, table, sep = "."))
# df <- collect(df)


# COMMAND ----------

# Load the data fromt the specified catalog, schema and table
sql_query <- paste("SELECT * FROM", catalog, ".", schema, ".", table)
df <- tbl(sc, sql(sql_query))
df <- collect(df)

# COMMAND ----------

# Display the Dataframe
head(df)

# COMMAND ----------

# Rename columns
df <- df %>%
  rename(
    person_age = PERSON_AGE,
    person_income = PERSON_INCOME,
    person_home_ownership = PERSON_HOME_OWNERSHIP,
    person_emp_length = PERSON_EMP_LENGTH,
    loan_intent = LOAN_INTENT,
    loan_grade = LOAN_GRADE,
    loan_amnt = LOAN_AMNT,
    loan_int_rate = LOAN_INT_RATE,
    loan_status = LOAN_STATUS,
    loan_percent_income = LOAN_PERCENT_INCOME,
    cb_person_default_on_file = CB_PERSON_DEFAULT_ON_FILE,
    cb_person_cred_hist_length = CB_PERSON_CRED_HIST_LENGTH
  )

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

# Check out the datatypes
glimpse(df)

# COMMAND ----------

# Load Required Libraries
library(ggplot2)

# Frequency Plot of the Loan Status
ggplot(df, aes(x = loan_status, fill = factor(loan_status))) +
  geom_bar() +
  labs(title = "Loan Status: A categorical variable indicating if the loan was paid back or defaulted.") +
  scale_fill_manual(values = c("blue", "red"))

# COMMAND ----------

# Cross table of the loan intent and loan status
cross_table <- table(df$loan_intent, df$loan_status)

# Display the cross table
print(cross_table)

# COMMAND ----------

# Cross table of person_home_ownership with loan_status and loan_grade
cross_table <- ftable(df$person_home_ownership, df$loan_status, df$loan_grade)

# Display the cross table
print(cross_table)

# COMMAND ----------

# Histogram of Loan Amount
hist(df$loan_amnt, breaks = "FD", col = "blue", main = "Histogram of Loan Amount", xlab = "Loan Amount")


# COMMAND ----------

# Scatter plot of Income Against Age with log transformation
plot(log10(df$person_income), df$person_age, 
     col = "blue", 
     pch = 16, 
     main = "Scatter plot of Income Against Age",
     xlab = "Log(Income)",
     ylab = "Age",
     cex = 0.8) # Reduce point size

# Add a grid to the plot
grid()

# Add a legend
legend("topleft", legend = "Income Against Age", col = "blue", pch = 16)


# COMMAND ----------

# Convert pandas dataframe to spark dataframe
spark_df <- copy_to(sc, df, "spark_df")

# Define personal prefix
personal_prefix <- "KFK_HVH"

# Write Spark DataFrame to table
spark_write_table(spark_df, paste0(personal_prefix, "_raw_abt"), mode = "overwrite")
