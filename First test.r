# Databricks notebook source
# Create a list of numbers
numbers <- c(3, 6, 9, 12, 15)

# Calculate the mean of the numbers
mean_value <- mean(numbers)

# Print the mean value
print(mean_value)

# COMMAND ----------

# Load the ggplot2 package
library(ggplot2)

# Create some sample data
x <- c(1, 2, 3, 4, 5)
y <- c(2, 4, 6, 8, 10)
data <- data.frame(x, y)

# Create a scatter plot
plot <- ggplot(data, aes(x = x, y = y)) +
        geom_point(color = "blue") +  # Set point color to blue
        labs(title = "Scatter Plot", x = "X-axis", y = "Y-axis")  # Add title and axis labels

# Print the plot
print(plot)
