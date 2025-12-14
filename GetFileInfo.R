#analyze scraping results

library(tidyverse)
library(lubridate)

get_file_info <- function(directory = ".") {
  # List all files (non-recursive). Use recursive = TRUE if you want subfolders too.
  files <- list.files(directory, full.names = TRUE)
  
  # Get file information
  info <- file.info(files)
  
  # Extract name from full path
  info$name <- basename(rownames(info))
  
  # Select relevant columns and reorder
  df <- data.frame(
    name = info$name,
    size_bytes = info$size,
    modified = info$mtime,
    created = info$ctime,
    row.names = NULL
  )
  
  # Sort by creation time (optional)
  df <- df[order(df$created, decreasing = TRUE), ]
  
  return(df)
}

# Example usage:
files <- get_file_info("data") %>%
  mutate(
    airport = str_sub(name, 1, 4),
    date = str_extract(name, "\\d{4}-\\d{2}"),
    date = as.Date(paste0(date, "-01"))
  )

summary <- files %>%
  group_by(airport) %>%
  summarise(
    n = n()
  )


#get missing dates

# Step 1: Define expected date range (month starts)
all_months <- tibble(
  date = seq(ymd(20200101), ymd(20250101), by = "1 month")
)

# Step 2: Define the airports you expect
airports <- files %>% distinct(airport)

# Step 3: Cross join all airports × all months
expected <- crossing(airports, all_months)

# Step 4: Find missing airport-month combinations
missing <- expected %>%
  anti_join(files, join_by(airport,date))

