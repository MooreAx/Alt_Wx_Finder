library(jsonlite)
data <- fromJSON("parsed_reports.json")

json_files <- data %>%
  select(file) %>% 
  distinct() %>%
  mutate(
    airport = str_sub(file, 1, 4),
    date = str_extract(file, "\\d{4}-\\d{2}"),
    date = as.Date(paste0(date, "-01"))
  )
