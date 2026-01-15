#run the following files first!
#1) analyze_hourly_tafs.R
#2) analyze_hourly_metars.R


library(tidyverse)
library(lubridate)
library(janitor)

#build schedule - analyze_hourly_tafs already has everything

window_start = as_datetime(ymd(20221001))
window_end = as_datetime(ymd(20251031))+hours(23)

#read alt list
alt_list <- read_csv("2AltPolicy.csv",  col_types = cols(.default = "c")) %>%
  clean_names() %>%
  mutate(
    aerodrome = str_c("C", aerodrome),
    alt1 = str_c("C", alt1),
    alt2 = str_c("C", alt2),
    distalt1 = parse_number(str_remove(distalt1, "nm")),
    distalt2 = parse_number(str_remove(distalt2, "nm")),
    distalt2incr = parse_number(str_remove(distalt2incr, "nm"))
  )

all_stations <- c(
  alt_list %>% pull(aerodrome),
  alt_list %>% pull(alt1),
  alt_list %>% pull(alt2)
) %>%
  unique()

all_stations_analysis <- crossing(
  station = all_stations,
  time = seq(window_start, window_end, by = "1 hour")
  ) %>%
  left_join(
    metars_with_requirements %>%
      select(station, issued_time, raw, wind_reconstructed, vis, sigwx, clouds, ceiling, usable_runways, lowest_haa, lowest_apch_ban_vis, suitable_for_landing) %>%
      #add floor_hr to make join more manageable
      mutate(issued_time_floor_hr = floor_date(issued_time, unit = "hours")),
    join_by(station, time == issued_time_floor_hr),
    relationship = "one-to-many"
  ) %>%
  group_by(station, time) %>%
  summarise(
    #if there is any METAR issued within the hour that is not suitable for landing, whole hour is considered not suitable
    suitable_for_landing = case_when(
      any(suitable_for_landing == FALSE, na.rm = TRUE) ~ FALSE,        # any FALSE → FALSE
      all(suitable_for_landing == TRUE, na.rm = TRUE) & any(is.na(suitable_for_landing)) ~ NA,  # TRUE + NA → NA
      all(suitable_for_landing == TRUE, na.rm = TRUE) ~ TRUE          # all TRUE → TRUE
    ),
    .groups = "drop"
  )



all_stations_analysis_month <- all_stations_analysis %>%
  mutate(
    month = floor_date(time, unit = "month"),
    month_no = month(time)
  )

all_station_analysis_month_summary1 <- all_stations_analysis_month %>%
  group_by(station, month) %>%
  summarise(
    n = n(),
    n_known = sum(!is.na(suitable_for_landing)),
    na = sum(is.na(suitable_for_landing)) / n,
    suitable = sum(suitable_for_landing, na.rm = TRUE) / n_known,
    unsuitable = sum(!suitable_for_landing, na.rm = TRUE) / n_known,
    .groups = "drop"
  ) %>%
  arrange(station, month)

all_station_analysis_month_summary2 <- all_stations_analysis_month %>%
  group_by(station, month_no) %>%
  summarise(
    n = n(),
    n_known = sum(!is.na(suitable_for_landing)),
    na = sum(is.na(suitable_for_landing)) / n,
    suitable = sum(suitable_for_landing, na.rm = TRUE) / n_known,
    unsuitable = sum(!suitable_for_landing, na.rm = TRUE) / n_known,
    .groups = "drop"
  ) %>%
  arrange(station, month_no)

#write csv
all_station_analysis_month_summary1 %>% write_csv("alt_eval_outputs/metars-destination_suitability__bymonth.csv")
all_station_analysis_month_summary2 %>% write_csv("alt_eval_outputs/metars-destination_suitability__bymonthindex.csv")




#### same thing for TAFs

taf_all_stations_month <- crossing(
  station = all_stations,
  time = seq(window_start, window_end, by = "1 hour")
  ) %>%
  left_join(
    alternate_availability,
    join_by(station, time),
    relationship = "one-to-one"
  ) %>%
  mutate(
    month = floor_date(time, unit = "month"),
    month_no = month(time),
    
    outcome = case_when(
      is.na(status) ~ "NA",
      status != "NORMAL" ~ "NA",
      status == "NORMAL" & suitable_alternate == TRUE ~ "suitable",
      status == "NORMAL" & suitable_alternate == FALSE ~ "unsuitable",
    )
  )

alternate_availability_month_summary1 <- taf_all_stations_month %>%
  group_by(station, month) %>%
  summarise(
    n = n(),
    na = sum(outcome == "NA") / n,
    alt_suitable = sum(outcome == "suitable") / n,
    alt_unsuitable = sum(outcome == "unsuitable") / n,
    .groups = "drop"
  )


alternate_availability_month_summary2 <- taf_all_stations_month %>%
  group_by(station, month_no) %>%
  summarise(
    n = n(),
    na = sum(outcome == "NA") / n,
    alt_suitable = sum(outcome == "suitable") / n,
    alt_unsuitable = sum(outcome == "unsuitable") / n,
    .groups = "drop"
  )

#write csv
alternate_availability_month_summary1 %>% write_csv("alt_eval_outputs/tafs-alternate_availability__bymonth.csv")
alternate_availability_month_summary2 %>% write_csv("alt_eval_outputs/tafs-alternate_availability__bymonthindex.csv")



#### now merge them -- when a station is below landing minimums, how often are
# the two alts available?

metars_tafs <- all_stations_analysis %>%
  filter(
    suitable_for_landing == FALSE
  ) %>%
  left_join(
    alt_list %>% select(aerodrome, alt1, alt2),
    join_by(station == aerodrome),
    relationship = "many-to-one"
  ) %>%
  left_join(
    taf_all_stations_month %>%
      select(station, time, outcome) %>%
      rename(alt1_alternate = outcome),
    join_by(alt1 == station, time),
    relationship = "many-to-one" #many because aerodromes can have common alts.
  ) %>%
  left_join(
    taf_all_stations_month %>%
      select(station, time, outcome) %>%
      rename(alt2_alternate = outcome),
    join_by(alt2 == station, time),
    relationship = "many-to-one" #many because aerodromes can have common alts.
  ) %>%
  mutate(
    month = floor_date(time, unit = "months"),
    month_no = month(time)
  ) %>%
  drop_na(alt1, alt2)

metar_tafs_month_summary1 <- metars_tafs %>%
  group_by(station, month, alt1, alt2) %>%
  summarise(
    n = n(),
    alt1_available = sum(alt1_alternate == "suitable") / n,
    alt2_available = sum(alt2_alternate == "suitable") / n,
    alt1and2_available = sum(alt1_alternate == "suitable" & alt2_alternate == "suitable") / n,
    .groups = "drop"
  )

metar_tafs_month_summary2 <- metars_tafs %>%
  group_by(station, month_no, alt1, alt2) %>%
  summarise(
    n = n(),
    alt1_available = sum(alt1_alternate == "suitable") / n,
    alt2_available = sum(alt2_alternate == "suitable") / n,
    alt1and2_available = sum(alt1_alternate == "suitable" & alt2_alternate == "suitable") / n,
    .groups = "drop"
  )

#write csv
metar_tafs_month_summary1 %>% write_csv("alt_eval_outputs/metars_tafs-alternates_when_destination_unsuitable__bymonth.csv")
metar_tafs_month_summary2 %>% write_csv("alt_eval_outputs/metars_tafs-alternates_when_destination_unsuitable__bymonthindex.csv")

