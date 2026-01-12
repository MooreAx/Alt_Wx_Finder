# assess suitability of station for landing

library(tidyverse)
library(lubridate)

source("functions.R")

metars <- read_csv("metars_parsed.csv", col_types = cols(.default = "c")) %>%
  rename(vis = visibility) %>%
  mutate(
    issued_time = parse_date_time(issued, orders = "ymdHM"),
    .after = issued
  ) %>%
  mutate(
    ceiling = parse_number(ceiling),
    vis = parse_number(vis),
    wind_speed = parse_number(wind_speed),
    wind_gust = parse_number(wind_gust),
  ) %>%
  replace_na(list(ceiling = Inf)) %>%
  mutate(
    wind_reconstructed = str_c(
      str_pad(wind_dir, 3, pad = "0"),         # 3 digits for direction
      str_pad(round(wind_speed), 2, pad = "0"),       # at least 2 digits for speed
      if_else(!is.na(wind_gust),
              str_c("G", str_pad(wind_gust, 2, pad = "0")),
              ""),
      "KT"
    ),
    .after = wind_gust
  )


#read landing req'ts
landing_minima <- read_csv(
  "LandingMinimaReqts.csv",
  col_types = cols(.default = "c")
) %>%
  mutate(
    precision_apch = parse_number(precision_apch),
    lowest_haa = parse_number(lowest_haa),
    advisory_vis = parse_number(advisory_vis),
    apch_ban_vis = parse_number(apch_ban_vis),
    variation = parse_number(variation),
    rwy_bearing = parse_number(rwy_bearing)
  ) %>%
  replace_na(list(variation = 0))

#calculate usable runways to get lowest minima
metars_minima <- metars %>%
  select(station, issued_time, wind_reconstructed) %>%
  left_join(
    landing_minima,
    join_by(station == airport),
    relationship = "many-to-many"
  ) %>%
  mutate(
    #this is slow
    max_tailwind = mapply(calc_max_tailwind, wind_reconstructed, rwy_bearing, variation),
  ) %>%
  replace_na(list(max_tailwind = 0)) %>%
  filter(
    max_tailwind < 10
  ) %>%
  group_by(station, issued_time) %>%
  mutate(
    usable_runways = str_c(sort(unique(rwy)), collapse = ", ")
  ) %>%
  slice_min(
    #now choose 1 runway to actually use
    order_by = lowest_haa,
    n = 1,
    with_ties = FALSE
  ) %>%
  summarise(
    lowest_haa = lowest_haa,
    lowest_apch_ban_vis = apch_ban_vis,
    usable_runways = usable_runways,
    .groups = "drop"
  ) 

metars_with_requirements <- metars %>%
  left_join(
    metars_minima,
    join_by(station, issued_time),
    relationship = "one-to-one"
  ) %>%
  mutate(
    suitable_for_landing = !is.na(ceiling) & !is.na(vis) &
      ceiling >= lowest_haa & vis >= lowest_apch_ban_vis
  )

metars_with_requirements %>% write_csv("metars_processed.csv")

#sample dataset
metars_with_requirements %>%
  select(-c(wind_dir, wind_speed, wind_gust, issued)) %>%
  filter(issued_time > ymd(20251001)) %>%
  write_csv("Oct_2025_Processed_METARs.csv")




