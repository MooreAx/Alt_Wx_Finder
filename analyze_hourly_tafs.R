# assess suitability of station as flight planning alternate

library(tidyverse)
library(lubridate)

#get functions
source("functions.R")

tafs <- read_csv("tafs_hourly.csv", col_types = cols(.default = "c")) %>%
  mutate(
    issued_time = parse_date_time(issued, orders = "ymdHM"),
    .after = issued
  ) %>%
  mutate(
    time = ymd_hms(time),
    valid_taf = case_when(
      status == "NORMAL" ~ TRUE,
      .default = FALSE
    ),
    altmin_ceiling = parse_number(altmin_ceiling),
    altmin_vis = parse_number(altmin_vis),
    prob_ceiling = parse_number(prob_ceiling),
  ) %>%
  #filter(valid_taf == TRUE) %>%
  replace_na(list(altmin_ceiling = Inf))


#read alternate minima req'ts (note, cannot take credit for LPV minima):
alt_min_reqts <- read_csv(
  "AlternateMinimaReqts.csv",
  col_types = cols(.default = "c")
  ) %>%
  mutate(
    precision_apch = parse_number(precision_apch),
    lowest_haa = parse_number(lowest_haa),
    advisory_vis = parse_number(advisory_vis),
    variation = parse_number(variation),
    rwy_bearing = parse_number(rwy_bearing)
  ) %>%
  replace_na(list(variation = 0))

#calculate usable runways to get applicable minimas
tafs_alt_reqts <- tafs %>%
  select(station, issued_time, time, wind) %>%
  left_join(
    alt_min_reqts,
    join_by(station == airport),
    relationship = "many-to-many"
  ) %>%
  mutate(
    #this is slow
    max_tailwind = mapply(calc_max_tailwind, wind, rwy_bearing, variation)
  ) # %>%

## join this up after...

#testing purposes:
yyq_data <- tafs_alt_reqts %>% filter(station == "CYYQ") %>% head(10)

tafs_alt_reqts2 <- tafs_alt_reqts %>%
  filter(
    max_tailwind < 10 #nil tafs dropped here
  ) %>%
  group_by(station, issued_time, time) %>%
  mutate(
    usable_runways = str_c(sort(unique(rwy)), collapse = ", "),
    n_precision_apch = sum(precision_apch)
  ) %>%
  slice_min(
    #now choose 1 runway
    order_by = lowest_haa,
    n = 1,
    with_ties = FALSE
  ) %>%
  summarise(
    n_precision_apch = n_precision_apch,
    lowest_haa = lowest_haa,
    lowest_advisory_vis = advisory_vis,
    usable_runways = usable_runways,
    .groups = "drop"
  ) %>%
  mutate(
    std_alt_min_ceiling = case_when(
      n_precision_apch >= 2 ~ NA, #"std alt minima" only applies with 1 or 0 ILS
      n_precision_apch == 1 ~ 600,
      n_precision_apch == 0 ~ 800
    ),
    std_alt_min_vis = case_when(
      n_precision_apch >= 2 ~ NA, #"std alt minima" only applies with 1 or 0 ILS
      n_precision_apch == 1 ~ 2,
      n_precision_apch == 0 ~ 2
    ),
    required_ceiling = case_when(
      n_precision_apch >= 2 ~ round_ceiling_aviation(pmax(400, lowest_haa + 200)),
      n_precision_apch == 1 ~ round_ceiling_aviation(pmax(600, lowest_haa + 300)),
      n_precision_apch == 0 ~ round_ceiling_aviation(pmax(800, lowest_haa + 300))
    ),
    required_vis = case_when(
      n_precision_apch >= 2 ~ pmax(1, lowest_advisory_vis + 0.5),
      n_precision_apch == 1 ~ pmax(2, lowest_advisory_vis + 1),
      n_precision_apch == 0 ~ pmax(2, lowest_advisory_vis + 1),
    ),
    std_alt_min_applies = case_when(
      required_ceiling == std_alt_min_ceiling & 
        std_alt_min_vis == required_vis ~ TRUE,
      .default = FALSE
    ),
    sam_required_ceiling2 = case_when(
      std_alt_min_applies & std_alt_min_ceiling == 600 ~ 700,
      std_alt_min_applies & std_alt_min_ceiling == 800 ~ 900,
    ),
    sam_required_vis2 = case_when(
      std_alt_min_applies ~ 1.5
    ),
    sam_required_ceiling3 = case_when(
      std_alt_min_applies & std_alt_min_ceiling == 600 ~ 800,
      std_alt_min_applies & std_alt_min_ceiling == 800 ~ 1000,
    ),
    sam_required_vis3 = case_when(
      std_alt_min_applies ~ 1
    )
  ) %>%
  rowwise() %>%
  mutate(
    required_ceiling_vis = paste(
      na.omit(c(
        if (!is.na(required_ceiling) & !is.na(required_vis)) paste0(required_ceiling, "-", required_vis),
        if (!is.na(sam_required_ceiling2) & !is.na(sam_required_vis2)) paste0(sam_required_ceiling2, "-", sam_required_vis2),
        if (!is.na(sam_required_ceiling3) & !is.na(sam_required_vis3)) paste0(sam_required_ceiling3, "-", sam_required_vis3)
      )),
      collapse = ", "
    )
  )

#Now join back with TAF data:

taf_with_requirements <- tafs %>%
  left_join(
    tafs_alt_reqts2,
    join_by(station, issued_time, time),
    relationship = "one-to-one"
  ) %>%
  mutate(
    cond1 = !is.na(required_ceiling) & !is.na(required_vis) &
      (altmin_ceiling >= required_ceiling & altmin_vis >= required_vis),
    cond2 = !is.na(sam_required_ceiling2) & !is.na(sam_required_vis2) &
      (altmin_ceiling >= sam_required_ceiling2 & altmin_vis >=  sam_required_vis2),
    cond3 = !is.na(sam_required_ceiling3) & !is.na(sam_required_vis3) &
      (altmin_ceiling >= sam_required_ceiling3 & altmin_vis >=  sam_required_vis3),
    
    meets_ceiling_and_vis = cond1 | cond2 | cond3,
    
    meets_prob_ceiling = prob_ceiling >= round_ceiling_aviation(lowest_haa),
    
    suitable_alternate = meets_ceiling_and_vis & coalesce(meets_prob_ceiling, TRUE),
    
    reason = case_when(
      status != "NORMAL" ~ "TAF UNUSABLE",
      !suitable_alternate & !meets_ceiling_and_vis & !meets_prob_ceiling ~
        "vis/ceiling < required *AND* PROB ceiling < landing minima",
      !meets_ceiling_and_vis ~ "vis/ceiling < required",
      !meets_prob_ceiling ~ "PROB ceiling < landing minima"
    )
  )

final_taf_data <- taf_with_requirements %>%
  select(-c(std_alt_min_ceiling, std_alt_min_vis, 
            required_ceiling, required_ceiling, std_alt_min_applies, 
            sam_required_ceiling2, sam_required_vis2, sam_required_ceiling3, sam_required_vis3, 
            cond1, cond2, cond3))

#sample dataset
final_taf_data %>%
  select(-c(issued)) %>%
  filter(issued_time >= ymd(20251001)) %>%
  write_csv("Oct_2025_Processed_TAFs.csv")


#now construct a schedule of 1 hour periods for each period, and 
#get the lagged forecast for each hour and whether it can be used as an alternate


alternate_availability <- final_taf_data %>%
  select(station, time) %>%
  group_by(station) %>%
  complete(
    time = seq(min(time), max(time), by = "1 hour"),
    fill = list()
  ) %>%
  ungroup() %>%
  left_join(
    final_taf_data,
    join_by(station, time),
    relationship = "many-to-many"
  ) %>%
  filter(issued_time <= time - hours(3)) %>%
  group_by(station, time) %>%
  slice_max(order_by = issued_time, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  mutate(
    taf_age_hours = as.numeric(difftime(time, issued_time, units = "hours")),
    .after = issued_time
  ) %>%
  select(-issued)

alternate_availability %>% write_csv("available_alternates.csv")


