#run the following files first!
#1) analyze_hourly_tafs.R
#2) analyze_hourly_metars.R


library(tidyverse)
library(lubridate)
library(ggplot2)

#build schedule - analyze_hourly_tafs already has everything


window_start = as_datetime(ymd(20250101))
window_end = as_datetime(ymd(20251031))+hours(23)

yyq_analysis <- crossing(
  station = "CYYQ",
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
  #slice_max(order_by = issued_time, n = 1, with_ties = FALSE) %>%
  summarise(
    #if there is any METAR issued within the hour that is not suitable for landing, whole hour is considered not suitable
    suitable_for_landing = case_when(
      any(suitable_for_landing == FALSE, na.rm = TRUE) ~ FALSE,        # any FALSE → FALSE
      all(suitable_for_landing == TRUE, na.rm = TRUE) & any(is.na(suitable_for_landing)) ~ NA,  # TRUE + NA → NA
      all(suitable_for_landing == TRUE, na.rm = TRUE) ~ TRUE          # all TRUE → TRUE
    ),
    .groups = "drop"
  )

yyq_suitable_for_landing <- yyq_analysis %>%
  group_by(station) %>%
  summarise(
    suitable_for_landing = mean(as.numeric(suitable_for_landing), na.rm = TRUE)
  )


alt_order <- c("CYGX", "CYTH", "CYYL", "CYNE", "CYQD")

available_alts_wide <- alternate_availability %>% 
  select(station, time, suitable_alternate) %>%
  filter(station %in% alt_order) %>%
  pivot_wider(
   names_from = station,
   values_from = suitable_alternate
  ) %>%
  relocate(CYGX, CYTH, CYYL, CYNE, CYQD, .after = time)


collapsed_alts <- alternate_availability %>%
  filter(suitable_alternate, station %in% alt_order) %>%
  mutate(station = factor(station, levels = alt_order)) %>% # preference order
  group_by(time) %>%
  arrange(station) %>% # ensure preference order
  summarize(
    all_alts = paste(station, collapse = ", "), # all available alternates
    best_alts = paste(head(station, 2), collapse = ", "), # first 2 preferred
    n_alts = n(), # count of available alternates
    .groups = "drop"
  )

yyq_analysis2 <- yyq_analysis %>%
  left_join(
    available_alts_wide,
    join_by(time),
    relationship = "one-to-one"
  ) %>%
  left_join(
    collapsed_alts,
    join_by(time),
    relationship = "one-to-one"
  ) %>%
  replace_na(list(n_alts = 0))

#sample data
yyq_analysis2 %>%
  filter(time>ymd(20251001)) %>%
  write_csv("Oct_2025_Alternate_Availability.csv")


#distribution of number of available alternates:
dis_num_alts <- yyq_analysis2 %>%
  count(n_alts) %>%
  mutate(frac = n / sum(n))

#summary stats
summary_stats <- yyq_analysis2 %>%
  summarize(
    frac_churchill_suitable = mean(suitable_for_landing, na.rm = TRUE),
    frac_CYGX = mean(CYGX, na.rm = TRUE),
    frac_CYTH = mean(CYTH, na.rm = TRUE),
    frac_CYYL = mean(CYYL, na.rm = TRUE),
    frac_CYNE = mean(CYNE, na.rm = TRUE),
    frac_CYQD = mean(CYQD, na.rm = TRUE),
    frac_at_least_1_alt = mean(n_alts >= 1, na.rm = TRUE)
  )


write_csv(summary_stats, "summary_stats.csv")
write_csv(dis_num_alts, "num_alt_dist.csv")




#breakdown of alternate availability, including breakout of unusable
alt_available_breakdown <- alternate_availability %>%
  filter(time >= window_start, time <= window_end) %>%
  mutate(
    outcome = case_when(
      suitable_alternate == FALSE & reason == "TAF UNUSABLE" ~ "UNSUITABLE - TAF UNUSABLE",
      suitable_alternate == FALSE & !(reason == "TAF UNUSABLE") ~ "UNSUITABLE - TAF < ALT MINIMA",
      suitable_alternate == TRUE ~ "SUITABLE"
    )
  ) %>%
  group_by(station, outcome) %>%
  summarise(
    n=n(),
    .groups = "drop"
  )

alt_available_breakdown %>% write_csv("alt_available_breakdown.csv")

