#run the following files first!
#1) analyze_hourly_tafs.R
#2) analyze_hourly_metars.R


library(tidyverse)
library(lubridate)
library(gglplot2)

#build schedule - analyze_hourly_tafs already has everything


window_start = as_datetime(ymd(20250101))
window_end = as_datetime(ymd(20251031))+hours(23)

yyq_analysis <- crossing(
  station = "CYYQ",
  time = seq(window_start, window_end, by = "1 hour"),
  ) %>%
  left_join(
    metars_with_requirements %>% select(station, issued_time, raw, wind_reconstructed, vis, sigwx, clouds, ceiling, usable_runways, lowest_haa, lowest_ops_spec_vis, suitable_for_landing),
    join_by(station),
    relationship = "many-to-many"
  ) %>%
  filter(
    issued_time <= time
  ) %>%
  group_by(station, time) %>%
  slice_max(order_by = issued_time, n = 1, with_ties = FALSE) %>%
  ungroup()


alt_order <- c("CYGX", "CYTH", "CYYL", "CYNE", "CYQD")

available_alts_wide <- alternate_availability %>% 
  select(station, time, suitable_alternate) %>%
  pivot_wider(
   names_from = station,
   values_from = suitable_alternate
  ) %>%
  select(-CYYQ) %>%
  relocate(CYGX, CYTH, CYYL, CYNE, CYQD, .after = time)


collapsed_alts <- alternate_availability %>%
  filter(suitable_alternate, station != "CYYQ") %>%
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
  )

#sample data
yyq_analysis2 %>%
  select(-c(issued_time:lowest_ops_spec_vis)) %>%
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
