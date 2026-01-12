#The updated version (run in January 2026) produces different results over the
#same time period (10 months ending in Oct 31). The updated version finds aerodromes
#are suitable for use as an alternate much less often. The data is the same,
#so this file will be used to analyze the results and try to identify what
#is causing differences.

old_file <- read_csv("Oct_2025_Processed_TAFs___OG.csv")

compare_results <- old_file %>% 
  distinct() %>%
  left_join(
    alternate_availability,
    join_by(station, time),
    relationship = "many-to-one"
  )

#so one problem in the old file is station-times are not unique: for example
# CYGX on 2025-10-01 at 13:00


#find differences: different tafs
differences1 <- compare_results %>%
  filter(
    raw_taf.x != raw_taf.y,
    status == "NORMAL"
  )

differences1 %>% write_csv("differences/differences1.csv")

#FINDING1: in all the instances of different TAFs getting pulled for the
#same station-time, the inital version is incorrect. The later version pulls
#the correct TAF. This is closed.

#same taf, different result
differences2 <- compare_results %>%
  filter(
    raw_taf.x == raw_taf.y,
    suitable_alternate.x != suitable_alternate.y
  )

differences2 %>% write_csv("differences/differences2.csv")

#FINDING2: VRB wind was not getting processed properly in the latest version,
#and was throwing NAs. So anytime the wind was VRB --> aerodrome was unsuitable.
#Bug was corrected and the results are now largely in agreement with what was
#found before. This is closed.
