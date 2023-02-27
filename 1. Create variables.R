#######################
###### Libraries ######
#######################

library(tidyverse)
library(stringr)
library(tidyr)
library(purrr)
library(pbapply)
library(data.table)
library(janitor)
library(ggrepel)
library(aws.s3)
library(parallel)
library(pbmcapply)

##########################
###### Read in data ######
##########################

#Clean up the global environment

rm(list = ls())

#Directories in S3

IHT_bucket <- "s3://thf-dap-tier0-projects-iht-067208b7-projectbucket-1mrmynh0q7ljp"
USOC_subfolder <- "/Understanding Society/UKDA-6614-tab/tab/ukhls"
R_workbench <- path.expand("~")
localgit <- dirname(rstudioapi::getSourceEditorContext()$path)

#Read in data

#Survey responses
usoc_long <- s3read_using(fread
                          , object = paste0("/Understanding Society/","USOC long.csv") # File to open
                          , bucket = IHT_bucket) # Bucket name defined above

#Relationships between respondents
egoalt_long <- s3read_using(fread
                            , object = paste0("/Understanding Society/","egoalt long.csv") # File to open
                            , bucket = IHT_bucket) # Bucket name defined above

#Add wave variable

wave.mini <- data.frame(wave=letters,wave_num=1:length(letters))

usoc_long <- usoc_long %>%
  mutate(wave=as.character(wave)) %>% 
  left_join(.,wave.mini,by="wave") %>%
  mutate(wave_pidp=paste(wave_num,pidp,sep="-"))

egoalt_long <- egoalt_long %>%
  mutate(wave=as.character(wave)) %>% 
  left_join(.,wave.mini,by="wave")

#Date variable

usoc_long <- usoc_long %>%
  mutate(.,itv_date=lubridate::ymd(paste(intdatydv,intdatmdv,intdatddv,sep="-")),
         itv_year=as.character(ifelse(intdatydv>0,intdatydv,NA)))

#Was interviewed

usoc_long <- usoc_long %>%
  mutate(interviewed=ifelse(indall==0&!is.na(indall),1,ifelse(is.na(indall)|indall==1,0,0)))

#Add wave years
wave_years_small <- usoc_long %>%
  group_by(wave_num) %>%
  summarise(year_min=min(itv_year,na.rm=TRUE),
            year_max=max(itv_year,na.rm=TRUE)) %>% 
  ungroup() %>%
  mutate(year_min_max=paste(year_min,year_max,sep="-")) %>%
  select(wave_num,year_min_max)

usoc_long <- usoc_long %>%
  left_join(.,wave_years_small,by="wave_num")
rm(wave_years_small)

###########################
###### New variables ######
###########################

#Age groups
usoc_long <- usoc_long %>%
  mutate(agedv=ifelse(agedv<0,NA,agedv)) %>%
  mutate(.,agecatlong=case_when(agedv>=16&agedv<=30 ~ "16 to 30",
                            agedv>=31&agedv<=40 ~ "30 to 40",
                            agedv>=41&agedv<=50 ~ "40 to 50",
                            agedv>=51&agedv<=60 ~ "50 to 60",
                            agedv>=61&agedv<=70 ~ "60 to 70",
                            agedv>=71 ~ "70+",
                            TRUE ~ "NA"), 
         agecat=case_when(agedv>=16&agedv<=30 ~ "16 to 30",
                            agedv>=31&agedv<=50 ~ "31 to 50",
                            agedv>=51&agedv<=70 ~ "51 to 70",
                            agedv>=71 ~ "70+",
                            TRUE ~ "NA"))

#Recode sex
usoc_long <- usoc_long %>%
  mutate(.,sexdv=factor(x=as.character(sexdv),
                        labels=c("Male","Female"),
                        levels = as.character(1:2)))
#Recode region

# Value label	Value	Absolute frequency	Relative frequency	
# missing	-9	6	0.07%	
# North East	1	276	3.22%	
# North West	2	967	11.29%	
# Yorkshire and the Humber	3	812	9.48%	
# East Midlands	4	587	6.85%	
# West Midlands	5	807	9.42%	
# East of England	6	746	8.71%	
# London	7	1239	14.46%	
# South East	8	933	10.89%	
# South West	9	628	7.33%	
# Wales	10	469	5.47%	
# Scotland	11	588	6.86%	
# Northern Ireland	12	510	5.95%	

usoc_long <- usoc_long %>%
  mutate(.,gordv=factor(x=as.character(gordv),
                        labels=c("North East","North West","Yorkshire and the Humber",
                                 "East Midlands","West Midlands","East of England",
                                 "London","South East","South West",
                                 "Wales","Scotland","Northern Ireland"),
                        levels = as.character(1:12)))

#Recode job status

# Refused	-2	51	0.16%	
# Dont know	-1	18	0.06%	
# Self employed	1	2431	7.6%	
# Paid employment(ft/pt)	2	15000	46.87%	
# Unemployed	3	1303	4.07%	
# Retired	4	8405	26.26%	
# On maternity leave	5	178	0.56%	
# Family care or home	6	1241	3.88%	
# Full-time student	7	1770	5.53%	
# LT sick or disabled	8	1093	3.41%	
# Govt training scheme	9	16	0.05%	
# Unpaid, family business	10	22	0.07%	
# On apprenticeship	11	80	0.25%	
# 12	156	0.49%	
# 13	25	0.08%	
# Doing something else	97	217	0.68%

usoc_long <- usoc_long %>%
  mutate(.,jbstat=ifelse(jbstat==12|jbstat==13,11,jbstat)) %>% 
  mutate(.,jbstat=factor(x=as.character(jbstat),
                         labels=c("Self employed","Paid employment (ft/pt)","Unemployed",
                                  "Retired","On maternity leave","Family care or home",
                                  "Full-time student","LT sick or disabled","Govt training scheme",
                                  "Unpaid, family business","On apprenticeship",
                                  "Doing something else"),
                         levels = as.character(c(1:11,97))))

#Recode relationship

# Value label	Value	Absolute frequency	Relative frequency	
# missing	-9	160	0.15%	
# husband/wife	1	18882	17.6%	
# partner/cohabitee	2	4440	4.14%	
# civil partner	3	118	0.11%	
# natural son/daughter	4	26335	24.55%	
# adopted son/daughter	5	225	0.21%	
# foster child	6	70	0.07%	
# stepson/stepdaughter	7	889	0.83%	
# son-in-law/daughter-in-law	8	416	0.39%	
# natural parent	9	26335	24.55%	
# adoptive parent	10	225	0.21%	
# foster parent	11	70	0.07%	
# step-parent	12	889	0.83%	
# parent-in-law	13	416	0.39%	
# natural brother/sister	14	19528	18.2%	
# half-brother/sister	15	1595	1.49%	
# step-brother/sister	16	283	0.26%	
# adopted brother/sister	17	108	0.1%	
# foster brother/sister	18	50	0.05%	
# brother/sister-in-law	19	434	0.4%	
# grand-child	20	1097	1.02%	
# grand-parent	21	1097	1.02%	
# cousin	22	258	0.24%	
# aunt/uncle	23	581	0.54%	
# niece/nephew	24	581	0.54%	
# other relative	25	164	0.15%	
# employee	26	2	0.0%	
# employer	27	2	0.0%	
# lodger/boarder/tenant	28	111	0.1%	
# landlord/landlady	29	111	0.1%	
# other non-relative	30	1804	1.68%

egoalt_long <- egoalt_long %>%
  mutate(.,relationshipdv=factor(x=as.character(relationship_dv),
                                 labels=c("husband/wife","partner/cohabitee","civil partner","natural son/daughter","adopted son/daughter",
                                          "foster child","stepson/stepdaughter","son-in-law/daughter-in-law","natural parent","adoptive parent",
                                          "foster parent","step-parent","parent-in-law","natural brother/sister","half-brother/sister",
                                          "step-brother/sister","adopted brother/sister","foster brother/sister","brother/sister-in-law","grand-child",
                                          "grand-parent","cousin","aunt/uncle","niece/nephew","other relative",
                                          "employee","employer","lodger/boarder/tenant","landlord/landlady","other non-relative"),
                                 levels = as.character(1:30)))

#Gross and equivalised household income

usoc_long <- usoc_long %>%
  mutate(fihhmngrsdv=as.numeric(fihhmngrsdv),
         hhsize=as.numeric(hhsize)) %>%
  mutate(annual_household_income=12*fihhmngrsdv) %>%
  mutate(equ_annual_household_income=annual_household_income/sqrt(hhsize)) %>%
  mutate(annual_household_income_pp=annual_household_income/hhsize)

#This is specific to wave 12, using a survey weight
usoc_long <- usoc_long %>%
  mutate(equ_annual_income_dec=case_when(equ_annual_household_income>=0&equ_annual_household_income<=12042 ~ "1 (10% poorest)",
                                         equ_annual_household_income>12042&equ_annual_household_income<=16099.02 ~ "2",
                                         equ_annual_household_income>16099.02&equ_annual_household_income<=19521.83 ~ "3",
                                         equ_annual_household_income>19521.83&equ_annual_household_income<=23035.33 ~ "4",
                                         equ_annual_household_income>23035.33&equ_annual_household_income<=26919.31 ~ "5",
                                         equ_annual_household_income>26919.31&equ_annual_household_income<=31448.52 ~ "6",
                                         equ_annual_household_income>31448.52&equ_annual_household_income<=36706.52 ~ "7",
                                         equ_annual_household_income>36706.52&equ_annual_household_income<=44249.52 ~ "8",
                                         equ_annual_household_income>44249.52&equ_annual_household_income<=56594.76 ~ "9",
                                         equ_annual_household_income>56594.76 ~ "10 (10% richest)")) %>%
  mutate(equ_annual_income_dec=as.factor(equ_annual_income_dec)) %>% 
  mutate(equ_annual_income_dec=fct_relevel(equ_annual_income_dec,
                                           c("1 (10% poorest)",2:9,"10 (10% richest)")))

#######################
###### Save data ######
#######################

s3write_using(usoc_long # What R object we are saving
              , FUN = fwrite # Which R function we are using to save
              , object = paste0("/Understanding Society/","USOC long with new variables mini.csv") # Name of the file to save to (include file type)
              , bucket = IHT_bucket) # Bucket name defined above