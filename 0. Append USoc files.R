###################
###### Notes ######
###################

#This script allows various survey files from Understanding Society (respondents, non-respondents, household)
#to be merged, and reshaped into a convenient long format for analysis

#######################
###### Libraries ######
#######################

library("tidyverse")
library("stringr")
library("haven")
library("data.table")
library("tidyr")
library("aws.s3")
library("panelr")
library("splitstackshape")

#Clean up the global environment
rm(list = ls())

#Directories in S3

IHT_bucket <- "s3://thf-dap-tier0-projects-iht-067208b7-projectbucket-1mrmynh0q7ljp"
USOC_subfolder <- "/Understanding Society/UKDA-6614-tab/tab/ukhls"
R_workbench <- path.expand("~")
localgit <- dirname(rstudioapi::getSourceEditorContext()$path)

###########################################
###### Lists of all downloaded files ######
###########################################

bucket_contents <- get_bucket("s3://thf-dap-tier0-projects-iht-067208b7-projectbucket-1mrmynh0q7ljp/Understanding Society/UKDA-6614-tab/tab/ukhls/",
                              prefix = "Understanding Society/UKDA-6614-tab/tab/ukhls/",
                              max = Inf)

bucket_files <- data.frame(file.name=rep(NA,length(bucket_contents)))
for (i in 1:length(bucket_contents)){
  bucket_files$file.name[i] <- bucket_contents[[i]]$Key
}

listind <- filter(bucket_files, endsWith(file.name, "indresp.tab"))
listindall <- filter(bucket_files, endsWith(file.name, "indall.tab"))
listhh <- filter(bucket_files, endsWith(file.name, "hhresp.tab"))
listegoalt <- filter(bucket_files, endsWith(file.name, "egoalt.tab"))

############################
###### Household data ######
############################

hh <- vector(mode = "list", length = nrow(listhh))
for (s in 1:nrow(listhh)){
  print(paste0(round(s/nrow(listhh)*100,0)," %"))

  hh[[s]] <- s3read_using(read.table
                            , object = listhh[s,] # File to open
                            , bucket = IHT_bucket
                            , header = TRUE, sep = "\t", fill = TRUE) # Bucket name defined above
}

#Add any variables you are interested in to this list

hh <- lapply(hh, function(x) x%>% select(ends_with("_hidp"),
                                         ends_with("_psu"),
                                         ends_with("_fihhmnnet1_dv"),
                                         ends_with("_ieqmoecd_dv"),
                                         contains("fihhmngrs"),
                                         contains("foodbank"),
                                         contains("hhsize")))

names(hh) <- stringr::word(listhh[,1],5,sep="/") %>% str_replace_all(.,".tab","")

#extract the individual dataframes from the list
list2env(hh, .GlobalEnv)

############################################
###### Individuals data (respondents) ######
############################################

ind <- vector(mode = "list", length = nrow(listind))
for (s in 1:nrow(listind)){
  print(paste0(round(s/nrow(listhh)*100,0)," %"))
  
  ind[[s]] <- s3read_using(read.table
                          , object = listind[s,] # File to open
                          , bucket = IHT_bucket
                          , header = TRUE, sep = "\t", fill = TRUE) # Bucket name defined above
}

#Add any variables you are interested in to this list

ind <- lapply(ind, function(x) x%>% select(ends_with("pno"),
                                           ends_with("idp"),
                                           ends_with("_jbstat"),
                                           ends_with("_health"),
                                           ends_with("_scsf1"),contains("scghql"),
                                           -ends_with("_ff_jbstat"),
                                           ends_with("_sclfsato"),contains("sclonely"), 
                                           ends_with("_health"), 
                                           ends_with("_sex_dv"), 
                                           ends_with("_age_dv"),
                                           contains("urban_dv"),
                                           contains("nchild_dv"),contains("npensioner_dv"),
                                           ends_with("_mastat_dv"), 
                                           ends_with("_hiqual_dv"),
                                           ends_with("_xw"),ends_with("_lw"),ends_with("_xd"),ends_with("_li"),ends_with("strata"),
                                           contains("aid"),
                                           contains("casch"),
                                           contains("adl"),
                                           contains("hlpinf"),contains("hinfano"),contains("hlpform"),
                                           contains("privval"),contains("privper"),contains("helphours"),
                                           contains("helpcode"),contains("privval"),contains("privper"),
                                           contains("havedp"),contains("incass"),contains("lahelp"),
                                           contains("careass"),contains("persbudg"),
                                           contains("lacare"),contains("laval"),contains("laper"),contains("paypriv"),
                                           contains("hl2hop"),contains("hl2gp"),
                                           contains("_hosp"),
                                           contains("_ypltdis"),
                                           contains("hcond"),contains("mhealthtyp"),contains("hcondcode"),
                                           contains("ethn_dv"),
                                           contains("country"),
                                           contains("gor_dv"),
                                           contains("prfitb"),
                                           contains("intdatd_dv"),contains("intdatm_dv"),contains("intdaty_dv"),
                                           contains("casch"),contains("aideft"),contains("finnow"),
                                           contains("finfut"),contains("jbft_dv"),contains("benbase"),
                                           contains("bendis"),contains("othben")))

ind <- lapply(ind, function(x) x%>% mutate(indresp="1"))

names(ind) <- stringr::word(listind[,1],5,sep="/") %>% str_replace_all(.,".tab","")

list2env(ind, .GlobalEnv)

#######################################################################
###### Ego-alt files for relationships between household members ######
#######################################################################

egoalt <- vector(mode = "list", length = nrow(listegoalt))
for (s in 1:nrow(listegoalt)){
  print(paste0(round(s/nrow(listegoalt)*100,0)," %"))
  
  egoalt[[s]] <- s3read_using(read.table
                          , object = listegoalt[s,] # File to open
                          , bucket = IHT_bucket
                          , header = TRUE, sep = "\t", fill = TRUE) # Bucket name defined above
}

#Add any variables you are interested in to this list

egoalt <- lapply(egoalt, function(x) x%>% select(ends_with("pno"),
                                                 ends_with("pidp"),
                                                 ends_with("_hidp"),
                                                 ends_with("_relationship_dv")))

names(egoalt) <- stringr::word(listegoalt[,1],5,sep="/") %>% str_replace_all(.,".tab","")

for (s in 1:nrow(listegoalt)){
  print(paste0(round(s/nrow(listegoalt)*100,0)," %"))
  
  egoalt[[s]]$wave <- names(egoalt)[[s]]

  names(egoalt[[s]]) <- str_replace_all(names(egoalt[[s]]),paste0(substr(names(egoalt)[s],1,1),"_"),"")

}

egoalt <- rbindlist(egoalt) %>%
  mutate(wave=str_replace_all(wave,"_egoalt",""))

s3write_using(egoalt # What R object we are saving
              , FUN = fwrite # Which R function we are using to save
              , object = paste0("/Understanding Society/","egoalt long.csv") # Name of the file to save to (include file type)
              , bucket = IHT_bucket) # Bucket name defined above

######################################################
###### Individuals data (incl. non-respondents) ######
######################################################

indall <- vector(mode = "list", length = nrow(listindall))
for (s in 1:nrow(listindall)){
  print(paste0(round(s/nrow(listindall)*100,0)," %"))
  
  indall[[s]] <- s3read_using(read.table
                              , object = listindall[s,] # File to open
                              , bucket = IHT_bucket
                              , header = TRUE, sep = "\t", fill = TRUE) # Bucket name defined above
}

#Add any variables you are interested in to this list

indall <- lapply(indall, function(x) x%>% select(ends_with("pno"),
                                                 ends_with("pidp"),
                                                 ends_with("_hidp"),
                                                 contains("sex_dv"),contains("age_dv"),contains("intdat"),
                                                 contains("npensioner_dv"),contains("nchild_dv"),contains("country"),
                                                 contains("gor_dv"),contains("urban_dv"),contains("reasref")))

indall <- lapply(indall, function(x) x%>% mutate(indall="1"))

names(indall) <- stringr::word(listindall[,1],5,sep="/") %>% str_replace_all(.,".tab","")

list2env(indall, .GlobalEnv)

#Add non-respondents to respondent list and de-duplicate
#This isn't interated - repeat for any additional waves you want to add

a_indresp <- plyr::rbind.fill(a_indresp,a_indall) %>% arrange(pidp,desc(indresp)) %>% group_by(pidp) %>% mutate(dup=row_number()) %>% ungroup() %>% filter(dup==1) %>% mutate(a_indall=ifelse(!is.na(indall),1,ifelse(is.na(indall),0,0))) %>% select(-c("indall","indresp","dup")) 
b_indresp <- plyr::rbind.fill(b_indresp,b_indall) %>% arrange(pidp,desc(indresp)) %>% group_by(pidp) %>% mutate(dup=row_number()) %>% ungroup() %>% filter(dup==1) %>% mutate(b_indall=ifelse(!is.na(indall),1,ifelse(is.na(indall),0,0))) %>% select(-c("indall","indresp","dup")) 
c_indresp <- plyr::rbind.fill(c_indresp,c_indall) %>% arrange(pidp,desc(indresp)) %>% group_by(pidp) %>% mutate(dup=row_number()) %>% ungroup() %>% filter(dup==1) %>% mutate(c_indall=ifelse(!is.na(indall),1,ifelse(is.na(indall),0,0))) %>% select(-c("indall","indresp","dup"))
d_indresp <- plyr::rbind.fill(d_indresp,d_indall) %>% arrange(pidp,desc(indresp)) %>% group_by(pidp) %>% mutate(dup=row_number()) %>% ungroup() %>% filter(dup==1) %>% mutate(d_indall=ifelse(!is.na(indall),1,ifelse(is.na(indall),0,0))) %>% select(-c("indall","indresp","dup"))
e_indresp <- plyr::rbind.fill(e_indresp,e_indall) %>% arrange(pidp,desc(indresp)) %>% group_by(pidp) %>% mutate(dup=row_number()) %>% ungroup() %>% filter(dup==1) %>% mutate(e_indall=ifelse(!is.na(indall),1,ifelse(is.na(indall),0,0))) %>% select(-c("indall","indresp","dup"))
f_indresp <- plyr::rbind.fill(f_indresp,f_indall) %>% arrange(pidp,desc(indresp)) %>% group_by(pidp) %>% mutate(dup=row_number()) %>% ungroup() %>% filter(dup==1) %>% mutate(f_indall=ifelse(!is.na(indall),1,ifelse(is.na(indall),0,0))) %>% select(-c("indall","indresp","dup"))
g_indresp <- plyr::rbind.fill(g_indresp,g_indall) %>% arrange(pidp,desc(indresp)) %>% group_by(pidp) %>% mutate(dup=row_number()) %>% ungroup() %>% filter(dup==1) %>% mutate(g_indall=ifelse(!is.na(indall),1,ifelse(is.na(indall),0,0))) %>% select(-c("indall","indresp","dup"))
h_indresp <- plyr::rbind.fill(h_indresp,h_indall) %>% arrange(pidp,desc(indresp)) %>% group_by(pidp) %>% mutate(dup=row_number()) %>% ungroup() %>% filter(dup==1) %>% mutate(h_indall=ifelse(!is.na(indall),1,ifelse(is.na(indall),0,0))) %>% select(-c("indall","indresp","dup"))
i_indresp <- plyr::rbind.fill(i_indresp,i_indall) %>% arrange(pidp,desc(indresp)) %>% group_by(pidp) %>% mutate(dup=row_number()) %>% ungroup() %>% filter(dup==1) %>% mutate(i_indall=ifelse(!is.na(indall),1,ifelse(is.na(indall),0,0))) %>% select(-c("indall","indresp","dup"))
j_indresp <- plyr::rbind.fill(j_indresp,j_indall) %>% arrange(pidp,desc(indresp)) %>% group_by(pidp) %>% mutate(dup=row_number()) %>% ungroup() %>% filter(dup==1) %>% mutate(j_indall=ifelse(!is.na(indall),1,ifelse(is.na(indall),0,0))) %>% select(-c("indall","indresp","dup"))
k_indresp <- plyr::rbind.fill(k_indresp,k_indall) %>% arrange(pidp,desc(indresp)) %>% group_by(pidp) %>% mutate(dup=row_number()) %>% ungroup() %>% filter(dup==1) %>% mutate(k_indall=ifelse(!is.na(indall),1,ifelse(is.na(indall),0,0))) %>% select(-c("indall","indresp","dup"))
l_indresp <- plyr::rbind.fill(l_indresp,l_indall) %>% arrange(pidp,desc(indresp)) %>% group_by(pidp) %>% mutate(dup=row_number()) %>% ungroup() %>% filter(dup==1) %>% mutate(l_indall=ifelse(!is.na(indall),1,ifelse(is.na(indall),0,0))) %>% select(-c("indall","indresp","dup"))

#Merge individual and household data
#This isn't interated - repeat for any additional waves you want to add

u1 <- merge(a_indresp, a_hhresp, by="a_hidp")
u2 <- merge(b_indresp, b_hhresp, by="b_hidp")
u3 <- merge(c_indresp, c_hhresp, by="c_hidp")
u4 <- merge(d_indresp, d_hhresp, by="d_hidp")
u5 <- merge(e_indresp, e_hhresp, by="e_hidp")
u6 <- merge(f_indresp, f_hhresp, by="f_hidp")
u7 <- merge(g_indresp, g_hhresp, by="g_hidp")
u8 <- merge(h_indresp, h_hhresp, by="h_hidp")
u9 <- merge(i_indresp, i_hhresp, by="i_hidp")
u10 <- merge(j_indresp, j_hhresp, by="j_hidp")
u11 <- merge(k_indresp, k_hhresp, by="k_hidp")
u12 <- merge(l_indresp, l_hhresp, by="l_hidp")

#Merge the waves, resulting in "wide" data.  
#Expand the list for additional waves.

usoc <- Reduce(function(x,y) merge(x, y, by="pidp", all=TRUE), list(u1, u2, u3, u4, u5, u6, u7, u8, u9, u10, u11, u12))

#Cleanup files

rm(list=ls(pattern = "[0-9]$"), ind, hh)
rm(list=ls(pattern = "resp"))

#Change names so we can pivot longer

part1 <- word(names(usoc)[-1], 2, -1,sep="_") %>% str_replace_all(.,"_","")
part2 <- word(names(usoc)[-1], 1,sep="_")
names(usoc) <- c("pidp",paste0(part1,"_",part2))

#############################
###### Reshape to long ######
#############################

#For reshape to long, the wave specifier needs to be at the 
#end (so, sex_a, not a_sex).  The lines below will move the 
#prefix to the end for the patterns of variable names used in 
#this example; they might not capture all possible patterns.

usoc_long <- merged.stack(usoc, var.stubs = unique(part1), sep = "_") %>%
  dplyr::rename(wave='.time_1')

#######################
###### Save file ######
#######################

s3write_using(usoc_long # What R object we are saving
              , FUN = fwrite # Which R function we are using to save
              , object = paste0("/Understanding Society/","USOC long.csv") # Name of the file to save to (include file type)
              , bucket = IHT_bucket) # Bucket name defined above