### This file is for converting CARS Model outputs to RDS for dashboard data ####
library(dplyr)
dest_Dir = "C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/DATA"
setwd("C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/")

dump_date = '2021-07-08'
prev_dump_dates = '2020-12-21'
#dump_date = '2019-03-20'
#prev_dump_dates = '2018-12-24'

DOT_Exclusion = read.csv(paste0("C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/DOT_excluded_",dump_date,".csv"),stringsAsFactors = F)
Train_DOT_Exclusion =  DOT_Exclusion %>% filter(IS_TRAIN==1)
Test_DOT_Exclusion = DOT_Exclusion %>% filter(IS_TRAIN ==0)

#Train_data <- read.csv(paste0("C:/Users/aparfenyuk/source/repos/CARS/3_CARSModelRuns/CSV/Train_Data_",dump_date,".csv"),stringsAsFactors = F)
#Train_Data <- anti_join(Train_data,Train_DOT_Exclusion %>% select(DOT_NUMBER),c('DOT_NUMBER'='DOT_NUMBER'))
#saveRDS(Train_Data,paste(dest_Dir,paste0("model_data_",dump_date,"_2017_2018.rds"),sep="/"))

#Test_data <- read.csv(paste0("C:/Users/aparfenyuk/source/repos/CARS/3_CARSModelRuns/CSV/Test_Data_",dump_date,".csv"),stringsAsFactors = F)
#Test_Data <- anti_join(Test_data,Test_DOT_Exclusion %>% select(DOT_NUMBER),c('DOT_NUMBER'='DOT_NUMBER'))
#saveRDS(Test_Data,paste(dest_Dir,paste0("model_data_",dump_date,"_2018_2019.rds"),sep="/"))

Result <- read.csv(paste0("C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/cab_",dump_date,".csv"),stringsAsFactors = F)
Result$X.1 = NULL
saveRDS(Result,paste(dest_Dir,paste0("cab_",dump_date,".rds"),sep="/"))

insurer <- read.csv(paste0("C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/cab_score_insurer_",dump_date,".csv"),stringsAsFactors = F)
saveRDS(insurer,paste(dest_Dir,paste0("company_insurer_data_",dump_date,".rds"),sep="/"))

varimp <- read.csv(paste0("C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/cab_variable_importance_rolling_",dump_date,".csv"),stringsAsFactors = F)
saveRDS(varimp,paste(dest_Dir,paste0("model_varimps_",dump_date,".rds"),sep="/"))

#all_dumps_dots <- readRDS(paste0("F:/Philadelphia/GTC/CARS_final_20200109/TigerCode/TigerCode/Vivek/CAB/MG_new/Data/all_dump_dots_",prev_dump_dates,".RDS"))
#dump_dots <- data.frame('DUMP_DATE' = dump_date,'DOT_NUMBER'= Test_Data$DOT_NUMBER)
#all_dumps_dots_New = rbind(all_dumps_dots,dump_dots)
#saveRDS(all_dumps_dots_New,paste0("F:/Philadelphia/GTC/CARS_final_20200109/TigerCode/TigerCode/Vivek/CAB/MG_new/Data/all_dump_dots_",dump_date,".rds"))









































































import pandas as pd
import numpy as np
import os
os.chdir('C:/TFS/CARS_DEV/CARS/CARS/3_CARS_Model_Refresh')
import sys
sys.dont_write_bytecode = True

import copy
from cars.config import default_config
from Process_Results import *
import pdb

#===========================================================================================
""" CARS Risk Score Model (Compound Model) """
#==========================================================================================


dump_date = '2021-07-08'

dataset_path = 'C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/PICKLE/dataset_'+dump_date+'.pickle'
train_path = 'C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/Train_Data_'+dump_date+'.csv'
test_path = 'C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/Test_Data_'+dump_date+'.csv'
insurers_path = 'C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/Insurance_'+dump_date+'.csv'
latest_totpwr_path = 'C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/recent_total_power_'+dump_date+'.csv'
excluded_dots_path = 'C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/DOT_excluded_'+dump_date+'.csv'
models_path = 'C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/PICKLE/carsmodel_'+dump_date+'.pickle'
results_path = 'C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/cab_'+dump_date+'.csv'
varimps_path = 'C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/variable_importance_rolling_'+dump_date+'.csv'
insurer_score_path = 'C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/cars_model_score_insurer_'+dump_date+'.csv'
#census_path = 'C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/census_data.csv'


my_config = copy.deepcopy(default_config)
dataset = get_data(my_config,dataset_path,train_path,test_path,insurers_path,latest_totpwr_path,excluded_dots_path)
is_monotonic = False

## Monotonic constraints are ignored
my_config['database']['dataset']['variables']['ignore_monotonicity']= True
models,importances = get_models(models_path,dataset,my_config,is_monotonic)
model_results = get_cars_results(dataset,models,importances,insurers_path,is_monotonic)
model_results = change_column_names(model_results)
Score_insurer = get_model_score_insurer(dataset,model_results)
#Consildated_data = get_Consolidated_Data(model_results,census_path)

importances.to_csv('C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/cab_variable_importance_rolling_'+dump_date+'.csv',index=False)
model_results.to_csv('C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/cab_'+dump_date+'.csv',index=False)
Score_insurer.to_csv('C:/TFS/CARS_DEV/CARS/CARS/3_CARSModelRuns/CSV/cab_score_insurer_'+dump_date+'.csv',index=False)
