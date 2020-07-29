# -*- coding: utf-8 -*-
"""
Created: Wed Aug 02 2017
Last change: Thu Jul 29 2020, 10:45

@author: Alexander Buchholz, Gilang Hardadi
"""
# Script Footprint2013_ITCpxp.py
# Define version
version = str('Jul-29-2020')
#%%


################
# INTRODUCTION #
################
# This script is written in order to calculate four different income-specific environmental
# footprints of German household consumption: carbon, land use, material use and water use.
# To do so, it will import the MRIO table Exiobase 3.6 and data from a consumer survey
# that was conducted by the Federal Statistical Office of Germany.
# Results will be saved in two different matrices and the following vector:
    # 1) A footprint type * iteration matrix
    # 2) A product type * iteration matrix
    # 3) A vector containing the overall amount of money spent during an iteration
# Before starting:    
# Define the number of iterations for the MC-Simulation
iterations = 1000
#%%


###################################
# 1. Step: Income group selection #
###################################
# First, one has to select, which income group will be selected by choosing the corresponding number.
# National average (0) has to be selected in the initial run to obtain the correspondence matrices
# that later will be used to estimate final demand (in EXIOBASE sectors) of other income groups.

# The income groups are defined in the following way:
# 0 = national average
# 1 = 500 - < 900                                         
# 2 = 900 - < 1300
# 3 = 1300 - < 1500
# 4 = 1500 - < 1700
# 5 = 1700 - < 2000
# 6 = 2000 - < 2600
# 7 = 2600 - < 3600
# 8 = 3600 - < 5000
# 9 = 5000 - < 7500
# 10 = 7500 - < 10000
# 11 = 10000 - < 18000

income_group = 0
#%%

#########################################
# 2. Step: Import of required libraries #
#########################################
# In this step all the libraries needed within the script are imported
import os  
import sys
import logging
import xlrd, xlsxwriter
import numpy as np
import time
import datetime
import scipy.io
import scipy
import shutil   
import uuid
import pandas as pd
from scipy.sparse import csr_matrix
from scipy.stats import truncnorm
#%%

  

############################################
# 3. Step: Definition of cut out functions #
############################################
# By Stefan Pauliuk
# Definition of a funtion that allows to cut out parts of a given string
def config_string_cutout(String, Code, leftstart, rightstart): 
    # Returns substring between = and EoL for given identifier in config file
    # example: if Config_File_Line is "Data_Path_Network_1=K:\Research_Data" the function call
    # config_string_cutout(Config_File_Line,'Data_Path_Network_1','=','\n') will return 'K:\Research_Data'
    Codeindex = String.find(Code,0,len(String))
    if Codeindex == -1:
        return 'None'
    else:
        Startindex = String.find(leftstart,Codeindex,len(String)) 
        Endindex   = String.find(rightstart,Codeindex,len(String))
        return String[Startindex +1:Endindex]
#%%



########################################
# 4. Step: Configuration of the script #
########################################
# By Stefan Pauliuk
# In this step the script will be configurated be reading the configuration text and excel file
# and by importing custom functions made by Stefan Pauliuk
# Read configuration data
FolderPath = os.path.expanduser("~\\PythonConfigFile.txt") ## machine-dependent but OS independent path finder
FolderFile = open(FolderPath, 'r') 
FolderText = FolderFile.read()
# Extract path names from main file
ProjectSpecs_User_Name     = config_string_cutout(FolderText,'UserName','=','\n').strip()
ProjectSpecs_Path_Main     = config_string_cutout(FolderText,'Project_Path_1','=','\n').strip()
ProjectSpecs_Name_ConFile  = config_string_cutout(FolderText,'Configuration_File_1','=','\n').strip()
ProjectSpecs_DataPath1     = config_string_cutout(FolderText,'MRIO_Model_Path','=','\n').strip()
ProjectSpecs_PackagePath1  = config_string_cutout(FolderText,'Package_Path_1','=','\n').strip()
sys.path.append(ProjectSpecs_PackagePath1)
ProjectSpecs_DataBaseUser  = config_string_cutout(FolderText,'DB_User','=','\n').strip()
ProjectSpecs_DataBasePW    = config_string_cutout(FolderText,'DB_PW','=','\n').strip()

# Import packages whose location is now on the system path:    
import Utils_Pauliuk as up

# Load project-specific config file
Project_Configfile  = xlrd.open_workbook(ProjectSpecs_Path_Main + 'Calculation\\' + ProjectSpecs_Name_ConFile)
Project_Configsheet = Project_Configfile.sheet_by_name('Config')
# Naming script and defining of name specifications (e.g. date when the script was used)
Name_Script        = Project_Configsheet.cell_value(6,3)
if Name_Script != 'Footprint2013_ITCpxp': # Name of this script must equal the specified name in the Excel config file
    print('Fatal error: The name of the current script does not match to the sript name specfied in the project configuration file. Exiting the script.')
    sys.exit()
Name_Scenario      = Project_Configsheet.cell_value(5,3)
StartTime          = datetime.datetime.now()
TimeString         = str(StartTime.year) + '_' + str(StartTime.month) + '_' + str(StartTime.day) + '__' + str(StartTime.hour) + '_' + str(StartTime.minute) + '_' + str(StartTime.second)
DateString         = str(StartTime.year) + '_' + str(StartTime.month) + '_' + str(StartTime.day)
Path_Result        = ProjectSpecs_Path_Main + 'Results\\' + Name_Script + '\\' + Name_Scenario + '_' + TimeString + '\\'
# Importing information about Exiobase 2.2 from excel file
EB2_NoofCountries  = int(Project_Configsheet.cell_value(4,8))
EB2_NoofProducts   = int(Project_Configsheet.cell_value(5,8))
EB2_NoofIndustries = int(Project_Configsheet.cell_value(6,8))
EB2_NoofIOSectors  = int(Project_Configsheet.cell_value(7,8))
EB2_NoofFDCategories = int(Project_Configsheet.cell_value(8,8))
# Read control and selection parameters into dictionary
ScriptConfig = {'Scenario_Description': Project_Configsheet.cell_value(7,3)}
for m in range(10,16): # add all dfined control parameters to dictionary
    ScriptConfig[Project_Configsheet.cell_value(m,1)] = Project_Configsheet.cell_value(m,3)
ScriptConfig['Current_UUID'] = str(uuid.uuid4())
# Create scenario folder
up.ensure_dir(Path_Result)
# Copy script and Config file into that folder
shutil.copy(ProjectSpecs_Path_Main + 'Calculation\\' + ProjectSpecs_Name_ConFile, Path_Result +ProjectSpecs_Name_ConFile)
shutil.copy(ProjectSpecs_Path_Main + 'Calculation\\' + Name_Script + '.py', Path_Result + Name_Script + '.py')
# Initialize logger    
[Mylog,console_log,file_log] = up.function_logger(logging.DEBUG, Name_Scenario + '_' + TimeString, Path_Result, logging.DEBUG) 
# log header and general information
Mylog.info('<html>\n<head>\n</head>\n<body bgcolor="#ffffff">\n<br>')
Mylog.info('<font "size=+5"><center><b>Script ' + Name_Script + '.py</b></center></font>')
Mylog.info('<font "size=+5"><center><b>Version: ' + version +'.</b></center></font>')
Mylog.info('<font "size=+4"> <b>Current User: ' + ProjectSpecs_User_Name + '.</b></font><br>')
Mylog.info('<font "size=+4"> <b>Current Path: ' + ProjectSpecs_Path_Main + '.</b></font><br>')
Mylog.info('<font "size=+4"> <b>Current Scenario: ' + Name_Scenario + '.</b></font><br>')
Mylog.info(ScriptConfig['Scenario_Description'])
Mylog.info('Unique ID of scenario run: <b>' + ScriptConfig['Current_UUID'] + '</b>')
# Start the timer
Time_Start = time.time()
Mylog.info('<font "size=+4"> <b>Start of simulation: ' + time.asctime() + '.</b></font><br>')
  
#%%



################################
# 5. Step: Import Exiobase 3.6 #
################################ 
# Now, Exiobase 3.6 will be imported. This includes the following:
    # L-Matrix containing the Leontief-Inverse
    # S-Matrix containing the emissions
    # Y-Matrix containing the final demands
    # FDE-Matrix containing the direct emissions caused by the final demands
Mylog.info('<p>Loading Exiobase 3.6 data. <br>')
MRIO_Name = ScriptConfig['DataBase'] + '_' + ScriptConfig['Layer'] + '_' + ScriptConfig['Regions']
if  MRIO_Name == 'EXIOBASE3_13_Mon_49R':
    Mylog.info('<p><b>Loading '+ MRIO_Name +' model from hard disc.</b><br>')
    Filestring_Matlab_in      = ProjectSpecs_DataPath1  + MRIO_Name + '_' + ScriptConfig['Datestamp'] + '_' + ScriptConfig['Construct'] + '.mat' 
    Mylog.info('Reading '+ MRIO_Name + '_' + ScriptConfig['Datestamp'] + '_' + ScriptConfig['Construct'] + ' model from ' + Filestring_Matlab_in)
    Mylog.info('<p>Import L-Matrix (Leontief-Inverse).<br>')
    MRIO_L = scipy.io.loadmat(Filestring_Matlab_in)['EB3_L_ITC']
    Mylog.info('<p>Import S-Matrix (Emissions).<br>')
    MRIO_S = scipy.io.loadmat(Filestring_Matlab_in)['EB3_S_ITC']
    Mylog.info('<p>Import Y-Matrix (Final Demands).<br>')
    MRIO_Y = scipy.io.loadmat(Filestring_Matlab_in)['EB3_Y']
    Mylog.info('<p>Import FDE-Matrix (Direct Emissions from Final Demand).<br>')
    MRIO_FDE = scipy.io.loadmat(Filestring_Matlab_in)['EB3_FinalDemand_Emissions']
    Mylog.info('<p>Import the Names of Industry Sectors.<br>')
    MRIO_Prod = scipy.io.loadmat(Filestring_Matlab_in)['EB3_ProductNames200']
    Mylog.info('<p>Import the Names of Extension Codes.<br>')
    MRIO_Ext = scipy.io.loadmat(Filestring_Matlab_in)['EB3_Extensions']
    Mylog.info('<p>Import the Names of Regions.<br>')
    MRIO_Reg = scipy.io.loadmat(Filestring_Matlab_in)['EB3_RegionList']
    # Importing numbers of parameters from Exiobase 3.6
    EB3_NoofCountries  = len(MRIO_Reg)
    EB3_NoofProducts = len(MRIO_Prod)
    EB3_NoofFDCategories = len(MRIO_Y)
    EB3_NoofInventories = len(MRIO_Ext)

#%%



############################################
# 6. Step: Import characterisation factors #
############################################
# In order to calculate the environmental footprints, characterisation factors are needed
# to convert the emissions received by S*L*Y to midpoint indicators
Mylog.info('<p>Import characterisation factors to calculate midpoint indicators.<br>')
# Import excel file containing the midpoint indicator characterisation factors.
ImpactFile  = xlrd.open_workbook(ProjectSpecs_DataPath1 + 'Proxies/Characterization_EB36.xlsx')
ImpactSheet = ImpactFile.sheet_by_name('Emissions')
ImpactCategory_Names = []
for m in range(0,36):
    ImpactCategory_Names.append(ImpactSheet.cell_value(0,m))
    
MRIO_Char = np.zeros((36,1707))
for m in range(0,36):
    for n in range(0,1707):
        MRIO_Char[m,n] = ImpactSheet.cell_value(n+1,m+1)
        
#%%
   


##############################################
# 7. Step: Import household expenditure data #
##############################################     
# Now the expenditure data from the consumer survey (CS) conducted by the Federal Statistical
# Office of Germany will be imported. 
Mylog.info('<p><b>Import Excel file containing the consumer survey data.</b><br>')
# First, the excel file is read and the category names as well as the income group classifications are imported.
HH_expenditure_file = xlrd.open_workbook(ProjectSpecs_DataPath1 + 'Proxies/DeStatis_Data2013.xlsx')
HH_expenditure_sheet = HH_expenditure_file.sheet_by_name('hh_expenditure')
HHE_CategoryNames = []
for m in range(0,103):
    HHE_CategoryNames.append(HH_expenditure_sheet.cell_value(m+2,1))
HHE_IncomeGroups = []
for m in range(0,12):
    HHE_IncomeGroups.append(HH_expenditure_sheet.cell_value(1,m+2))
# Then, the expenditure data itself is imported.        
HH_expenditure = np.zeros((103,12))
for m in range(0,103):
    for n in range(0,12):
        HH_expenditure[m,n] = HH_expenditure_sheet.cell_value(m+2,n+2)

# In order to start the MC-Simulation, information about the uncertainty is needed. Luckily, the CS data contains
# an uncertainty value (Relative Standard Error of Mean) which can be used to create a probability distribution.
Mylog.info('<p><b> Import excel file containing the uncertainty for every COICOP category for all income groups. </b><br>')
HH_uncertainty_sheet = HH_expenditure_file.sheet_by_name('hh_uncertainty')
HH_uncertainty = np.zeros((103,12))
for m in range(0,103):
    for n in range(0,12):
        HH_uncertainty[m,n] = HH_uncertainty_sheet.cell_value(m+2,n+2)

Mylog.info('<p><b> Import excel file containing the household numbers for every COICOP category for all income groups. </b><br>')
HH_households_sheet = HH_expenditure_file.sheet_by_name('hh_households')
HH_households = np.zeros((103,12))
for m in range(0,103):
    for n in range(0,12):
        HH_households[m,n] = HH_households_sheet.cell_value(m+2,n+2)

HH_FinalDemand_Avg = np.zeros((9800,12))
HH_FinalDemand_StE = np.zeros((9800,12))

# Lastly, meta data for the expenditure data is defined. 
# Data is taken from DeStatis -"Wirtschaftsrechnungen - Einkommens- und Verbrauchsstichprobe 
# Aufwendung privater Haushalte für den privaten Konsum" (Fachserie 15, Heft 5, 2013)
# Population data is taken from World Bank      
HH_number = [39326,179+2756,4042,2129,2134,3139,5578,6925,6079,4635,1143,587] # Number of households in 1000
ICG_name = ["Avg", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"]
HH_avg_members = [2, 1, 1.2, 1.4, 1.5, 1.5, 1.8, 2.2, 2.6, 3.0, 3.0, 3.0] # Average number of persons per household
Population = 80645605
                 
HH_FD_Ind = np.zeros((200,12))
                 
#%%



#####################################
# 8. Step: Import aggregation table #
#####################################
# In order to aggregate the environmental footprints based on consumed products, producing sectors and regions, aggregation table is needed
# to aggregate the detailed information of emissions into a set of aggregated sections
Mylog.info('<p>Import Excel file containing aggregation table.<br>')
# Excel file created by Gilang Hardadi based on the excel file named
# EB340_Aggregation.xlsx
AggregationFile  = xlrd.open_workbook(ProjectSpecs_DataPath1 + 'Proxies/EB340_Aggregation.xlsx')

# 1) Product aggregation
ProductSheet = AggregationFile.sheet_by_name('Products')
Pv = [] # List of aggregation indices
Pa = [] # Names of aggregated categories
for m in range(0,200):
    Pv.append(int(ProductSheet.cell_value(m+1,1)))
for m in range(0,12):
    Pa.append(ProductSheet.cell_value(m+1,3))

# 2a) Sector aggregation for Carbon Footprint
SectorCFSheet = AggregationFile.sheet_by_name('SectorsCF')
SCv = [] # List of aggregation indices
SCa = [] # Names of aggregated categories
for m in range(0,200):
    SCv.append(int(SectorCFSheet.cell_value(m+1,1)))
for m in range(0,6):
    SCa.append(SectorCFSheet.cell_value(m+1,3))

# 2b) Sector aggregation for Land Footprint
SectorLFSheet = AggregationFile.sheet_by_name('SectorsLF')
SLv = [] # List of aggregation indices
SLa = [] # Names of aggregated categories
for m in range(0,200):
    SLv.append(int(SectorLFSheet.cell_value(m+1,1)))
for m in range(0,6):
    SLa.append(SectorLFSheet.cell_value(m+1,3))

# 2c) Sector aggregation for Material Footprint
SectorMFSheet = AggregationFile.sheet_by_name('SectorsMF')
SMv = [] # List of aggregation indices
SMa = [] # Names of aggregated categories
for m in range(0,200):
    SMv.append(int(SectorMFSheet.cell_value(m+1,1)))
for m in range(0,6):
    SMa.append(SectorMFSheet.cell_value(m+1,3))

# 2d) Sector aggregation for Water Footprint
SectorWFSheet = AggregationFile.sheet_by_name('SectorsWF')
SWv = [] # List of aggregation indices
SWa = [] # Names of aggregated categories
for m in range(0,200):
    SWv.append(int(SectorWFSheet.cell_value(m+1,1)))
for m in range(0,6):
    SWa.append(SectorWFSheet.cell_value(m+1,3))

# 3) Region aggregation
RegionSheet = AggregationFile.sheet_by_name('Regions')
Rv = [] # List of aggregation indices
Ra = [] # Names of aggregated categories
for m in range(0,49):
    Rv.append(int(RegionSheet.cell_value(m+1,1)))
for m in range(0,6):
    Ra.append(RegionSheet.cell_value(m+1,3))

# 3) Region aggregation
ImportSheet = AggregationFile.sheet_by_name('ImportReg')
Iv = [] # List of aggregation indices
Ia = [] # Names of aggregated categories
for m in range(0,49):
    Iv.append(int(ImportSheet.cell_value(m+1,1)))
for m in range(0,2):
    Ia.append(ImportSheet.cell_value(m+1,3))

# 4) Monetary aggregation
MonetarySheet = AggregationFile.sheet_by_name('Monetary')
FP_Monetary = np.zeros((200,6))
HH_Monetary = np.zeros((9800,6))
for m in range(0,200):
    for n in range(0,6):
        FP_Monetary[m,n] = MonetarySheet.cell_value(m+1,n+1)

for m in range(0,200):
    for n in range(0,6):
        for i in range(0,49):
            HH_Monetary[i*200+m,n] = MonetarySheet.cell_value(m+1,n+1)
                
#%%



######################################################
# 9. Step: Preparation of the Monte-Carlo-Simulation #
######################################################       

# Since the CS data and Exciobase use different classification systems it is necessary to redistribute
# the monetary flows from one classification system to the other one (from COICOP to ISIC).
# This is done via a correspondence matrix that defines from which COICOP category 
# to which ISIC categories the money has to be redistributed. 
# The correspondence matrix was created in excel. This file will now be imported.
Mylog.info('<p>1. Import correspondence matrix to redistribute monetary flows from CS classification (COICOP) to Exiobase classification (ISIC). <br>')
CorrespondenceMatrix_file = xlrd.open_workbook(ProjectSpecs_DataPath1 + 'Proxies/Concordance_Matrix.xlsx')
CorrespondenceMatrix_sheet = CorrespondenceMatrix_file.sheet_by_name('concordance')

# Use this correspondence matrix as iteration point for obtaining matrix (matrices) transforming 
# expenditures in CES sectors into EXIOBASE sectors.
Mylog.info('<p>2. Set correspondence matrix as iteration matrix. <br>')
CorrespondenceMatrixInitial = np.zeros((200,103))
for m in range(0,200):
    for n in range(0,103):
        CorrespondenceMatrixInitial[m,n] = CorrespondenceMatrix_sheet.cell_value(m+4,n+3)       

EVS_Prod = np.empty(103, dtype=object)
for n in range(0,103):
    EVS_Prod[n] = CorrespondenceMatrix_sheet.cell_value(1,n+3)       

IterationMatrix = np.copy(CorrespondenceMatrixInitial)

# Collect data from System of National Account (SNA) to obtain the tax and margin
# rates to convert expenses from purchaser's to basic price.
Mylog.info('<p>3. Collect System of National Account (SNA) data. <br>')
FD_DE_pp = np.zeros((200))
FD_DE_tax = np.zeros((200))
FD_DE_mar = np.zeros((200))
for m in range(0,200):
    FD_DE_pp[m] = CorrespondenceMatrix_sheet.cell_value(m+4,106)
    FD_DE_tax[m] = CorrespondenceMatrix_sheet.cell_value(m+4,107)
    FD_DE_mar[m] = CorrespondenceMatrix_sheet.cell_value(m+4,108)

FD_DE_sum = FD_DE_pp.sum(axis=0)

Tax_Rate = np.zeros((200))
FD_DE_ft = np.zeros((200))
Mar_Rate = np.zeros((200))

for m in range(0,200):
    if FD_DE_pp[m] == 0:
        Tax_Rate[m] = 0
        FD_DE_ft[m] = 0
        Mar_Rate[m] = 0
    else:
        Tax_Rate[m] = FD_DE_tax[m] / FD_DE_pp[m]
        FD_DE_ft[m] = FD_DE_pp[m] - FD_DE_tax[m]
        if FD_DE_mar[m] < 0:
            Mar_Rate[m] = 0
        else:
            Mar_Rate[m] = FD_DE_mar[m] / FD_DE_ft[m]
        
HH_DE_avg = np.zeros((103))
HH_DE_std = np.zeros((103))
HH_DE_hhn = np.zeros((103))
HH_DE_sna = np.zeros((103))

for m in range(0,103):
    HH_DE_avg[m] = HH_expenditure_sheet.cell_value(m+2,2)
    HH_DE_std[m] = HH_uncertainty_sheet.cell_value(m+2,2)
    HH_DE_hhn[m] = HH_households_sheet.cell_value(m+2,2)
    HH_DE_sna[m] = CorrespondenceMatrix_sheet.cell_value(3,m+3)
    HH_DE_sna[m] = HH_DE_sna[m]/80.645605*2

# Demand proportion from each dataset (R and S vector) is calculated.
# Definition of R and S vector in detail is available in Supplementary Information.
Mylog.info('<p>4. Calculate demand proportion of CES (R vector) and EXIOBASE (S vector) data. <br>')
HH_DE_series = np.zeros((iterations, 103))
CorrespondenceMatrix_series = np.zeros((iterations, 200, 103))

# Set RAS function to obtain matrix (matrices) transforming expenditure in CES into EXIOBASE sectors.
Mylog.info('<p>5. Set RAS function. <br>')                       
def RAS(i,j,k):
    IterationMatrix0 = i
    HH_DE = j
    FD_DE = k
    IterationMatrix1 = IterationMatrix0.dot(np.diag(np.nan_to_num(HH_DE/(IterationMatrix0.sum(axis=0)))))
    counter = 0
    while True:
        IterationMatrix2 = np.diag(np.nan_to_num(FD_DE/(IterationMatrix1.sum(axis=1)))).dot(IterationMatrix1)
        IterationMatrix1 = IterationMatrix2.dot(np.diag(np.nan_to_num(HH_DE/(IterationMatrix2.sum(axis=0)))))
        counter = counter + 1
        if counter == 250:
            return IterationMatrix1
                
# Calculate correspondence matrices for each iteration, transforming expenditure in 103 CES sectors
# into 200 EXIOBASE sectors.
Mylog.info('<p>6. Calculate correspondence matrices for each iteration. <br>')

lower = HH_DE_avg
upper = np.zeros((103))
for i in range(0,103):
    if HH_DE_avg[i] + 1.96 * HH_DE_std[i] * HH_DE_avg[i] > HH_DE_sna[i]:
        upper[i] = HH_DE_avg[i] + 1.96 * HH_DE_std[i] * HH_DE_avg[i]
    else:
        upper[i] = HH_DE_sna[i]

a = (lower-HH_DE_avg)/(HH_DE_std * HH_DE_avg * np.sqrt(HH_DE_hhn))
b = (upper-HH_DE_avg)/(HH_DE_std * HH_DE_avg * np.sqrt(HH_DE_hhn))

for i in range(0, iterations):
    print(i)
    np.random.seed(i)
    HH_DE_series[i,:] = truncnorm.rvs(a, b, loc = HH_DE_avg, scale = HH_DE_std * HH_DE_avg * np.sqrt(HH_DE_hhn))
    CorrespondenceMatrix_series[i,:,:] = RAS(IterationMatrix, HH_DE_series[i,:], FD_DE_pp/80645.605)
    for m in range(0,200):
        for n in range(0,103):
            CorrespondenceMatrix_series[i,m,n] = np.nan_to_num(CorrespondenceMatrix_series[i,m,n]/HH_DE_series[i,n])

# In order to be able to redistribute the money, one has to know the actual shares of the Exiobase values, i.e. the share of
# an industry from a specific country in the industrial total of all 49 countries. This has to be done, because the correspondence matrix
# redistributes the money to one industry value instead of 49 times the same industry. The first step to calculate the shares though is 
# the calculation of the industry sum, that sums up all values of an industry over all 49 countries. This is done for all 200 industries.       
Mylog.info('<p>6. Calculate industry specific worldwide sum. <br>')
MRIO_Y_IndustrialTotal = np.zeros(200)
MRIO_Y_Temp = np.zeros((200,49))
counter = 0
for m in range(0,49):
    for n in range(0,200):
        MRIO_Y_Temp[n,m] = MRIO_Y[counter,35]
        counter = counter + 1
MRIO_Y_IndustrialTotal = np.sum(MRIO_Y_Temp,axis=1)

# Now that the industry sums are calculated, it is possible to calculate the share each country has in the total sum by dividing the
# Exiobase value by the industrial total.
Mylog.info('<p>7. Calculate the share of every country-specific industry in the worldwide industrial total. <br>')
MRIO_Y_IndustrialShares = np.zeros(9800)
counter = 0
for m in range(0,49):
    for n in range(0,200):
        if MRIO_Y_IndustrialTotal[n] == 0:
            MRIO_Y_IndustrialShares[counter] == 0
        else:
            MRIO_Y_IndustrialShares[counter] = MRIO_Y[counter,35]/MRIO_Y_IndustrialTotal[n]
        counter = counter + 1

# To check if the calculation has been done correctly, a check for correctness will be done. This will be done by summing up all
# previous calculated shares. There are 200 industries, but some of them have no entry, so there shares add up to be zero. All
# the other 135 industries should have a share sum of exactly 1 (equals 100%), therefore the value of this check for correctness 
# should add up to be 135.
# NOTE: This could also be done by creating a vector containing the share some of all industries. This would probably make more sense
#       since one could see which industry would not add up to 100%. Additionally, the script could finish right here, when there is a 
#       mistake in the calculations.
Mylog.info('<p>8. Check whether or not the calculations have been done correctly.<br>')
CorrectnessCheck = np.sum(MRIO_Y_IndustrialShares, axis=0) 

# Calculate correspondence matrices for each iteration, transforming expenditure in 115 CES sectors
# into 200 EXIOBASE sectors.
Mylog.info('<p>9. Calculate correspondence matrices for each iteration. <br>')

CorrespondenceMatrix = RAS(IterationMatrix,HH_DE_avg,FD_DE_pp/80.645605*2)
MatrixSum = CorrespondenceMatrix.sum(axis=0)
for m in range(0,200):
    for n in range(0,103):
        CorrespondenceMatrix[m,n] = np.nan_to_num(CorrespondenceMatrix[m,n]/MatrixSum[n])

Exp_pp = np.zeros((200,103,12))
Exp_bp = np.zeros((200,103,12))
for i in range(0,12):
    Exp_pp[:,:,i] = CorrespondenceMatrix.dot(np.diag(HH_expenditure[:,i]))
    for m in range(0,200):
        for n in range(0,103):
            Exp_bp[m,n,i] = (1-Mar_Rate[m]) * (1-Tax_Rate[m]) * Exp_pp[m,n,i]
        Exp_bp[146,n,i] = 1.03 * Exp_bp[146,n,i]
        Exp_bp[151,n,i] = 1.03 * Exp_bp[151,n,i]
        Exp_bp[152,n,i] = 1.17 * Exp_bp[152,n,i]
        Exp_bp[153,n,i] = 0.68 * (((Mar_Rate[0:124] * (1-Tax_Rate[0:124])).dot(Exp_pp[0:124,n,i])) - 0.03*Exp_bp[146,n,i] - 0.03*Exp_bp[151,n,i] - 0.17*Exp_bp[152,n,i])
        Exp_bp[154,n,i] = 0.32 * (((Mar_Rate[0:124] * (1-Tax_Rate[0:124])).dot(Exp_pp[0:124,n,i])) - 0.03*Exp_bp[146,n,i] - 0.03*Exp_bp[151,n,i] - 0.17*Exp_bp[152,n,i])

FD_Share = np.zeros((49,200))
FD_DE = MRIO_Y[:,35].reshape(49,200)
for i in range(49):
    for j in range(0,200):
        FD_Prod = FD_DE[:,j].sum(axis=0)
        if FD_Prod != 0:
            FD_Share[i,j] = FD_DE[i,j]/FD_Prod 
FD_Share = FD_Share.reshape(9800)

Exp_bp_ByCountry = np.zeros((9800,103))
Expenditure = np.zeros((9800,103,12))
for i in range(0,12):
    for m in range(0,103):
        HH_Demand_Product = Exp_bp[:,m,i]
        counter = 0
        for n in range(0,49):
            for o in range(0,200):
                Exp_bp_ByCountry[counter,m] = HH_Demand_Product[o] * FD_Share[counter]
                counter = counter+1
        Expenditure[:,m,i] = Exp_bp_ByCountry[:,m]

Mylog.info('<p>Preparation for MC-Simulation finished. <br>')

#%%
   


######################################################################
# 10. Step: Calculating footprints without underreporting correction #
######################################################################    
# To compare the changes in new method (Correcting underreporting in truncated
# normal model), footprints are initially calculated without correction.
Mylog.info('<p><b>Calculating footprints without underreporting correction.</b><br>')

CF_EVS = np.zeros((103,12))
LF_EVS = np.zeros((103,12))
MF_EVS = np.zeros((103,12))
WF_EVS = np.zeros((103,12))

MRIO_Mult = MRIO_Char.dot(MRIO_S.dot(MRIO_L))

for m in range(0,103):
    for n in range(0,12):
        CF_EVS[m,n] = MRIO_Mult[4].dot(Expenditure[:,m,n])/(HH_avg_members[n]*1e6)
        LF_EVS[m,n] = MRIO_Mult[8].dot(Expenditure[:,m,n])/HH_avg_members[n]
        MF_EVS[m,n] = MRIO_Mult[22].dot(Expenditure[:,m,n])/HH_avg_members[n]
        WF_EVS[m,n] = MRIO_Mult[31].dot(Expenditure[:,m,n])/HH_avg_members[n]

CF_EXIO = np.zeros((200,12))
LF_EXIO = np.zeros((200,12))
MF_EXIO = np.zeros((200,12))
WF_EXIO = np.zeros((200,12))

for n in range(0,12):
    CF_EXIO[:,n] = (MRIO_Mult[4].dot(np.diag(Expenditure[:,:,n].sum(axis=1)))/(HH_avg_members[n]*1e6)).reshape(49,200).sum(axis=0)
    LF_EXIO[:,n] = (MRIO_Mult[8].dot(np.diag(Expenditure[:,:,n].sum(axis=1)))/HH_avg_members[n]).reshape(49,200).sum(axis=0)
    MF_EXIO[:,n] = (MRIO_Mult[22].dot(np.diag(Expenditure[:,:,n].sum(axis=1)))/HH_avg_members[n]).reshape(49,200).sum(axis=0)
    WF_EXIO[:,n] = (MRIO_Mult[31].dot(np.diag(Expenditure[:,:,n].sum(axis=1)))/HH_avg_members[n]).reshape(49,200).sum(axis=0)

icg_share = np.zeros((8,12))
for i in range(0,12):
    icg_share[0,i] = HH_expenditure[33,i]*HH_avg_members[0]/(HH_avg_members[i]*HH_DE_sna[33])
    icg_share[1,i] = HH_expenditure[32,i]*HH_avg_members[0]/(HH_avg_members[i]*HH_DE_sna[32])
    icg_share[2,i] = HH_expenditure[31,i]*HH_avg_members[0]/(HH_avg_members[i]*HH_DE_sna[31])
    icg_share[3,i] = icg_share[0,i]*0.03 + icg_share[1,i]*0.44 + icg_share[2,i]*0.53
    icg_share[4,i] = HH_expenditure[58,i]*HH_avg_members[0]/(HH_avg_members[i]*HH_DE_sna[58])
    icg_share[5,i] = icg_share[3,i]*0.54 + icg_share[4,i]*0.46
    icg_share[6,i] = np.sum(HH_expenditure[22:27,i])*HH_avg_members[0]/(HH_avg_members[i]*np.sum(HH_DE_sna[22:27]))
    icg_share[7,i] = HH_expenditure[29,i]*HH_avg_members[0]/(HH_avg_members[i]*HH_DE_sna[29])

Mylog.info('<p><b>Save environmental footprint. </b><br>')
# By product type
#
Result_workbook  = xlsxwriter.Workbook(Path_Result + 'Environmental Footprint.xlsx') 
bold = Result_workbook.add_format({'bold': True})
#
# Footprints per household
#
Mylog.info('<p>Total footprint per capita, by COICOP product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Average, by COICOP') 
Result_worksheet.write(0, 0, 'Product Type', bold)
Result_worksheet.write(0, 1, 'CF, Unit: kg CO2eq', bold)
Result_worksheet.write(0, 2, 'LF, Unit: m2', bold)
Result_worksheet.write(0, 3, 'MF, Unit: kg', bold)
Result_worksheet.write(0, 4, 'WF, Unit: m3', bold)
Result_worksheet.write(104,0, 'Total Indirect', bold)
Result_worksheet.write(105,0, 'Direct Housing', bold)
Result_worksheet.write(106,0, 'Direct Transport', bold)
Result_worksheet.write(107,0, 'Total', bold)
for m in range(0,103):    
    Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
    Result_worksheet.write(m+1, 1, CF_EVS[m,0])    
    Result_worksheet.write(m+1, 2, LF_EVS[m,0])
    Result_worksheet.write(m+1, 3, MF_EVS[m,0])
    Result_worksheet.write(m+1, 4, WF_EVS[m,0])

Result_worksheet.write(104, 1, CF_EVS[:,0].sum(axis=0))
Result_worksheet.write(104, 2, LF_EVS[:,0].sum(axis=0))
Result_worksheet.write(104, 3, MF_EVS[:,0].sum(axis=0))
Result_worksheet.write(104, 4, WF_EVS[:,0].sum(axis=0))

Result_worksheet.write(105, 1, icg_share[3,0]*MRIO_Char[4,:].dot(MRIO_FDE[:,35])/Population)
Result_worksheet.write(105, 2, icg_share[6,0]*MRIO_Char[8,:].dot(MRIO_FDE[:,35])/Population*1E6)
Result_worksheet.write(105, 3, 0)
Result_worksheet.write(105, 4, icg_share[7,0]*MRIO_Char[31,:].dot(MRIO_FDE[:,35])/Population*1E6)

Result_worksheet.write(106, 1, icg_share[4,0]*MRIO_Char[4,:].dot(MRIO_FDE[:,35])/Population)
Result_worksheet.write(106, 2, 0)
Result_worksheet.write(106, 3, 0)
Result_worksheet.write(106, 4, 0)

Result_worksheet.write(107, 1, CF_EVS[:,0].sum(axis=0) + icg_share[5,0]*MRIO_Char[4,:].dot(MRIO_FDE[:,35])/Population)
Result_worksheet.write(107, 2, LF_EVS[:,0].sum(axis=0) + icg_share[6,0]*MRIO_Char[8,:].dot(MRIO_FDE[:,35])/Population*1E6)
Result_worksheet.write(107, 3, MF_EVS[:,0].sum(axis=0))
Result_worksheet.write(107, 4, WF_EVS[:,0].sum(axis=0) + icg_share[7,0]*MRIO_Char[31,:].dot(MRIO_FDE[:,35])/Population*1E6)

Mylog.info('<p>Total footprint per capita, by EXIOBASE product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Footprint, by EXIOBASE') 
Result_worksheet.write(0, 0, 'Product Type', bold)
Result_worksheet.write(0, 1, 'CF, Unit: kg CO2eq', bold)
Result_worksheet.write(0, 2, 'LF, Unit: m2', bold)
Result_worksheet.write(0, 3, 'MF, Unit: kg', bold)
Result_worksheet.write(0, 4, 'WF, Unit: m3', bold)
Result_worksheet.write(201,0, 'Total Indirect', bold)
Result_worksheet.write(202,0, 'Direct Housing', bold)
Result_worksheet.write(203,0, 'Direct Transport', bold)
Result_worksheet.write(204,0, 'Total', bold)
for m in range(0,200):    
    Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
    Result_worksheet.write(m+1, 1, CF_EXIO[m,0])    
    Result_worksheet.write(m+1, 2, LF_EXIO[m,0])
    Result_worksheet.write(m+1, 3, MF_EXIO[m,0])
    Result_worksheet.write(m+1, 4, WF_EXIO[m,0])

Result_worksheet.write(201, 1, CF_EXIO[:,0].sum(axis=0))
Result_worksheet.write(201, 2, LF_EXIO[:,0].sum(axis=0))
Result_worksheet.write(201, 3, MF_EXIO[:,0].sum(axis=0))
Result_worksheet.write(201, 4, WF_EXIO[:,0].sum(axis=0))

Result_worksheet.write(202, 1, icg_share[3,0]*MRIO_Char[4,:].dot(MRIO_FDE[:,35])/Population)
Result_worksheet.write(202, 2, icg_share[6,0]*MRIO_Char[8,:].dot(MRIO_FDE[:,35])/Population*1E6)
Result_worksheet.write(202, 3, 0)
Result_worksheet.write(202, 4, icg_share[7,0]*MRIO_Char[31,:].dot(MRIO_FDE[:,35])/Population*1E6)

Result_worksheet.write(203, 1, icg_share[4,0]*MRIO_Char[4,:].dot(MRIO_FDE[:,35])/Population)
Result_worksheet.write(203, 2, 0)
Result_worksheet.write(203, 3, 0)
Result_worksheet.write(203, 4, 0)

Result_worksheet.write(204, 1, CF_EXIO[:,0].sum(axis=0) + icg_share[5,0]*MRIO_Char[4,:].dot(MRIO_FDE[:,35])/Population)
Result_worksheet.write(204, 2, LF_EXIO[:,0].sum(axis=0) + icg_share[6,0]*MRIO_Char[8,:].dot(MRIO_FDE[:,35])/Population*1E6)
Result_worksheet.write(204, 3, MF_EXIO[:,0].sum(axis=0))
Result_worksheet.write(204, 4, WF_EXIO[:,0].sum(axis=0) + icg_share[7,0]*MRIO_Char[31,:].dot(MRIO_FDE[:,35])/Population*1E6)

Result_worksheet = Result_workbook.add_worksheet('Income Group') 
Result_worksheet.write(0, 0, 'Income Group', bold)
Result_worksheet.write(0, 1, 'CF, Unit: kg CO2eq', bold)
Result_worksheet.write(0, 2, 'LF, Unit: m2', bold)
Result_worksheet.write(0, 3, 'MF, Unit: kg', bold)
Result_worksheet.write(0, 4, 'WF, Unit: m3', bold)
for m in range(0,12):    
    Result_worksheet.write(m+1, 0, HHE_IncomeGroups[0+m])
    Result_worksheet.write(m+1, 1, CF_EVS[:,m].sum(axis=0)+icg_share[5,m]*MRIO_Char[4,:].dot(MRIO_FDE[:,35])/Population)    
    Result_worksheet.write(m+1, 2, LF_EVS[:,m].sum(axis=0)+icg_share[6,m]*MRIO_Char[8,:].dot(MRIO_FDE[:,35])/Population*1E6)
    Result_worksheet.write(m+1, 3, MF_EVS[:,m].sum(axis=0))
    Result_worksheet.write(m+1, 4, WF_EVS[:,m].sum(axis=0)+icg_share[7,m]*MRIO_Char[31,:].dot(MRIO_FDE[:,35])/Population*1E6)

Result_workbook.close()

#%%



########################################################################
# 11. Step: Creating final demand vectors using Monte-Carlo-Simulation #
########################################################################
# This is were the core calculation of this script is happening. Within this MC-Simulation random expenditure values are picked
# from a given normal distribution. This normal distribution is created by using the uncertainty information given in the CS data.
# The original value represents the mean of the distribution, while the uncertainty (Relative Standard Error of Mean) is used as 
# standard deviation. 
# For all 105 COICOP categories a random value is picked and the monetary data is redistributed according to the correspondence matrix
# and the previously calculated industrial shares. This procedure is repeated 1,000 times resulting in many different final demand
# vectors that will all be used for a footprint calculation. This way, the uncertainty of the CS data is taken into account.
# Additionally, the overall spending for every final demand vector will be calculated.
# In a last step, two additional final demand vector that represent extreme scenarios, namely the highest and the lowest spending
# scenario are created.
Mylog.info('<p><b>Creation of final demand vectors using Monte-Carlo-Simulation </b><br>')
Mylog.info('<p>1. Pick a random expenditure value from a uniform distribution defined by the CS uncertainty information. <br>')
Mylog.info('<p>2. Distribute the money of the household expenditure data (COICOP) to the Exiobase categories (ISIC). <br>')
Mylog.info('<p>3. Create final demand vector based on the money distribution matrix that converts COICOP (DeStatis) to ISIC (Exiobase). <br>')
Mylog.info('<p>4. Calculate the overall spending in order to calculate the footprint per Euro spent later on. <br>')
Mylog.info('<p>5. Final creation of the final demand vector by distributing the money based on the original percentages in MRIO_Y. <br>')
Mylog.info('<p>6. Create an additional matrix containing the overall spending on each industrial sector worldwide. <br>')
Mylog.info('<p>7. Create extreme scenarios using the highest and the lowest spending on each industry from the ' + str(iterations) +' iterations. <br>')

Mylog.info('<p><b>Preparation of the Monte-Carlo-Simulation.</b><br>')
# Create an empty matrix that will contain all the final demand vectors.    
HH_FinalDemand_MC = np.zeros((iterations,103,9800))
HH_DE_factor = np.zeros((103,12))

for i in range(0,103):
    for j in range(0,12):
        HH_DE_factor[i,j] = HH_expenditure[i,j]/HH_expenditure[i,0]

lower_i = np.zeros((103,12))
upper_i = np.zeros((103,12))

for i in range(0,103):
    for j in range(0,12):
        lower_i[i,j] = HH_expenditure[i,j] #- 1.96 * HH_uncertainty[:,income_group] * HH_expenditure[:,income_group])
        if upper[i] == HH_DE_sna[i]:
            upper_i[i,j] = HH_DE_factor[i,j] * HH_DE_sna[i]
        else:
            upper_i[i,j] = (HH_expenditure[i,j] + 1.96 * HH_uncertainty[i,j] * HH_expenditure[i,j])

a = np.zeros((103,12))
b = np.zeros((103,12))

for i in range(0,103):
    for j in range(0,12):
        a[i,j] = np.nan_to_num((lower_i[i,j]-HH_expenditure[i,j])/(HH_uncertainty[i,j] * HH_expenditure[i,j] * np.sqrt(HH_households[i,j])))
        b[i,j] = np.nan_to_num((upper_i[i,j]-HH_expenditure[i,j])/(HH_uncertainty[i,j] * HH_expenditure[i,j] * np.sqrt(HH_households[i,j])))

# Additionally, create an empty matrix that will contain the total spending on each industry sector
HH_Spending_ConsSurvey = np.zeros((iterations,103))
HH_Spending_EXIOBASEpp = np.zeros((iterations,200))
HH_Spending_EXIOBASEbp = np.zeros((iterations,200))

for i in range(0,iterations):    
    # 1. Create a probability distribution and pick a random value for all 103 COICOP categories.       
    print(i)
    HH_expenditure_MC = np.zeros((103,103))
    for j in range(0,103):
        np.random.seed(i)
        if HH_expenditure[j,income_group] == 0:
        #HH_expenditure_MC[j] = HH_expenditure[j,income_group]
            HH_expenditure_MC[j,j] = 0
        else:
            HH_expenditure_MC[j,j] = truncnorm.rvs(a[j,income_group], b[j,income_group], loc = HH_expenditure[j,income_group], scale = HH_uncertainty[j,income_group] * HH_expenditure[j,income_group] * np.sqrt(HH_households[j,income_group]))
    # 2. Redistribute the money by multiplying the monetary flow from the expenditure data by the corresponding share 
    #    in the correspondence matrix.
    MoneyRedistribution = CorrespondenceMatrix_series[i,:,:].dot(HH_expenditure_MC)
    HH_Spending_ConsSurvey[i,:] = HH_expenditure_MC.sum(axis=0)
    HH_Spending_EXIOBASEpp[i,:] = MoneyRedistribution.sum(axis=1)
    HH_Demand_Industry = np.zeros((MoneyRedistribution.shape))
    
    # 3. Create a vector containing the row sum of the money redistribution matrix. Thus, this vector represents the final demand
    #    for a single industry (ISIC) created by the expenditure of German households.
    for m in range(0,200):
        HH_Demand_Industry[m,:] = (1-Mar_Rate[m]) * (1-Tax_Rate[m]) * MoneyRedistribution[m,:]
    HH_Demand_Industry[146,:] = 1.03 * HH_Demand_Industry[146,:]
    HH_Demand_Industry[151,:] = 1.03 * HH_Demand_Industry[151,:]
    HH_Demand_Industry[152,:] = 1.17 * HH_Demand_Industry[152,:]
    HH_Demand_Industry[153,:] = 0.68 * (((Mar_Rate[0:124] * (1-Tax_Rate[0:124])).dot(MoneyRedistribution[0:124,:])) - 0.03*HH_Demand_Industry[146,:] - 0.03*HH_Demand_Industry[151,:] - 0.17*HH_Demand_Industry[152,:])
    HH_Demand_Industry[154,:] = 0.32 * (((Mar_Rate[0:124] * (1-Tax_Rate[0:124])).dot(MoneyRedistribution[0:124,:])) - 0.03*HH_Demand_Industry[146,:] - 0.03*HH_Demand_Industry[151,:] - 0.17*HH_Demand_Industry[152,:])
    
    # 4. Now, it is checked, whether the original Exiobase value scaled down to one household or the value from the
    #    CS data will be used. 
    HH_Spending_EXIOBASEbp[i,:] = HH_Demand_Industry.sum(axis=1)    
    HH_Demand_Industry_ByCountry = np.zeros((9800))
    for m in range(0,103):
        HH_Demand_Product = HH_Demand_Industry[:,m]
        counter = 0
        for n in range(0,49):
            for o in range(0,200):
                HH_Demand_Industry_ByCountry[counter] = HH_Demand_Product[o] * MRIO_Y_IndustrialShares[counter]
                counter = counter+1
        HH_FinalDemand_MC[i,m,:] = HH_Demand_Industry_ByCountry

Mylog.info('<p>End of Monte-Carlo-Simulation. <br>')

# 8. Calculate the mean and standard deviation of final demand vectors
HH_FinalDemand_Avg[:,income_group] = np.mean(HH_FinalDemand_MC.sum(axis=1),axis=0)
HH_FinalDemand_StE[:,income_group] = np.std(HH_FinalDemand_MC.sum(axis=1),axis=0)

Mylog.info('<p>1. Calculate average percental differences of every income group compared to the average.<br>')

for n in range(0,200):
    HH_FD_Ind[n,income_group] = HH_Demand_Industry[n,:].sum(axis=0) 

# At first, the perccental difference between a specific income group and the average value will be calculated
# After this, all the percentages will be averaged to receive the average percental difference between
# the spending of a specific income group and the average spending of all households.

HOc_percentage = np.zeros((iterations,12))
for i in range(0,iterations):
    avg_exp = HH_DE_sna[33]
    for n in range(0,12):
        np.random.seed(i)
        icg_exp = truncnorm.rvs(a[33,n], b[33,n], loc = HH_expenditure[33,n], scale = HH_uncertainty[33,n] * HH_expenditure[33,n] * np.sqrt(HH_households[33,n]))
        HOc_percentage[i,n] = icg_exp/avg_exp
        
HOo_percentage = np.zeros((iterations,12))
for i in range(0,iterations):
    avg_exp = HH_DE_sna[32]
    for n in range(0,12):
        np.random.seed(i)
        icg_exp = truncnorm.rvs(a[32,n], b[32,n], loc = HH_expenditure[32,n], scale = HH_uncertainty[32,n] * HH_expenditure[32,n] * np.sqrt(HH_households[32,n]))
        HOo_percentage[i,n] = icg_exp/avg_exp

HOg_percentage = np.zeros((iterations,12))
for i in range(0,iterations):
    np.random.seed(i)
    avg_exp = HH_DE_sna[31]
    for n in range(0,12):
        np.random.seed(i)
        icg_exp = truncnorm.rvs(a[31,n], b[31,n], loc = HH_expenditure[31,n], scale = HH_uncertainty[31,n] * HH_expenditure[31,n] * np.sqrt(HH_households[31,n]))
        HOg_percentage[i,n] = icg_exp/avg_exp
        
TR_percentage = np.zeros((iterations,12))
for i in range(0,iterations):
    np.random.seed(i)
    avg_exp = HH_DE_sna[58]
    for n in range(0,12):
        np.random.seed(i)
        icg_exp = truncnorm.rvs(a[58,n], b[58,n], loc = HH_expenditure[58,n], scale = HH_uncertainty[58,n] * HH_expenditure[58,n] * np.sqrt(HH_households[58,n]))
        TR_percentage[i,n] = icg_exp/avg_exp

LF_percentage = np.zeros((iterations,12))
for i in range(0,iterations):
    avg_exp = np.sum(HH_DE_sna[22:27])
    for n in range(0,12):
        np.random.seed(i)
        icg_exp = np.sum(truncnorm.rvs(a[22:27,n], b[22:27,n], loc = HH_expenditure[22:27,n], scale = HH_uncertainty[22:27,n] * HH_expenditure[22:27,n] * np.sqrt(HH_households[22:27,n])))
        LF_percentage[i,n] = icg_exp/avg_exp

WF_percentage = np.zeros((iterations,12))
for i in range(0,iterations):
    avg_exp = HH_DE_sna[29]
    for n in range(0,12):
        np.random.seed(i)
        icg_exp = truncnorm.rvs(a[29,n], b[29,n], loc = HH_expenditure[29,n], scale = HH_uncertainty[29,n] * HH_expenditure[29,n] * np.sqrt(HH_households[29,n]))
        WF_percentage[i,n] = icg_exp/avg_exp

# The calculated percental differences will be used to calculate the direct emissions for all household groups.
# This will be done by taking the given vector for German households from the FDE-Matrix and multiply it with 
# the corresponding income group percentages.
# Since the FDE-Matrix is given for all households in Germany, the values have to be divided by the population
# to get the direct emissions of one household.
# 0.54 and 0.46 represents direct emissions from housing and transportation, respectively.
# 0.03, 0.44, and 0.53 represents direct housing emissions from coal/wood, heating oil, and gas, respectively.
Mylog.info('<p>2. Create vectors representing direct emissions or resources use for analyzed income group. <br>')   
Mylog.info('<p>Use percental differences to scale the average vector to every income group and divide them by the population. <br>')
MRIO_DirectCarbon = np.zeros((iterations,1707))
MRIO_DirectCarbonHO = np.zeros((iterations,1707))
MRIO_DirectCarbonHOc = np.zeros((iterations,1707))
MRIO_DirectCarbonHOo = np.zeros((iterations,1707))
MRIO_DirectCarbonHOg = np.zeros((iterations,1707))
MRIO_DirectCarbonTR = np.zeros((iterations,1707))
MRIO_DirectLand = np.zeros((iterations,1707))
MRIO_DirectMaterial = np.zeros((iterations,1707))
MRIO_DirectWater = np.zeros((iterations,1707))
for i in range(0,iterations):
    for m in range(0,1707):
        MRIO_DirectCarbon[i,m] = (MRIO_FDE[m,35]*(HOc_percentage[i,income_group]*0.54*0.03+HOo_percentage[i,income_group]*0.54*0.44+HOg_percentage[i,income_group]*0.54*0.53+TR_percentage[i,income_group]*0.46))*HH_avg_members[0]/Population
        MRIO_DirectCarbonHO[i,m] = (MRIO_FDE[m,35]*(HOc_percentage[i,income_group]*0.54*0.03+HOo_percentage[i,income_group]*0.54*0.44+HOg_percentage[i,income_group]*0.54*0.53))*HH_avg_members[0]/Population
        MRIO_DirectCarbonHOc[i,m] = (MRIO_FDE[m,35]*(HOc_percentage[i,income_group]*0.54*0.03))*HH_avg_members[0]/Population
        MRIO_DirectCarbonHOo[i,m] = (MRIO_FDE[m,35]*(HOo_percentage[i,income_group]*0.54*0.44))*HH_avg_members[0]/Population
        MRIO_DirectCarbonHOg[i,m] = (MRIO_FDE[m,35]*(HOg_percentage[i,income_group]*0.54*0.53))*HH_avg_members[0]/Population
        MRIO_DirectCarbonTR[i,m] = (MRIO_FDE[m,35]*(TR_percentage[i,income_group])*0.46)*HH_avg_members[0]/Population
        MRIO_DirectLand[i,m] = (MRIO_FDE[m,35]*(LF_percentage[i,income_group]))*HH_avg_members[0]/Population
        MRIO_DirectWater[i,m] = (MRIO_FDE[m,35]*(WF_percentage[i,income_group]))*HH_avg_members[0]/Population
                          
#%%



#########################################
# 12. Step: Export final demand matrix  #
#########################################
# Since the final demand matrix will be used to analyze not only the overall spending but also on which products the money 
# has been spend on the matrix has to be exported to an excel file.
Mylog.info('<p>Save matrix containing the overall spending on each industry as excel file. <br>')       
Result_workbook  = xlsxwriter.Workbook(Path_Result + 'FinalDemand_' + HHE_IncomeGroups[income_group] + '.xlsx') 
bold = Result_workbook.add_format({'bold': True})
Result_worksheet = Result_workbook.add_worksheet('Final Demand Spending (Euro)') 
Result_worksheet.write(0, 0, 'Product groups, Consumer Survey, COICOP', bold)
Result_worksheet.write(104,0, 'Total', bold)
for m in range(0,103):
        Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
        for n in range(0,iterations):
            Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
            Result_worksheet.write(m+1,n+1, HH_Spending_ConsSurvey[n,m])
            Result_worksheet.write(104,n+1, HH_Spending_ConsSurvey[n,:].sum(axis=0))

Result_worksheet = Result_workbook.add_worksheet('By EXIOBASE Sector (pp)') 
Result_worksheet.write(0, 0, 'Product groups, EXIOBASE, ISIC', bold)
Result_worksheet.write(201,0, 'Total', bold)
for m in range(0,200):
        Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
        for n in range(0,iterations):
            Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
            Result_worksheet.write(m+1,n+1, HH_Spending_EXIOBASEpp[n,m])
            Result_worksheet.write(201,n+1, HH_Spending_EXIOBASEpp[n,:].sum(axis=0))

Result_worksheet = Result_workbook.add_worksheet('By EXIOBASE Sector (bp)') 
Result_worksheet.write(0, 0, 'Product groups, EXIOBASE, ISIC', bold)
Result_worksheet.write(201,0, 'Total', bold)
for m in range(0,200):
        Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
        for n in range(0,iterations):
            Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
            Result_worksheet.write(m+1,n+1, HH_Spending_EXIOBASEbp[n,m])
            Result_worksheet.write(201,n+1, HH_Spending_EXIOBASEbp[n,:].sum(axis=0))

Result_worksheet = Result_workbook.add_worksheet('By Country and Sector (Average)') 
Result_worksheet.write(0, 0, 'Product groups, Exiobase, ISIC', bold)
Result_worksheet.write(9801,0, 'Total', bold)
Result_worksheet.write(0, 1, 'Country', bold)
Result_worksheet.write(0, 2, 'Average', bold)
Result_worksheet.write(0, 3, 'Standard Deviation', bold)
for m in range(0,9800):
        Result_worksheet.write(m+1, 0, MRIO_Prod[(0+m)%200])
        Result_worksheet.write(m+1, 1, MRIO_Reg[int((0+m)/200)])
        Result_worksheet.write(m+1, 2, np.average(HH_FinalDemand_Avg[m,income_group]))
        Result_worksheet.write(m+1, 3, np.average(HH_FinalDemand_StE[m,income_group]))

Result_workbook.close()

#%%

##########################################################
# 13. Step: Calculate direct emissions or resources use  #
##########################################################
# Households cause direct emissions of greenhouse gases which are not accounted for, when looking only at the
# upstream emissions. In order to take them into account, the given FDE-Matrix has been used and transformed
# to represent the different income groups (FY-Vector).
# This vector will now be multiplied with the characterisation factors to receive the direct emissions, that 
# are important for the calculations of the overall footprints.
Mylog.info('<p>Calculate direct household emissions or resources use. <br>')  
Scope1_LandFP     = (MRIO_DirectLand * MRIO_Char[8,:]).sum(axis=1)
Scope1_CarbonFP   = (MRIO_DirectCarbon * MRIO_Char[4,:]).sum(axis=1)
Scope1_HOCarbonFP = (MRIO_DirectCarbonHO * MRIO_Char[4,:]).sum(axis=1)
Scope1_HOcCarbonFP = (MRIO_DirectCarbonHOc * MRIO_Char[4,:]).sum(axis=1)
Scope1_HOoCarbonFP = (MRIO_DirectCarbonHOo * MRIO_Char[4,:]).sum(axis=1)
Scope1_HOgCarbonFP = (MRIO_DirectCarbonHOg * MRIO_Char[4,:]).sum(axis=1)
Scope1_TRCarbonFP = (MRIO_DirectCarbonTR * MRIO_Char[4,:]).sum(axis=1)
Scope1_WaterFP    = (MRIO_DirectWater * MRIO_Char[31,:]).sum(axis=1)
#%%



###################################
# 13. Step: Footprint calculation #
###################################
# The calculation is done by using the following formula:
    # Footprint = C * S * L * Y
    # C = Characterisation factor matrix
    # S = Emission matrix
    # L = Leontief-Inverse
    # Y = Final demand vector.
# NOTE: In order to make the calculations faster, the S and L matrices will be multiplied first
# and the result will be multiplied by Y. 
Mylog.info('<p>Multiply S and L matrix.<br>')
# Multiplying S and L uses the linearity of the model to make the calculations faster.
MRIO_SL = MRIO_S.dot(MRIO_L)
MRIO_Carbon = np.zeros((1707,9800))
MRIO_Land = np.zeros((1707,9800))
MRIO_Material = np.zeros((1707,9800))
MRIO_Water = np.zeros((1707,9800))
for m in range(0,1707):
    for n in range(0,9800):
        MRIO_Carbon[m,n] = MRIO_Char[4,m]*MRIO_SL[m,n]
        MRIO_Land[m,n] = MRIO_Char[8,m]*MRIO_SL[m,n]
        MRIO_Material[m,n] = MRIO_Char[22,m]*MRIO_SL[m,n]
        MRIO_Water[m,n] = MRIO_Char[31,m]*MRIO_SL[m,n]

Mylog.info('<p><b>Start of footprint calculation.</b><br>')
Mylog.info('<p>1. Overall footprint.<br>')
# Create empty matrices that will contain the overall results.
Mylog.info('<p>1.1. Land use footprint.<br>')
Footprint_Land_EXIO = np.zeros((iterations,200))
Footprint_Land_Prod = np.zeros((iterations,103))
Footprint_Land_ProEuro = np.zeros((iterations,103))
Mylog.info('<p>1.2. Carbon footprint.<br>')
Footprint_Carbon_EXIO = np.zeros((iterations,200))
Footprint_Carbon_Prod = np.zeros((iterations,103))
Footprint_Carbon_ProEuro = np.zeros((iterations,200))
Mylog.info('<p>1.3. Material footprint.<br>')
Footprint_Material_EXIO = np.zeros((iterations,200))
Footprint_Material_Prod = np.zeros((iterations,103))
Footprint_Material_ProEuro = np.zeros((iterations,200))
Mylog.info('<p>1.4. Water footprint.<br>')
Footprint_Water_EXIO = np.zeros((iterations,200))
Footprint_Water_Prod = np.zeros((iterations,103))
Footprint_Water_ProEuro = np.zeros((iterations,200))
for m in range (0,iterations):
    print(m)
    for n in range (0,103):
        Footprint_Carbon = MRIO_Carbon.dot(HH_FinalDemand_MC[m,n,:])/1E6
        Footprint_Carbon_Prod[m,n] = Footprint_Carbon.sum(axis=0) # in kg CO2eq
        Footprint_Land = MRIO_Land.dot(HH_FinalDemand_MC[m,n,:])                            
        Footprint_Land_Prod[m,n] = Footprint_Land.sum(axis=0) # in 1000 m²
        Footprint_Material = MRIO_Material.dot(HH_FinalDemand_MC[m,n,:])                            
        Footprint_Material_Prod[m,n] = Footprint_Material.sum(axis=0) # in kg
        Footprint_Water = MRIO_Water.dot(HH_FinalDemand_MC[m,n,:])                           
        Footprint_Water_Prod[m,n] = Footprint_Water.sum(axis=0) # in m³    

#Only for income group 0
for m in range (0,iterations):
    print(m)
    Footprint_Carbon = MRIO_Carbon.dot(np.diag(HH_FinalDemand_MC[m,:,:].sum(axis=0)))/1E6 
    Footprint_Carbon_EXIO[m,:] = Footprint_Carbon.sum(axis=0).reshape(49,200).transpose().sum(axis=1) # in kg CO2eq
    Footprint_Land = MRIO_Land.dot(np.diag(HH_FinalDemand_MC[m,:,:].sum(axis=0)))                            
    Footprint_Land_EXIO[m,:] = Footprint_Land.sum(axis=0).reshape(49,200).transpose().sum(axis=1) # in 1000 m²
    Footprint_Material = MRIO_Material.dot(np.diag(HH_FinalDemand_MC[m,:,:].sum(axis=0)))                            
    Footprint_Material_EXIO[m,:] = Footprint_Material.sum(axis=0).reshape(49,200).transpose().sum(axis=1) # in kg
    Footprint_Water = MRIO_Water.dot(np.diag(HH_FinalDemand_MC[m,:,:].sum(axis=0)))                           
    Footprint_Water_EXIO[m,:] = Footprint_Water.sum(axis=0).reshape(49,200).transpose().sum(axis=1) # in m³    
    
#%%



################################################################
# 14. Step: A) Save results in Excel files: Land use footprint #
################################################################
# In this step, all results will be saved and exported as excel files. The footprints will be calculated
# in three different ways: Per household, per capita and per Euro spent.
# Note: The unit of the final demand vectors is Million Euros (MEUR). In order to calculate the footprints
# per Euro, the footprints have to be divided by the columnsum of the final demand vector and multiplied by
# a million afterwards. Otherwise, one would receive the footprint per MEUR!
    
# Different than 15. Step, this step is conducted based on COICOP classification.
Mylog.info('<p><b>Save land use footprint. </b><br>')
# By product type
#
Result_workbook  = xlsxwriter.Workbook(Path_Result + 'LandFP_ByProduct_' + HHE_IncomeGroups[income_group] + '.xlsx') 
bold = Result_workbook.add_format({'bold': True})
#
# Footprints per household
#
Mylog.info('<p>Total footprint per household, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per HH, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: m²', bold)
Result_worksheet.write(104,0, 'Total Indirect', bold)
Result_worksheet.write(105,0, 'Direct Use', bold)
Result_worksheet.write(106,0, 'Total', bold)
for m in range(0,103):    
    Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Land_Prod[n,m])
for n in range(0,iterations):
    Result_worksheet.write(104,n+1, Footprint_Land_Prod[n,:].sum(axis=0))
    Result_worksheet.write(105,n+1, Scope1_LandFP[n]*1E6)
    Result_worksheet.write(106,n+1, Footprint_Land_Prod[n,:].sum(axis=0) + Scope1_LandFP[n]*1E6)
    
#
# Footprints per capita
#
Mylog.info('<p>Total footprint per capita, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per cap, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: m²', bold)
Result_worksheet.write(104,0, 'Total Indirect', bold)
Result_worksheet.write(105,0, 'Direct Use', bold)
Result_worksheet.write(106,0, 'Total', bold)
for m in range(0,103):    
    Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Land_Prod[n,m]/HH_avg_members[income_group])
for n in range(0,iterations):
    Result_worksheet.write(104,n+1, Footprint_Land_Prod[n,:].sum(axis=0)/HH_avg_members[income_group])
    Result_worksheet.write(105,n+1, Scope1_LandFP[n]*1E6/HH_avg_members[income_group])
    Result_worksheet.write(106,n+1, (Footprint_Land_Prod[n,:].sum(axis=0) + Scope1_LandFP[n]*1E6)/HH_avg_members[income_group])

Result_workbook.close()
#%%



##############################################################
# 14. Step: B) Save results in Excel files: Carbon footprint #
##############################################################
Mylog.info('<p><b>Save carbon footprint. </b><br>')
# By product type
#
Result_workbook  = xlsxwriter.Workbook(Path_Result + 'CarbonFP_ByProduct_' + HHE_IncomeGroups[income_group] + '.xlsx') 
bold = Result_workbook.add_format({'bold': True})
#
# Footprints per household
#
Mylog.info('<p>Total footprint per household, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per HH, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: kg CO2eq', bold)
Result_worksheet.write(104,0, 'Total Indirect', bold)
Result_worksheet.write(105,0, 'Direct Housing', bold)
Result_worksheet.write(106,0, 'Direct Housing, Coal', bold)
Result_worksheet.write(107,0, 'Direct Housing, Oil', bold)
Result_worksheet.write(108,0, 'Direct Housing, Gas', bold)
Result_worksheet.write(109,0, 'Direct Transport', bold)
Result_worksheet.write(110,0, 'Total', bold)
for m in range(0,103):    
    Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Carbon_Prod[n,m])
for n in range(0,iterations):
    Result_worksheet.write(104,n+1, Footprint_Carbon_Prod[n,:].sum(axis=0))       
    Result_worksheet.write(105,n+1, Scope1_HOCarbonFP[n])
    Result_worksheet.write(106,n+1, Scope1_HOcCarbonFP[n])
    Result_worksheet.write(107,n+1, Scope1_HOoCarbonFP[n])
    Result_worksheet.write(108,n+1, Scope1_HOgCarbonFP[n])
    Result_worksheet.write(109,n+1, Scope1_TRCarbonFP[n])
    Result_worksheet.write(110,n+1, Footprint_Carbon_Prod[n,:].sum(axis=0) + Scope1_CarbonFP[n])
#
# Footprints per capita
#
Mylog.info('<p>Total footprint per capita, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per cap, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: kg CO2eq', bold)
Result_worksheet.write(104,0, 'Total Indirect', bold)
Result_worksheet.write(105,0, 'Direct Housing', bold)
Result_worksheet.write(106,0, 'Direct Housing, Coal', bold)
Result_worksheet.write(107,0, 'Direct Housing, Oil', bold)
Result_worksheet.write(108,0, 'Direct Housing, Gas', bold)
Result_worksheet.write(109,0, 'Direct Transport', bold)
Result_worksheet.write(110,0, 'Total', bold)
for m in range(0,103):    
    Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Carbon_Prod[n,m]/HH_avg_members[income_group])
for n in range(0,iterations):
    Result_worksheet.write(104,n+1, Footprint_Carbon_Prod[n,:].sum(axis=0)/HH_avg_members[income_group])
    Result_worksheet.write(105,n+1, Scope1_HOCarbonFP[n]/HH_avg_members[income_group])
    Result_worksheet.write(106,n+1, Scope1_HOcCarbonFP[n]/HH_avg_members[income_group])
    Result_worksheet.write(107,n+1, Scope1_HOoCarbonFP[n]/HH_avg_members[income_group])
    Result_worksheet.write(108,n+1, Scope1_HOgCarbonFP[n]/HH_avg_members[income_group])
    Result_worksheet.write(109,n+1, Scope1_TRCarbonFP[n]/HH_avg_members[income_group])
    Result_worksheet.write(110,n+1, (Footprint_Carbon_Prod[n,:].sum(axis=0) + Scope1_CarbonFP[n])/HH_avg_members[income_group])

Result_workbook.close()
#%%



####################################################################
# 14. Step: C) Save results in Excel files: Material use footprint #
####################################################################
Mylog.info('<p><b>Save Material use footprint. </b><br>')
# By product type
#
Result_workbook  = xlsxwriter.Workbook(Path_Result + 'MaterialFP_ByProduct_' + HHE_IncomeGroups[income_group] + '.xlsx') 
bold = Result_workbook.add_format({'bold': True})
#
# Footprints per household
#
Mylog.info('<p>Total footprint per household, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per HH, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: kg', bold)
Result_worksheet.write(104,0, 'Total', bold)
Result_worksheet.write(0, 1, 'Best case', bold)
Result_worksheet.write(0, 2, 'Worst case', bold)
for m in range(0,103):    
    Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Material_Prod[n,m])
for n in range(0,iterations):
    Result_worksheet.write(104,n+1, Footprint_Material_Prod[n,:].sum(axis=0))
#
# Footprints per capita
#
Mylog.info('<p>Total footprint per capita, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per cap, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: kg', bold)
Result_worksheet.write(104,0, 'Total', bold)
Result_worksheet.write(0, 1, 'Best case', bold)
Result_worksheet.write(0, 2, 'Worst case', bold)
for m in range(0,103):    
    Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Material_Prod[n,m]/HH_avg_members[income_group])
for n in range(0,iterations):
    Result_worksheet.write(104,n+1, Footprint_Material_Prod[n,:].sum(axis=0)/HH_avg_members[income_group])

Result_workbook.close()
#%%

#################################################################
# 14. Step: D) Save results in Excel files: Water use footprint #
#################################################################
Mylog.info('<p><b>Save Water use footprint. </b><br>')
# By product type
#
Result_workbook  = xlsxwriter.Workbook(Path_Result + 'WaterFP_ByProduct_' + HHE_IncomeGroups[income_group] + '.xlsx') 
bold = Result_workbook.add_format({'bold': True})
#
# Footprints per household
#
Mylog.info('<p>Total footprint per household, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per HH, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: m³', bold)
Result_worksheet.write(104,0, 'Total Indirect', bold)
Result_worksheet.write(105,0, 'Direct Use', bold)
Result_worksheet.write(106,0, 'Total', bold)
for m in range(0,103):    
    Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Water_Prod[n,m])
for n in range(0,iterations):
        Result_worksheet.write(104,n+1, Footprint_Water_Prod[n,:].sum(axis=0))
        Result_worksheet.write(105,n+1, Scope1_WaterFP[n]*1E6)
        Result_worksheet.write(106,n+1, Footprint_Water_Prod[n,:].sum(axis=0) + Scope1_WaterFP[n]*1E6)

#
# Footprints per capita
#
Mylog.info('<p>Total footprint per capita, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per cap, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: m³', bold)
Result_worksheet.write(104,0, 'Total Indirect', bold)
Result_worksheet.write(105,0, 'Direct Use', bold)
Result_worksheet.write(106,0, 'Total', bold)
for m in range(0,103):    
    Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Water_Prod[n,m]/HH_avg_members[income_group])
for n in range(0,iterations):
    Result_worksheet.write(104,n+1, Footprint_Water_Prod[n,:].sum(axis=0)/HH_avg_members[income_group])
    Result_worksheet.write(105,n+1, Scope1_WaterFP[n]*1E6/HH_avg_members[income_group])
    Result_worksheet.write(106,n+1, (Footprint_Water_Prod[n,:].sum(axis=0) + Scope1_WaterFP[n]*1E6)/HH_avg_members[income_group])

Result_workbook.close()

#%%



################################################################
# 15. Step: A) Save results in Excel files: Land use footprint #
################################################################
# In this step, all results will be saved and exported as excel files. The footprints will be calculated
# in three different ways: Per household, per capita and per Euro spent.
# Note: The unit of the final demand vectors is Million Euros (MEUR). In order to calculate the footprints
# per Euro, the footprints have to be divided by the columnsum of the final demand vector and multiplied by
# a million afterwards. Otherwise, one would receive the footprint per MEUR!

# Different than 14. Step, this step is conducted based on EXIOBASE classification.
# This step is necessary only for income group 0, to check whether the sum is similar.
Mylog.info('<p><b>Save land use footprint. </b><br>')
# By product type
#
Result_workbook  = xlsxwriter.Workbook(Path_Result + 'LandFP_EXIOBASE' + HHE_IncomeGroups[income_group] + '.xlsx') 
bold = Result_workbook.add_format({'bold': True})
#
# Footprints per household
#
Mylog.info('<p>Total footprint per household, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per HH, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: m²', bold)
Result_worksheet.write(201,0, 'Total Indirect', bold)
Result_worksheet.write(202,0, 'Direct Use', bold)
Result_worksheet.write(203,0, 'Total', bold)
for m in range(0,200):    
    Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Land_EXIO[n,m])
for n in range(0,iterations):
    Result_worksheet.write(201,n+1, Footprint_Land_EXIO[n,:].sum(axis=0))
    Result_worksheet.write(202,n+1, Scope1_LandFP[n]*1E6)
    Result_worksheet.write(203,n+1, Footprint_Land_EXIO[n,:].sum(axis=0) + Scope1_LandFP[n]*1E6)
#
# Footprints per capita
#
Mylog.info('<p>Total footprint per capita, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per cap, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: m²', bold)
Result_worksheet.write(201,0, 'Total Indirect', bold)
Result_worksheet.write(202,0, 'Direct Use', bold)
Result_worksheet.write(203,0, 'Total', bold)
for m in range(0,200):    
    Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Land_EXIO[n,m]/HH_avg_members[income_group])
for n in range(0,iterations):
    Result_worksheet.write(201,n+1, Footprint_Land_EXIO[n,:].sum(axis=0)/HH_avg_members[income_group])
    Result_worksheet.write(202,n+1, Scope1_LandFP[n]*1E6/HH_avg_members[income_group])
    Result_worksheet.write(203,n+1, (Footprint_Land_EXIO[n,:].sum(axis=0) + Scope1_LandFP[n]*1E6)/HH_avg_members[income_group])

Result_workbook.close()
#%%



##############################################################
# 15. Step: B) Save results in Excel files: Carbon footprint #
##############################################################
Mylog.info('<p><b>Save carbon footprint. </b><br>')
# By product type
#
Result_workbook  = xlsxwriter.Workbook(Path_Result + 'CarbonFP_EXIOBASE' + HHE_IncomeGroups[income_group] + '.xlsx') 
bold = Result_workbook.add_format({'bold': True})
#
# Footprints per household
#
Mylog.info('<p>Total footprint per household, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per HH, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: kg CO2eq', bold)
Result_worksheet.write(201,0, 'Total Indirect', bold)
Result_worksheet.write(202,0, 'Direct Housing', bold)
Result_worksheet.write(203,0, 'Direct Transport', bold)
Result_worksheet.write(204,0, 'Total', bold)
for m in range(0,200):    
    Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Carbon_EXIO[n,m])
for n in range(0,iterations):
    Result_worksheet.write(201,n+1, Footprint_Carbon_EXIO[n,:].sum(axis=0))    
    Result_worksheet.write(202,n+1, Scope1_HOCarbonFP[n])
    Result_worksheet.write(203,n+1, Scope1_TRCarbonFP[n])
    Result_worksheet.write(204,n+1, Footprint_Carbon_EXIO[n,:].sum(axis=0) + Scope1_CarbonFP[n])
#
# Footprints per capita
#
Mylog.info('<p>Total footprint per capita, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per cap, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: kg CO2eq', bold)
Result_worksheet.write(201,0, 'Total Indirect', bold)
Result_worksheet.write(202,0, 'Direct Housing', bold)
Result_worksheet.write(203,0, 'Direct Transport', bold)
Result_worksheet.write(204,0, 'Total', bold)
for m in range(0,200):    
    Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Carbon_EXIO[n,m]/HH_avg_members[income_group])
for n in range(0,iterations):
    Result_worksheet.write(201,n+1, Footprint_Carbon_EXIO[n,:].sum(axis=0)/HH_avg_members[income_group])
    Result_worksheet.write(202,n+1, Scope1_HOCarbonFP[n]/HH_avg_members[income_group])
    Result_worksheet.write(203,n+1, Scope1_TRCarbonFP[n]/HH_avg_members[income_group])
    Result_worksheet.write(204,n+1, (Footprint_Carbon_EXIO[n,:].sum(axis=0) + Scope1_CarbonFP[n])/HH_avg_members[income_group])

Result_workbook.close()
#%%



####################################################################
# 15. Step: C) Save results in Excel files: Material use footprint #
####################################################################
Mylog.info('<p><b>Save Material use footprint. </b><br>')
# By product type
#
Result_workbook  = xlsxwriter.Workbook(Path_Result + 'MaterialFP_EXIOBASE' + HHE_IncomeGroups[income_group] + '.xlsx') 
bold = Result_workbook.add_format({'bold': True})
#
# Footprints per household
#
Mylog.info('<p>Total footprint per household, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per HH, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: kg', bold)
Result_worksheet.write(201,0, 'Total', bold)
for m in range(0,200):    
    Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Material_EXIO[n,m])
for n in range(0,iterations):
    Result_worksheet.write(201,n+1, Footprint_Material_EXIO[n,:].sum(axis=0))
#
# Footprints per capita
#
Mylog.info('<p>Total footprint per capita, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per cap, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: kg', bold)
Result_worksheet.write(201,0, 'Total', bold)
for m in range(0,200):    
    Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Material_EXIO[n,m]/HH_avg_members[income_group])
for n in range(0,iterations):
    Result_worksheet.write(201,n+1, Footprint_Material_EXIO[n,:].sum(axis=0)/HH_avg_members[income_group])

Result_workbook.close()
#%%

#################################################################
# 15. Step: D) Save results in Excel files: Water use footprint #
#################################################################
Mylog.info('<p><b>Save Water use footprint. </b><br>')
# By product type
#
Result_workbook  = xlsxwriter.Workbook(Path_Result + 'WaterFP_EXIOBASE' + HHE_IncomeGroups[income_group] + '.xlsx') 
bold = Result_workbook.add_format({'bold': True})
#
# Footprints per household
#
Mylog.info('<p>Total footprint per household, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per HH, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: m³', bold)
Result_worksheet.write(201,0, 'Total Indirect', bold)
Result_worksheet.write(202,0, 'Direct Use', bold)
Result_worksheet.write(203,0, 'Total', bold)
for m in range(0,200):    
    Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Water_EXIO[n,m])
for n in range(0,iterations):
    Result_worksheet.write(201,n+1, Footprint_Water_EXIO[n,:].sum(axis=0))
    Result_worksheet.write(202,n+1, Scope1_WaterFP[n]*1E6)
    Result_worksheet.write(203,n+1, Footprint_Water_EXIO[n,:].sum(axis=0) + Scope1_WaterFP[n]*1E6)

#
# Footprints per capita
#
Mylog.info('<p>Total footprint per capita, by product<br>') 
Result_worksheet = Result_workbook.add_worksheet('Total, per cap, by product') 
Result_worksheet.write(0, 0, 'Product Type, Unit: m³', bold)
Result_worksheet.write(201,0, 'Total Indirect', bold)
Result_worksheet.write(202,0, 'Direct Use', bold)
Result_worksheet.write(203,0, 'Total', bold)
for m in range(0,200):    
    Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
    for n in range(0,iterations):
        Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
        Result_worksheet.write(m+1,n+1, Footprint_Water_EXIO[n,m]/HH_avg_members[income_group])
for n in range(0,iterations):
    Result_worksheet.write(201,n+1, Footprint_Water_EXIO[n,:].sum(axis=0)/HH_avg_members[income_group])
    Result_worksheet.write(202,n+1, Scope1_WaterFP[n]*1E6/HH_avg_members[income_group])
    Result_worksheet.write(203,n+1, (Footprint_Water_EXIO[n,:].sum(axis=0) + Scope1_WaterFP[n]*1E6)/HH_avg_members[income_group])

Result_workbook.close()

#%%

##############################################################################
# 16. Step: Calculate footprint for other income groups and save the results #
##############################################################################
Mylog.info('<p><b>Calculate footprints for other income groups.</b><br>')

for income_group in range(1,12):
    HH_FinalDemand_MC = np.zeros((iterations,103,9800))

    HH_Spending_ConsSurvey = np.zeros((iterations,103))
    HH_Spending_EXIOBASEpp = np.zeros((iterations,200))
    HH_Spending_EXIOBASEbp = np.zeros((iterations,200))

    for i in range(0,iterations):    
        print(i)
        HH_expenditure_MC = np.zeros((103,103))
        for j in range(0,103):
            np.random.seed(i)
            if HH_expenditure[j,income_group] == 0:
                HH_expenditure_MC[j,j] = 0
            else:
                HH_expenditure_MC[j,j] = truncnorm.rvs(a[j,income_group], b[j,income_group], loc = HH_expenditure[j,income_group], scale = HH_uncertainty[j,income_group] * HH_expenditure[j,income_group] * np.sqrt(HH_households[j,income_group]))
        MoneyRedistribution = CorrespondenceMatrix_series[i,:,:].dot(HH_expenditure_MC)
        HH_Spending_ConsSurvey[i,:] = HH_expenditure_MC.sum(axis=0)
        HH_Spending_EXIOBASEpp[i,:] = MoneyRedistribution.sum(axis=1)
        HH_Demand_Industry = np.zeros((MoneyRedistribution.shape))
    
        for m in range(0,200):
            HH_Demand_Industry[m,:] = (1-Mar_Rate[m]) * (1-Tax_Rate[m]) * MoneyRedistribution[m,:]
        HH_Demand_Industry[146,:] = 1.03 * HH_Demand_Industry[146,:]
        HH_Demand_Industry[151,:] = 1.03 * HH_Demand_Industry[151,:]
        HH_Demand_Industry[152,:] = 1.17 * HH_Demand_Industry[152,:]
        HH_Demand_Industry[153,:] = 0.68 * (((Mar_Rate[0:124] * (1-Tax_Rate[0:124])).dot(MoneyRedistribution[0:124,:])) - 0.03*HH_Demand_Industry[146,:] - 0.03*HH_Demand_Industry[151,:] - 0.17*HH_Demand_Industry[152,:])
        HH_Demand_Industry[154,:] = 0.32 * (((Mar_Rate[0:124] * (1-Tax_Rate[0:124])).dot(MoneyRedistribution[0:124,:])) - 0.03*HH_Demand_Industry[146,:] - 0.03*HH_Demand_Industry[151,:] - 0.17*HH_Demand_Industry[152,:])
    
        HH_Spending_EXIOBASEbp[i,:] = HH_Demand_Industry.sum(axis=1)    
        HH_Demand_Industry_ByCountry = np.zeros((9800))
        for m in range(0,103):
            HH_Demand_Product = HH_Demand_Industry[:,m]
            counter = 0
            for n in range(0,49):
                for o in range(0,200):
                    HH_Demand_Industry_ByCountry[counter] = HH_Demand_Product[o] * MRIO_Y_IndustrialShares[counter]
                    counter = counter+1
            HH_FinalDemand_MC[i,m,:] = HH_Demand_Industry_ByCountry

    HH_FinalDemand_Avg[:,income_group] = np.mean(HH_FinalDemand_MC.sum(axis=1),axis=0)
    HH_FinalDemand_StE[:,income_group] = np.std(HH_FinalDemand_MC.sum(axis=1),axis=0)

    for n in range(0,200):
        HH_FD_Ind[n,income_group] = HH_Demand_Industry[n,:].sum(axis=0) 


    HOc_percentage = np.zeros((iterations,12))
    for i in range(0,iterations):
        avg_exp = HH_DE_sna[33]
        for n in range(0,12):
            np.random.seed(i)
            icg_exp = truncnorm.rvs(a[33,n], b[33,n], loc = HH_expenditure[33,n], scale = HH_uncertainty[33,n] * HH_expenditure[33,n] * np.sqrt(HH_households[33,n]))
            HOc_percentage[i,n] = icg_exp/avg_exp
        
    HOo_percentage = np.zeros((iterations,12))
    for i in range(0,iterations):
        avg_exp = HH_DE_sna[32]
        for n in range(0,12):
            np.random.seed(i)
            icg_exp = truncnorm.rvs(a[32,n], b[32,n], loc = HH_expenditure[32,n], scale = HH_uncertainty[32,n] * HH_expenditure[32,n] * np.sqrt(HH_households[32,n]))
            HOo_percentage[i,n] = icg_exp/avg_exp

    HOg_percentage = np.zeros((iterations,12))
    for i in range(0,iterations):
        avg_exp = HH_DE_sna[31]
        for n in range(0,12):
            np.random.seed(i)
            icg_exp = truncnorm.rvs(a[31,n], b[31,n], loc = HH_expenditure[31,n], scale = HH_uncertainty[31,n] * HH_expenditure[31,n] * np.sqrt(HH_households[31,n]))
            HOg_percentage[i,n] = icg_exp/avg_exp
        
    TR_percentage = np.zeros((iterations,12))
    for i in range(0,iterations):
        avg_exp = HH_DE_sna[58]
        for n in range(0,12):
            np.random.seed(i)
            icg_exp = truncnorm.rvs(a[58,n], b[58,n], loc = HH_expenditure[58,n], scale = HH_uncertainty[58,n] * HH_expenditure[58,n] * np.sqrt(HH_households[58,n]))
            TR_percentage[i,n] = icg_exp/avg_exp

    LF_percentage = np.zeros((iterations,12))
    for i in range(0,iterations):
        avg_exp = np.sum(HH_DE_sna[22:27])
        for n in range(0,12):
            np.random.seed(i)
            icg_exp = np.sum(truncnorm.rvs(a[22:27,n], b[22:27,n], loc = HH_expenditure[22:27,n], scale = HH_uncertainty[22:27,n] * HH_expenditure[22:27,n] * np.sqrt(HH_households[22:27,n])))
            LF_percentage[i,n] = icg_exp/avg_exp

    WF_percentage = np.zeros((iterations,12))
    for i in range(0,iterations):
        avg_exp = HH_DE_sna[29]
        for n in range(0,12):
            np.random.seed(i)
            icg_exp = truncnorm.rvs(a[29,n], b[29,n], loc = HH_expenditure[29,n], scale = HH_uncertainty[29,n] * HH_expenditure[29,n] * np.sqrt(HH_households[29,n]))
            WF_percentage[i,n] = icg_exp/avg_exp

    MRIO_DirectCarbon = np.zeros((iterations,1707))
    MRIO_DirectCarbonHO = np.zeros((iterations,1707))
    MRIO_DirectCarbonHOc = np.zeros((iterations,1707))
    MRIO_DirectCarbonHOo = np.zeros((iterations,1707))
    MRIO_DirectCarbonHOg = np.zeros((iterations,1707))
    MRIO_DirectCarbonTR = np.zeros((iterations,1707))
    MRIO_DirectLand = np.zeros((iterations,1707))
    MRIO_DirectMaterial = np.zeros((iterations,1707))
    MRIO_DirectWater = np.zeros((iterations,1707))
    for i in range(0,iterations):
        for m in range(0,1707):
            MRIO_DirectCarbon[i,m] = (MRIO_FDE[m,35]*(HOc_percentage[i,income_group]*0.54*0.03+HOo_percentage[i,income_group]*0.54*0.44+HOg_percentage[i,income_group]*0.54*0.53+TR_percentage[i,income_group]*0.46))*HH_avg_members[0]/Population
            MRIO_DirectCarbonHO[i,m] = (MRIO_FDE[m,35]*(HOc_percentage[i,income_group]*0.54*0.03+HOo_percentage[i,income_group]*0.54*0.44+HOg_percentage[i,income_group]*0.54*0.53))*HH_avg_members[0]/Population
            MRIO_DirectCarbonHOc[i,m] = (MRIO_FDE[m,35]*(HOc_percentage[i,income_group]*0.54*0.03))*HH_avg_members[0]/Population
            MRIO_DirectCarbonHOo[i,m] = (MRIO_FDE[m,35]*(HOo_percentage[i,income_group]*0.54*0.44))*HH_avg_members[0]/Population
            MRIO_DirectCarbonHOg[i,m] = (MRIO_FDE[m,35]*(HOg_percentage[i,income_group]*0.54*0.53))*HH_avg_members[0]/Population
            MRIO_DirectCarbonTR[i,m] = (MRIO_FDE[m,35]*(TR_percentage[i,income_group])*0.46)*HH_avg_members[0]/Population
            MRIO_DirectLand[i,m] = (MRIO_FDE[m,35]*(LF_percentage[i,income_group]))*HH_avg_members[0]/Population
            MRIO_DirectWater[i,m] = (MRIO_FDE[m,35]*(WF_percentage[i,income_group]))*HH_avg_members[0]/Population
                          
    Result_workbook  = xlsxwriter.Workbook(Path_Result + 'FinalDemand_' + HHE_IncomeGroups[income_group] + '.xlsx') 
    bold = Result_workbook.add_format({'bold': True})
    Result_worksheet = Result_workbook.add_worksheet('Final Demand Spending (Euro)') 
    Result_worksheet.write(0, 0, 'Product groups, Consumer Survey, COICOP', bold)
    Result_worksheet.write(104,0, 'Total', bold)
    for m in range(0,103):
            Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
            for n in range(0,iterations):
                Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
                Result_worksheet.write(m+1,n+1, HH_Spending_ConsSurvey[n,m])
                Result_worksheet.write(104,n+1, HH_Spending_ConsSurvey[n,:].sum(axis=0))

    Result_worksheet = Result_workbook.add_worksheet('By EXIOBASE Sector (pp)') 
    Result_worksheet.write(0, 0, 'Product groups, EXIOBASE, ISIC', bold)
    Result_worksheet.write(201,0, 'Total', bold)
    for m in range(0,200):
            Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
            for n in range(0,iterations):
                Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
                Result_worksheet.write(m+1,n+1, HH_Spending_EXIOBASEpp[n,m])
                Result_worksheet.write(201,n+1, HH_Spending_EXIOBASEpp[n,:].sum(axis=0))

    Result_worksheet = Result_workbook.add_worksheet('By EXIOBASE Sector (bp)') 
    Result_worksheet.write(0, 0, 'Product groups, EXIOBASE, ISIC', bold)
    Result_worksheet.write(201,0, 'Total', bold)
    for m in range(0,200):
            Result_worksheet.write(m+1, 0, MRIO_Prod[0+m])
            for n in range(0,iterations):
                Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
                Result_worksheet.write(m+1,n+1, HH_Spending_EXIOBASEbp[n,m])
                Result_worksheet.write(201,n+1, HH_Spending_EXIOBASEbp[n,:].sum(axis=0))

    Result_worksheet = Result_workbook.add_worksheet('By Country and Sector (Average)') 
    Result_worksheet.write(0, 0, 'Product groups, Exiobase, ISIC', bold)
    Result_worksheet.write(9801,0, 'Total', bold)
    Result_worksheet.write(0, 1, 'Country', bold)
    Result_worksheet.write(0, 2, 'Average', bold)
    Result_worksheet.write(0, 3, 'Standard Deviation', bold)
    for m in range(0,9800):
        Result_worksheet.write(m+1, 0, MRIO_Prod[(0+m)%200])
        Result_worksheet.write(m+1, 1, MRIO_Reg[int((0+m)/200)])
        Result_worksheet.write(m+1, 2, np.average(HH_FinalDemand_Avg[m,income_group]))
        Result_worksheet.write(m+1, 3, np.average(HH_FinalDemand_StE[m,income_group]))

    Result_workbook.close()

    Scope1_LandFP     = (MRIO_DirectLand * MRIO_Char[8,:]).sum(axis=1)
    Scope1_CarbonFP   = (MRIO_DirectCarbon * MRIO_Char[4,:]).sum(axis=1)
    Scope1_HOCarbonFP = (MRIO_DirectCarbonHO * MRIO_Char[4,:]).sum(axis=1)
    Scope1_HOcCarbonFP = (MRIO_DirectCarbonHOc * MRIO_Char[4,:]).sum(axis=1)
    Scope1_HOoCarbonFP = (MRIO_DirectCarbonHOo * MRIO_Char[4,:]).sum(axis=1)
    Scope1_HOgCarbonFP = (MRIO_DirectCarbonHOg * MRIO_Char[4,:]).sum(axis=1)
    Scope1_TRCarbonFP = (MRIO_DirectCarbonTR * MRIO_Char[4,:]).sum(axis=1)
    Scope1_WaterFP    = (MRIO_DirectWater * MRIO_Char[31,:]).sum(axis=1)
    
    MRIO_Carbon = np.zeros((1707,9800))
    MRIO_Land = np.zeros((1707,9800))
    MRIO_Material = np.zeros((1707,9800))
    MRIO_Water = np.zeros((1707,9800))
    for m in range(0,1707):
        for n in range(0,9800):
            MRIO_Carbon[m,n] = MRIO_Char[4,m]*MRIO_SL[m,n]
            MRIO_Land[m,n] = MRIO_Char[8,m]*MRIO_SL[m,n]
            MRIO_Material[m,n] = MRIO_Char[22,m]*MRIO_SL[m,n]
            MRIO_Water[m,n] = MRIO_Char[31,m]*MRIO_SL[m,n]

    Footprint_Land_EXIO = np.zeros((iterations,200))
    Footprint_Land_Prod = np.zeros((iterations,103))
    Footprint_Land_ProEuro = np.zeros((iterations,103))
    Footprint_Carbon_EXIO = np.zeros((iterations,200))
    Footprint_Carbon_Prod = np.zeros((iterations,103))
    Footprint_Carbon_ProEuro = np.zeros((iterations,200))
    Footprint_Material_EXIO = np.zeros((iterations,200))
    Footprint_Material_Prod = np.zeros((iterations,103))
    Footprint_Material_ProEuro = np.zeros((iterations,200))
    Footprint_Water_EXIO = np.zeros((iterations,200))
    Footprint_Water_Prod = np.zeros((iterations,103))
    Footprint_Water_ProEuro = np.zeros((iterations,200))
    for m in range (0,iterations):
        print(m)
        for n in range (0,103):
            Footprint_Carbon = MRIO_Carbon.dot(HH_FinalDemand_MC[m,n,:])/1E6
            Footprint_Carbon_Prod[m,n] = Footprint_Carbon.sum(axis=0) # in kg CO2eq
            Footprint_Land = MRIO_Land.dot(HH_FinalDemand_MC[m,n,:])                            
            Footprint_Land_Prod[m,n] = Footprint_Land.sum(axis=0) # in 1000 m²
            Footprint_Material = MRIO_Material.dot(HH_FinalDemand_MC[m,n,:])                            
            Footprint_Material_Prod[m,n] = Footprint_Material.sum(axis=0) # in kg
            Footprint_Water = MRIO_Water.dot(HH_FinalDemand_MC[m,n,:])                           
            Footprint_Water_Prod[m,n] = Footprint_Water.sum(axis=0) # in m³    

    Result_workbook  = xlsxwriter.Workbook(Path_Result + 'LandFP_ByProduct_' + HHE_IncomeGroups[income_group] + '.xlsx') 
    bold = Result_workbook.add_format({'bold': True})
    Result_worksheet = Result_workbook.add_worksheet('Total, per HH, by product') 
    Result_worksheet.write(0, 0, 'Product Type, Unit: m²', bold)
    Result_worksheet.write(104,0, 'Total Indirect', bold)
    Result_worksheet.write(105,0, 'Direct Use', bold)
    Result_worksheet.write(106,0, 'Total', bold)
    for m in range(0,103):    
        Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
        for n in range(0,iterations):
            Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
            Result_worksheet.write(m+1,n+1, Footprint_Land_Prod[n,m])
    for n in range(0,iterations):
        Result_worksheet.write(104,n+1, Footprint_Land_Prod[n,:].sum(axis=0))
        Result_worksheet.write(105,n+1, Scope1_LandFP[n]*1E6)
        Result_worksheet.write(106,n+1, Footprint_Land_Prod[n,:].sum(axis=0) + Scope1_LandFP[n]*1E6)
    
    Result_worksheet = Result_workbook.add_worksheet('Total, per cap, by product') 
    Result_worksheet.write(0, 0, 'Product Type, Unit: m²', bold)
    Result_worksheet.write(104,0, 'Total Indirect', bold)
    Result_worksheet.write(105,0, 'Direct Use', bold)
    Result_worksheet.write(106,0, 'Total', bold)
    for m in range(0,103):    
        Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
        for n in range(0,iterations):
            Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
            Result_worksheet.write(m+1,n+1, Footprint_Land_Prod[n,m]/HH_avg_members[income_group])
    for n in range(0,iterations):
        Result_worksheet.write(104,n+1, Footprint_Land_Prod[n,:].sum(axis=0)/HH_avg_members[income_group])
        Result_worksheet.write(105,n+1, Scope1_LandFP[n]*1E6/HH_avg_members[income_group])
        Result_worksheet.write(106,n+1, (Footprint_Land_Prod[n,:].sum(axis=0) + Scope1_LandFP[n]*1E6)/HH_avg_members[income_group])

    Result_workbook.close()

    Result_workbook  = xlsxwriter.Workbook(Path_Result + 'CarbonFP_ByProduct_' + HHE_IncomeGroups[income_group] + '.xlsx') 
    bold = Result_workbook.add_format({'bold': True})
    Result_worksheet = Result_workbook.add_worksheet('Total, per HH, by product') 
    Result_worksheet.write(0, 0, 'Product Type, Unit: kg CO2eq', bold)
    Result_worksheet.write(104,0, 'Total Indirect', bold)
    Result_worksheet.write(105,0, 'Direct Housing', bold)
    Result_worksheet.write(106,0, 'Direct Housing, Coal', bold)
    Result_worksheet.write(107,0, 'Direct Housing, Oil', bold)
    Result_worksheet.write(108,0, 'Direct Housing, Gas', bold)
    Result_worksheet.write(109,0, 'Direct Transport', bold)
    Result_worksheet.write(110,0, 'Total', bold)
    for m in range(0,103):    
        Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
        for n in range(0,iterations):
            Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
            Result_worksheet.write(m+1,n+1, Footprint_Carbon_Prod[n,m])
    for n in range(0,iterations):
        Result_worksheet.write(104,n+1, Footprint_Carbon_Prod[n,:].sum(axis=0))       
        Result_worksheet.write(105,n+1, Scope1_HOCarbonFP[n])     
        Result_worksheet.write(106,n+1, Scope1_HOcCarbonFP[n])
        Result_worksheet.write(107,n+1, Scope1_HOoCarbonFP[n])
        Result_worksheet.write(108,n+1, Scope1_HOgCarbonFP[n])
        Result_worksheet.write(109,n+1, Scope1_TRCarbonFP[n])
        Result_worksheet.write(110,n+1, Footprint_Carbon_Prod[n,:].sum(axis=0) + Scope1_CarbonFP[n])

    Result_worksheet = Result_workbook.add_worksheet('Total, per cap, by product') 
    Result_worksheet.write(0, 0, 'Product Type, Unit: kg CO2eq', bold)
    Result_worksheet.write(104,0, 'Total Indirect', bold)
    Result_worksheet.write(105,0, 'Direct Housing', bold)
    Result_worksheet.write(106,0, 'Direct Housing, Coal', bold)
    Result_worksheet.write(107,0, 'Direct Housing, Oil', bold)
    Result_worksheet.write(108,0, 'Direct Housing, Gas', bold)
    Result_worksheet.write(109,0, 'Direct Transport', bold)
    Result_worksheet.write(110,0, 'Total', bold)
    for m in range(0,103):    
        Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
        for n in range(0,iterations):
            Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
            Result_worksheet.write(m+1,n+1, Footprint_Carbon_Prod[n,m]/HH_avg_members[income_group])
    for n in range(0,iterations):
        Result_worksheet.write(104,n+1, Footprint_Carbon_Prod[n,:].sum(axis=0)/HH_avg_members[income_group])
        Result_worksheet.write(105,n+1, Scope1_HOCarbonFP[n]/HH_avg_members[income_group])
        Result_worksheet.write(106,n+1, Scope1_HOcCarbonFP[n]/HH_avg_members[income_group])
        Result_worksheet.write(107,n+1, Scope1_HOoCarbonFP[n]/HH_avg_members[income_group])
        Result_worksheet.write(108,n+1, Scope1_HOgCarbonFP[n]/HH_avg_members[income_group])
        Result_worksheet.write(109,n+1, Scope1_TRCarbonFP[n]/HH_avg_members[income_group])
        Result_worksheet.write(110,n+1, (Footprint_Carbon_Prod[n,:].sum(axis=0) + Scope1_CarbonFP[n])/HH_avg_members[income_group])

    Result_workbook.close()

    Result_workbook  = xlsxwriter.Workbook(Path_Result + 'MaterialFP_ByProduct_' + HHE_IncomeGroups[income_group] + '.xlsx') 
    bold = Result_workbook.add_format({'bold': True})
    Result_worksheet = Result_workbook.add_worksheet('Total, per HH, by product') 
    Result_worksheet.write(0, 0, 'Product Type, Unit: kg', bold)
    Result_worksheet.write(104,0, 'Total', bold)
    Result_worksheet.write(0, 1, 'Best case', bold)
    Result_worksheet.write(0, 2, 'Worst case', bold)
    for m in range(0,103):    
        Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
        for n in range(0,iterations):
            Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
            Result_worksheet.write(m+1,n+1, Footprint_Material_Prod[n,m])
    for n in range(0,iterations):
        Result_worksheet.write(104,n+1, Footprint_Material_Prod[n,:].sum(axis=0))

    Result_worksheet = Result_workbook.add_worksheet('Total, per cap, by product') 
    Result_worksheet.write(0, 0, 'Product Type, Unit: kg', bold)
    Result_worksheet.write(104,0, 'Total', bold)
    Result_worksheet.write(0, 1, 'Best case', bold)
    Result_worksheet.write(0, 2, 'Worst case', bold)
    for m in range(0,103):    
        Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
        for n in range(0,iterations):
            Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
            Result_worksheet.write(m+1,n+1, Footprint_Material_Prod[n,m]/HH_avg_members[income_group])
    for n in range(0,iterations):
        Result_worksheet.write(104,n+1, Footprint_Material_Prod[n,:].sum(axis=0)/HH_avg_members[income_group])

    Result_workbook.close()

    Result_workbook  = xlsxwriter.Workbook(Path_Result + 'WaterFP_ByProduct_' + HHE_IncomeGroups[income_group] + '.xlsx') 
    bold = Result_workbook.add_format({'bold': True})
    Result_worksheet = Result_workbook.add_worksheet('Total, per HH, by product') 
    Result_worksheet.write(0, 0, 'Product Type, Unit: m³', bold)
    Result_worksheet.write(104,0, 'Total Indirect', bold)
    Result_worksheet.write(105,0, 'Direct Use', bold)
    Result_worksheet.write(106,0, 'Total', bold)
    for m in range(0,103):    
        Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
        for n in range(0,iterations):
            Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
            Result_worksheet.write(m+1,n+1, Footprint_Water_Prod[n,m])
    for n in range(0,iterations):
        Result_worksheet.write(104,n+1, Footprint_Water_Prod[n,:].sum(axis=0))
        Result_worksheet.write(105,n+1, Scope1_WaterFP[n]*1E6)
        Result_worksheet.write(106,n+1, Footprint_Water_Prod[n,:].sum(axis=0) + Scope1_WaterFP[n]*1E6)

    Result_worksheet = Result_workbook.add_worksheet('Total, per cap, by product') 
    Result_worksheet.write(0, 0, 'Product Type, Unit: m³', bold)
    Result_worksheet.write(104,0, 'Total Indirect', bold)
    Result_worksheet.write(105,0, 'Direct Use', bold)
    Result_worksheet.write(106,0, 'Total', bold)
    for m in range(0,103):    
        Result_worksheet.write(m+1, 0, EVS_Prod[0+m])
        for n in range(0,iterations):
            Result_worksheet.write(0,n+1, str(n+1) + '. Iteration', bold)
            Result_worksheet.write(m+1,n+1, Footprint_Water_Prod[n,m]/HH_avg_members[income_group])
    for n in range(0,iterations):
        Result_worksheet.write(104,n+1, Footprint_Water_Prod[n,:].sum(axis=0)/HH_avg_members[income_group])
        Result_worksheet.write(105,n+1, Scope1_WaterFP[n]*1E6/HH_avg_members[income_group])
        Result_worksheet.write(106,n+1, (Footprint_Water_Prod[n,:].sum(axis=0) + Scope1_WaterFP[n]*1E6)/HH_avg_members[income_group])

    Result_workbook.close()

#%%

##################################################################
# 17. Step: Build tuple matrix to calculate footprint by sources #
##################################################################

# Define ancillary functions to construct aggregation matrices

def MI_Tuple(value, Is): 
    """
    Define function for obtaining multiindex tuple from index value
    value: flattened index position, Is: Number of values for each index dimension
    Example: MI_Tuple(10, [3,4,2,6]) returns [0,0,1,4]
    MI_Tuple(138, [100,10,5]) returns [2,7,3]
    MI_Tuple is the inverse of Tuple_MI.
    """
    IsValuesRev = []
    CurrentValue = value
    for m in range(0,len(Is)):
        IsValuesRev.append(CurrentValue % Is[len(Is)-m-1])
        CurrentValue = CurrentValue // Is[len(Is)-m-1]
    return IsValuesRev[::-1]    

def Tuple_MI(Tuple, IdxLength): 
    """
    Function to return the absolution position of a multiindex when the index tuple
    and the index hierarchy and size are given.
    Example: Tuple_MI([2,7,3],[100,10,5]) returns 138
    Tuple_MI([0,0,1,4],[3,4,2,6]) returns 10
    Tuple_MI is the inverse of MI_Tuple.
    """
    # First, generate the index position offset values
    IdxShift =  IdxLength[1:] +  IdxLength[:1] # Shift 1 to left
    IdxShift[-1] = 1 # Replace lowest index by 1
    IdxShift.reverse()
    IdxPosOffset = np.cumproduct(IdxShift).tolist()
    IdxPosOffset.reverse()
    Position = np.sum([a*b for a,b in zip(Tuple,IdxPosOffset)])
    return Position

def build_Aggregation_Matrix(Position_Vector): # from PySUT
    """Turn a vector of target positions into a matrix that aggregates 
    or re-arranges rows of the table it is multiplied to from the left 
    (or columns, if multiplied transposed from the right)"""
    AM_length = Position_Vector.max() + 1 # Maximum row number of new matrix (+1 to get the right length, as 0 is the smallest target position entry.)
    AM_width  = len(Position_Vector) # Number of rows of the to-be-aggregated matrix
    Rearrange_Matrix = np.zeros((AM_length,AM_width))
    for m in range(0,len(Position_Vector)):
        Rearrange_Matrix[Position_Vector[m].item(0),m] = 1 # place 1 in aggregation matrix at [PositionVector[m],m], so that column m is aggregated with Positionvector[m] in the aggregated matrix
    return Rearrange_Matrix

def build_MultiIndex_Aggregation_Matrix(Position_Vectors):
    """Turn a list of vectors of target positions that represent aggregations of the different levels of a multi-index of a table into a matrix that aggregates 
    or re-arranges rows of the multiindex table it is multiplied to from the left 
    (or columns, if multiplied transposed from the right)"""   
    OldLength = [len(i)    for i in Position_Vectors]
    NewLength = [max(i) +1 for i in Position_Vectors]
    Rearrange_Matrix = np.zeros((np.product(NewLength),np.product(OldLength)))
    for m in range(0,np.product(OldLength)):
        OldIndexTuple = MI_Tuple(m,OldLength) # convert running index to tuple (column index)
        NewIndexTuple = [Position_Vectors[i][OldIndexTuple[i]] for i in range(0,len(OldIndexTuple))] # convert unaggregated tuple to aggregated tuple
        NewIndexPos   = Tuple_MI(NewIndexTuple, NewLength)# Calculate new running index (row index)
        Rearrange_Matrix[NewIndexPos,m] = 1 # Aggregate/resort row m into row NewIndexPos.
    return Rearrange_Matrix


#%%

############################################
# 18. Step: Calculate footprint by sources #
############################################

# Different from previous calculation in 14-15 Step, this step calculates footprint
# based on the industrial source of emissions or resource uses.

Midpoints = MRIO_Char.dot(MRIO_S)
print(Midpoints.shape)

#Build aggregation matrices:
Aggregation_MatrixC_Rows = build_MultiIndex_Aggregation_Matrix([Rv,SCv])
Aggregation_MatrixL_Rows = build_MultiIndex_Aggregation_Matrix([Rv,SLv])
Aggregation_MatrixM_Rows = build_MultiIndex_Aggregation_Matrix([Rv,SMv])
Aggregation_MatrixW_Rows = build_MultiIndex_Aggregation_Matrix([Rv,SWv])

print(Aggregation_MatrixC_Rows.shape)
print(Aggregation_MatrixC_Rows.sum())
Aggregation_Matrix_Cols = build_MultiIndex_Aggregation_Matrix([np.zeros((49),dtype = 'int').tolist(),Pv])
print(Aggregation_Matrix_Cols.shape)
print(Aggregation_Matrix_Cols.sum())

HOc_avg = np.average(HOc_percentage, axis = 0)
HOo_avg = np.average(HOo_percentage, axis = 0)
HOg_avg = np.average(HOg_percentage, axis = 0)

TR_avg = np.average(TR_percentage, axis = 0)
    
LF_avg = np.average(LF_percentage, axis = 0)
WF_avg = np.average(WF_percentage, axis = 0)

Footprint_Carbon_Agg_All = np.zeros((12*36,12))
Footprint_Land_Agg_All = np.zeros((12*36,12))
Footprint_Material_Agg_All = np.zeros((12*36,12))
Footprint_Water_Agg_All = np.zeros((12*36,12))
for group in range(0,12): # For all 12 Y groups

    X_Full = (csr_matrix(MRIO_L) * csr_matrix(np.diag(HH_FinalDemand_Avg[:,group]))).toarray()  # use sparse matrix algebra as numpy.dot chrashes on some machines for large matrices.
    X_Full.shape # 9800 x 9800
    
    # Aggregate X_Full to sector x region for industries and products x 1 for products:
    # First, the product groups, here, columns are aggregated, 
    # which is done by multiplying the transposed aggregation matrix from the right:
    X_ProdAgg = X_Full.dot(Aggregation_Matrix_Cols.transpose())
    X_ProdAgg.shape # Full sector and region of emissions detail, 11 product groups.
    
    # Calculate footprint, do not sum over the industry dimension using np.einsum
    Footprint_Carbon_ProdAgg_Single = np.einsum('b,bc->bc',Midpoints[4,:],X_ProdAgg) # row 4 for carbon fp
    print(Footprint_Carbon_ProdAgg_Single.shape)
    Footprint_Carbon_Agg_Single     = Aggregation_MatrixC_Rows.dot(Footprint_Carbon_ProdAgg_Single)
    print(Footprint_Carbon_Agg_Single.shape)    
    Footprint_Carbon_Agg_All[group*36:group*36+36,:] = Footprint_Carbon_Agg_Single.copy()*1E-6/HH_avg_members[group]    
    # Determine and add direct indicator:
    Footprint_Direct_Housing_Carbon = (MRIO_Char[4,:].dot(MRIO_FDE[:,35]*(HOc_avg[group]*0.54*0.03+HOo_avg[group]*0.54*0.44+HOg_avg[group]*0.54*0.53)))*HH_avg_members[0]/(Population*HH_avg_members[group])
    Footprint_Direct_Transport_Carbon = (MRIO_Char[4,:].dot(MRIO_FDE[:,35]*TR_avg[group]*0.46))*HH_avg_members[0]/(Population*HH_avg_members[group])
    Footprint_Carbon_Agg_All[group * 36 + 5,3] = Footprint_Direct_Housing_Carbon # column offset 5 is for Germany direct, row 6 is for housing
    Footprint_Carbon_Agg_All[group * 36 + 5,10] = Footprint_Direct_Transport_Carbon # column offset 5 is for Germany direct, row 6 is for housing
  
    # Calculate footprint, do not sum over the industry dimension using np.einsum
    Footprint_Land_ProdAgg_Single = np.einsum('b,bc->bc',Midpoints[8,:],X_ProdAgg) # row 8 for land fp
    print(Footprint_Land_ProdAgg_Single.shape)
    Footprint_Land_Agg_Single     = Aggregation_MatrixL_Rows.dot(Footprint_Land_ProdAgg_Single)
    print(Footprint_Land_Agg_Single.shape)    
    Footprint_Land_Agg_All[group*36:group*36+36,:] = Footprint_Land_Agg_Single.copy()/HH_avg_members[group]       
    # Determine and add direct indicator:
    Footprint_Direct_Single_Land                   = (MRIO_Char[8,:].dot(MRIO_FDE[:,35]*(LF_avg[group]))*1E6)*HH_avg_members[0]/(Population*HH_avg_members[group])
    Footprint_Land_Agg_All[group * 36 + 5,6] = Footprint_Direct_Single_Land # column offset 5 is for Germany direct, row 2 is for direct emissions
 
    # Calculate footprint, do not sum over the industry dimension using np.einsum
    Footprint_Material_ProdAgg_Single = np.einsum('b,bc->bc',Midpoints[22,:],X_ProdAgg) # row 22 for material fp
    print(Footprint_Material_ProdAgg_Single.shape)
    Footprint_Material_Agg_Single     = Aggregation_MatrixM_Rows.dot(Footprint_Material_ProdAgg_Single)
    print(Footprint_Material_Agg_Single.shape)    
    Footprint_Material_Agg_All[group*36:group*36+36,:] = Footprint_Material_Agg_Single.copy()*1E-3/HH_avg_members[group]       
    
    # Calculate footprint, do not sum over the industry dimension using np.einsum
    Footprint_Water_ProdAgg_Single = np.einsum('b,bc->bc',Midpoints[31,:],X_ProdAgg) # row 31 for water fp
    print(Footprint_Water_ProdAgg_Single.shape)
    Footprint_Water_Agg_Single     = Aggregation_MatrixW_Rows.dot(Footprint_Water_ProdAgg_Single)
    print(Footprint_Water_Agg_Single.shape)    
    Footprint_Water_Agg_All[group*36:group*36+36,:] = Footprint_Water_Agg_Single.copy()/HH_avg_members[group]       
    # Determine and add direct indicator:
    Footprint_Direct_Single_Water                   = (MRIO_Char[31,:].dot(MRIO_FDE[:,35]*(WF_avg[group]))*1E6)*HH_avg_members[0]/(Population*HH_avg_members[group])
    Footprint_Water_Agg_All[group * 36 + 5,3] = Footprint_Direct_Single_Water # column offset 5 is for Germany direct, row 2 is for direct emissions                          
                           
# Move FP for all 12 income groups to pandas dataframe and export:
# Create multiindex for rows:
RowIndexCF = pd.MultiIndex.from_product([ICG_name[0:12],Ra,SCa], names=('ICG', 'region', 'sector'))
RowIndexLF = pd.MultiIndex.from_product([ICG_name[0:12],Ra,SLa], names=('ICG', 'region', 'sector'))
RowIndexMF = pd.MultiIndex.from_product([ICG_name[0:12],Ra,SMa], names=('ICG', 'region', 'sector'))
RowIndexWF = pd.MultiIndex.from_product([ICG_name[0:12],Ra,SWa], names=('ICG', 'region', 'sector'))
print(RowIndexCF)

RowDIndexICG = pd.MultiIndex.from_product([ICG_name[0:12],Ra,SMv], names=('ICG', 'region', 'sector'))

# Create dataframe:
CarbonFootprintDF_ICG = pd.DataFrame(Footprint_Carbon_Agg_All, index=RowIndexCF, columns=Pa)
LandFootprintDF_ICG = pd.DataFrame(Footprint_Land_Agg_All, index=RowIndexLF, columns=Pa)
MaterialFootprintDF_ICG = pd.DataFrame(Footprint_Material_Agg_All, index=RowIndexMF, columns=Pa)
WaterFootprintDF_ICG = pd.DataFrame(Footprint_Water_Agg_All, index=RowIndexWF, columns=Pa)

print(CarbonFootprintDF_ICG)
print(LandFootprintDF_ICG)
print(MaterialFootprintDF_ICG)
print(WaterFootprintDF_ICG)

Mylog.info('<p>Carbon Footprint per Income Group<br>')

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(Path_Result + 'Footprint Detail Per Income Group.xlsx', engine='xlsxwriter')

# Convert the dataframe to an XlsxWriter Excel object.
CarbonFootprintDF_ICG.to_excel(writer, sheet_name='CF_Detail')
LandFootprintDF_ICG.to_excel(writer, sheet_name='LF_Detail')
MaterialFootprintDF_ICG.to_excel(writer, sheet_name='MF_Detail')
WaterFootprintDF_ICG.to_excel(writer, sheet_name='WF_Detail')

# Close the Pandas Excel writer and output the Excel file.
writer.save()

Mylog.info('<p><b>Save Ratio of Household Spending of Sectors Related to Direct Emissions/Use. </b><br>')
# By product type
#
Result_workbook  = xlsxwriter.Workbook(Path_Result + 'Direct_SpendingRatio.xlsx') 
bold = Result_workbook.add_format({'bold': True})
Result_worksheet = Result_workbook.add_worksheet('Ratio') 
Result_worksheet.write(0, 0, 'Income Group', bold)
Result_worksheet.write(0, 1, 'Household Members', bold)
Result_worksheet.write(0, 2, 'DE_Housing Solid', bold)
Result_worksheet.write(0, 3, 'DE_Housing Oil', bold)
Result_worksheet.write(0, 4, 'DE_Housing Gas', bold)
Result_worksheet.write(0, 5, 'DE_Transport', bold)
Result_worksheet.write(0, 6, 'Land Use', bold)
Result_worksheet.write(0, 7, 'Water Use', bold)
for m in range(0,12):    
    Result_worksheet.write(m+1, 0, ICG_name[m])
    Result_worksheet.write(m+1, 1, HH_avg_members[m])
    Result_worksheet.write(m+1, 2, HOc_avg[m])
    Result_worksheet.write(m+1, 3, HOo_avg[m])
    Result_worksheet.write(m+1, 4, HOg_avg[m])
    Result_worksheet.write(m+1, 5, TR_avg[m])
    Result_worksheet.write(m+1, 6, LF_avg[m])
    Result_worksheet.write(m+1, 7, WF_avg[m])
Result_workbook.close()

#%%

#############################################################
# 19. Step: Scenario analysis of changing the demand source #
#############################################################

#These two sections are used for scenario modelling where 50% of clothing and household goods from developed countries consumed by low-income group are shifted to China,
# and 50% of clothing and household goods from developing countries consumed by high-income group are shifted to Italy and Germany, respectively. 
LI = np.array(np.where(np.array(Iv) == 0)).astype(int)[0]
HH_Demand_Industry_LowIncome = np.zeros(HH_FinalDemand_Avg[:,1].shape)
HH_Demand_Industry_LowIncome = HH_FinalDemand_Avg[:,1].copy()
Clothes = np.zeros(3)
Goods = np.zeros(3)
Furniture = np.zeros(1)
for k in LI:
    Clothes += 0.5*HH_Demand_Industry_LowIncome[k*200+54:k*200+57]
    HH_Demand_Industry_LowIncome[k*200+54:k*200+57] = HH_Demand_Industry_LowIncome[k*200+54:k*200+57] - 0.5*HH_Demand_Industry_LowIncome[k*200+54:k*200+57]
    Goods += 0.5*HH_Demand_Industry_LowIncome[k*200+118:k*200+121]
    HH_Demand_Industry_LowIncome[k*200+118:k*200+121] = HH_Demand_Industry_LowIncome[k*200+118:k*200+121] - 0.5*HH_Demand_Industry_LowIncome[k*200+118:k*200+121]
    Furniture += 0.5*HH_Demand_Industry_LowIncome[k*200+124]
    HH_Demand_Industry_LowIncome[k*200+124] = HH_Demand_Industry_LowIncome[k*200+124] - 0.5*HH_Demand_Industry_LowIncome[k*200+124]
        
HH_Demand_Industry_LowIncome[30*200+54:30*200+57] += Clothes
HH_Demand_Industry_LowIncome[30*200+118:30*200+121] += Goods
HH_Demand_Industry_LowIncome[30*200+124] += Furniture
    
HH_FinalDemand_Avg[:,1] = HH_Demand_Industry_LowIncome.copy()
                                
HI = np.array(np.where(np.array(Iv) == 1)).astype(int)[0]
HH_Demand_Industry_HighIncome = np.zeros(HH_FinalDemand_Avg[:,11].shape)
HH_Demand_Industry_HighIncome = HH_FinalDemand_Avg[:,11].copy()
Clothes = np.zeros(3)
Goods = np.zeros(3)
Furniture = np.zeros(1)
for l in HI:
    Clothes += 0.5*HH_Demand_Industry_HighIncome[l*200+54:l*200+57]
    HH_Demand_Industry_HighIncome[l*200+54:l*200+57] = HH_Demand_Industry_HighIncome[l*200+54:l*200+57] - 0.5*HH_Demand_Industry_HighIncome[l*200+54:l*200+57]
    Goods += 0.5*HH_Demand_Industry_HighIncome[l*200+118:l*200+121]
    HH_Demand_Industry_HighIncome[l*200+118:l*200+121] = HH_Demand_Industry_HighIncome[l*200+118:l*200+121] - 0.5*HH_Demand_Industry_HighIncome[l*200+118:l*200+121]
    Furniture += 0.5*HH_Demand_Industry_HighIncome[l*200+124]
    HH_Demand_Industry_HighIncome[l*200+124] = HH_Demand_Industry_HighIncome[l*200+124] - 0.5*HH_Demand_Industry_HighIncome[l*200+124]
        
HH_Demand_Industry_HighIncome[15*200+54:15*200+57] += Clothes
HH_Demand_Industry_HighIncome[5*200+118:5*200+121] += Goods
HH_Demand_Industry_HighIncome[5*200+124] += Furniture
    
HH_FinalDemand_Avg[:,11] = HH_Demand_Industry_HighIncome.copy()

for group in [1,11]: # For lowest and highest income groups

    X_Full = (csr_matrix(MRIO_L) * csr_matrix(np.diag(HH_FinalDemand_Avg[:,group]))).toarray()  # use sparse matrix algebra as numpy.dot chrashes on some machines for large matrices.
    X_Full.shape # 9800 x 9800
    
    # Aggregate X_Full to sector x region for industries and products x 1 for products:
    # First, the product groups, here, columns are aggregated, 
    # which is done by multiplying the transposed aggregation matrix from the right:
    X_ProdAgg = X_Full.dot(Aggregation_Matrix_Cols.transpose())
    X_ProdAgg.shape # Full sector and region of emissions detail, 11 product groups.
    
    # Calculate footprint, do not sum over the industry dimension using np.einsum
    Footprint_Carbon_ProdAgg_Single = np.einsum('b,bc->bc',Midpoints[4,:],X_ProdAgg) # row 4 for carbon fp
    print(Footprint_Carbon_ProdAgg_Single.shape)
    Footprint_Carbon_Agg_Single     = Aggregation_MatrixC_Rows.dot(Footprint_Carbon_ProdAgg_Single)
    print(Footprint_Carbon_Agg_Single.shape)    
    Footprint_Carbon_Agg_All[group*36:group*36+36,:] = Footprint_Carbon_Agg_Single.copy()*1E-6/HH_avg_members[group]    
    # Determine and add direct indicator:
    Footprint_Direct_Housing_Carbon = (MRIO_Char[4,:].dot(MRIO_FDE[:,35]*(HOc_avg[group]*0.54*0.03+HOo_avg[group]*0.54*0.44+HOg_avg[group]*0.54*0.53)))*HH_avg_members[0]/(Population*HH_avg_members[group])
    Footprint_Direct_Transport_Carbon = (MRIO_Char[4,:].dot(MRIO_FDE[:,35]*TR_avg[group]*0.46))*HH_avg_members[0]/(Population*HH_avg_members[group])
    Footprint_Carbon_Agg_All[group * 36 + 5,3] = Footprint_Direct_Housing_Carbon # column offset 5 is for Germany direct, row 6 is for housing
    Footprint_Carbon_Agg_All[group * 36 + 5,10] = Footprint_Direct_Transport_Carbon # column offset 5 is for Germany direct, row 6 is for housing
  
    # Calculate footprint, do not sum over the industry dimension using np.einsum
    Footprint_Land_ProdAgg_Single = np.einsum('b,bc->bc',Midpoints[8,:],X_ProdAgg) # row 8 for land fp
    print(Footprint_Land_ProdAgg_Single.shape)
    Footprint_Land_Agg_Single     = Aggregation_MatrixL_Rows.dot(Footprint_Land_ProdAgg_Single)
    print(Footprint_Land_Agg_Single.shape)    
    Footprint_Land_Agg_All[group*36:group*36+36,:] = Footprint_Land_Agg_Single.copy()/HH_avg_members[group]       
    # Determine and add direct indicator:
    Footprint_Direct_Single_Land                   = (MRIO_Char[8,:].dot(MRIO_FDE[:,35]*(LF_avg[group]))*1E6)*HH_avg_members[0]/(Population*HH_avg_members[group])
    Footprint_Land_Agg_All[group * 36 + 5,6] = Footprint_Direct_Single_Land # column offset 5 is for Germany direct, row 2 is for direct emissions
 
    # Calculate footprint, do not sum over the industry dimension using np.einsum
    Footprint_Material_ProdAgg_Single = np.einsum('b,bc->bc',Midpoints[22,:],X_ProdAgg) # row 22 for material fp
    print(Footprint_Material_ProdAgg_Single.shape)
    Footprint_Material_Agg_Single     = Aggregation_MatrixM_Rows.dot(Footprint_Material_ProdAgg_Single)
    print(Footprint_Material_Agg_Single.shape)    
    Footprint_Material_Agg_All[group*36:group*36+36,:] = Footprint_Material_Agg_Single.copy()*1E-3/HH_avg_members[group]       
    
    # Calculate footprint, do not sum over the industry dimension using np.einsum
    Footprint_Water_ProdAgg_Single = np.einsum('b,bc->bc',Midpoints[31,:],X_ProdAgg) # row 31 for water fp
    print(Footprint_Water_ProdAgg_Single.shape)
    Footprint_Water_Agg_Single     = Aggregation_MatrixW_Rows.dot(Footprint_Water_ProdAgg_Single)
    print(Footprint_Water_Agg_Single.shape)    
    Footprint_Water_Agg_All[group*36:group*36+36,:] = Footprint_Water_Agg_Single.copy()/HH_avg_members[group]       
    # Determine and add direct indicator:
    Footprint_Direct_Single_Water                   = (MRIO_Char[31,:].dot(MRIO_FDE[:,35]*(WF_avg[group]))*1E6)*HH_avg_members[0]/(Population*HH_avg_members[group])
    Footprint_Water_Agg_All[group * 36 + 5,3] = Footprint_Direct_Single_Water # column offset 5 is for Germany direct, row 2 is for direct emissions                          
                           
# Move FP for all 12 income groups to pandas dataframe and export:
# Create multiindex for rows:
RowIndexCF = pd.MultiIndex.from_product([ICG_name[0:12],Ra,SCa], names=('ICG', 'region', 'sector'))
RowIndexLF = pd.MultiIndex.from_product([ICG_name[0:12],Ra,SLa], names=('ICG', 'region', 'sector'))
RowIndexMF = pd.MultiIndex.from_product([ICG_name[0:12],Ra,SMa], names=('ICG', 'region', 'sector'))
RowIndexWF = pd.MultiIndex.from_product([ICG_name[0:12],Ra,SWa], names=('ICG', 'region', 'sector'))
print(RowIndexCF)

RowDIndexICG = pd.MultiIndex.from_product([ICG_name[0:12],Ra,SMv], names=('ICG', 'region', 'sector'))

# Create dataframe:
CarbonFootprintDF_ICG = pd.DataFrame(Footprint_Carbon_Agg_All, index=RowIndexCF, columns=Pa)
LandFootprintDF_ICG = pd.DataFrame(Footprint_Land_Agg_All, index=RowIndexLF, columns=Pa)
MaterialFootprintDF_ICG = pd.DataFrame(Footprint_Material_Agg_All, index=RowIndexMF, columns=Pa)
WaterFootprintDF_ICG = pd.DataFrame(Footprint_Water_Agg_All, index=RowIndexWF, columns=Pa)

print(CarbonFootprintDF_ICG)
print(LandFootprintDF_ICG)
print(MaterialFootprintDF_ICG)
print(WaterFootprintDF_ICG)

Mylog.info('<p>Carbon Footprint per Income Group<br>')

# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(Path_Result + 'Footprint Detail Per Income Group_Scenario.xlsx', engine='xlsxwriter')

# Convert the dataframe to an XlsxWriter Excel object.
CarbonFootprintDF_ICG.to_excel(writer, sheet_name='CF_Detail')
LandFootprintDF_ICG.to_excel(writer, sheet_name='LF_Detail')
MaterialFootprintDF_ICG.to_excel(writer, sheet_name='MF_Detail')
WaterFootprintDF_ICG.to_excel(writer, sheet_name='WF_Detail')

# Close the Pandas Excel writer and output the Excel file.
writer.save()

#%%

###########################
# 20. Step: Finish script #
###########################                   
Mylog.info('<br> Script is finished. Terminating logging process and closing all log files.<br>')
Time_End = time.time()
Time_Duration = Time_End - Time_Start
Mylog.info('<font "size=+4"> <b>End of simulation: ' + time.asctime() + '.</b></font><br>')
Mylog.info('<font "size=+4"> <b>Duration of simulation: %.1f seconds.</b></font><br>' % Time_Duration)
logging.shutdown()
# remove all handlers from logger
root = logging.getLogger()
root.handlers = [] # required if you don't want to exit the shell
# The End