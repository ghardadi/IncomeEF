# -*- coding: utf-8 -*-
"""
Created on Mon Jun 30 19:23:50 2014

@author: pauliuk
"""

"""
Define logging module and functions
"""

import os
import logging
import numpy as np
import pandas as pd

def function_logger(file_level, Name_Scenario, Path_Result, console_level):
    # remove all handlers from logger
    logger = logging.getLogger()
    logger.handlers = [] # required if you don't want to exit the shell
    logger.setLevel(logging.DEBUG) #By default, logs all messages

    if console_level != None:
        console_log = logging.StreamHandler() #StreamHandler logs to console
        console_log.setLevel(console_level)
        console_log_format = logging.Formatter('%(message)s') # ('%(asctime)s - %(message)s')
        console_log.setFormatter(console_log_format)
        logger.addHandler(console_log)

    file_log = logging.FileHandler(Path_Result + '\\' + Name_Scenario + '.html', mode='w', encoding=None, delay=False)
    file_log.setLevel(file_level)
    #file_log_format = logging.Formatter('%(asctime)s - %(lineno)d - %(levelname)-8s - %(message)s<br>')
    file_log_format = logging.Formatter('%(message)s<br>')
    file_log.setFormatter(file_log_format)
    logger.addHandler(file_log)

    return logger, console_log, file_log
    
"""
Other functions
"""
    
def ensure_dir(f): # Checks whether a given directory f exists, and creates it if not
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)         
    
def string_truncate(String, Separator): # Returns substring between leftmost and rightmost occurence of the separator
    if str.find(String,Separator) == -1:
        result = ''
    else:
        result = String[str.find(String,Separator)+1:str.rfind(String,Separator)]
    return result
    
def Build_Aggregation_Matrix_samesize(Position_Vector): # matrix that re-arranges rows of the table it is multiplied to from the left (or columns, if multiplied transposed from the right)
    AM_length = Position_Vector.max() + 1 # Maximum row number of new matrix (+1 to get the right length)
    AM_width  = len(Position_Vector) # Number of rows of the to-be-aggregated matrix
    Rearrange_Matrix = np.zeros((AM_length,AM_width))
    for m in range(0,len(Position_Vector)):
        Rearrange_Matrix[Position_Vector[m].item(0),m] = 1 # place 1 in aggregation matrix at [PositionVector[m],m], so that column m is aggregated with Positionvector[m] in the aggregated matrix
    return Rearrange_Matrix

def Build_Aggregation_Matrix_newsize(Position_Vector,newrows,newcols): # matrix that re-arranges rows of the table it is multiplied to from the left (or columns, if multiplied transposed from the right)
    Rearrange_Matrix = np.zeros((newrows,newcols)) # new size is given
    for m in range(0,len(Position_Vector)):
        Rearrange_Matrix[Position_Vector[m].item(0),m] = 1 # place 1 in aggregation matrix at [PositionVector[m],m], so that column m is aggregated with Positionvector[m] in the aggregated matrix
    return Rearrange_Matrix    
    
def Define_Boynton_Optimized_Colour_Scheme():
    BoyntonOpt=np.array([[0,0,255,255],
                [255,0,0,255],
                [0,255,0,255],
                [255,255,0,255],
                [255,0,255,255],
                [255,128,128,255],
                [128,128,128,255],
                [128,0,0,255],
                [255,128,0,255],]) / 255
    return BoyntonOpt    
 
def Aggregation_read(file, sheetname):
    """
    Read the aggregation excel file and build the aggregation matrix. The excel
    file shall have one aggregation scheme per sheet and have size:
            original number of sector x new number of sector
    """
    df = pd.io.excel.read_excel(file, sheetname=sheetname, header=0, index_col=0)
    Aggregation_Matrix = np.asarray(df)
    Aggregation_List = df.columns.values.tolist()
    for w1 in range(np.shape(Aggregation_Matrix)[0]):
        for w2 in range(np.shape(Aggregation_Matrix)[1]):
            if np.isnan(Aggregation_Matrix[w1,w2]) == True:
                Aggregation_Matrix[w1,w2] = 0
            else:
                pass
    return Aggregation_Matrix, Aggregation_List
    
def Projection_matrix(Agg_mat, nb_of_regions, Type='Multi'):
    """
    Build a projection matrix that sorts/aggregates a Multi-regional table.
    If type is 'One' it reproduces Agg_mat according to nb_of_region on top of each other
    If type is 'Multi' it reproduces Agg_mat on the diagonal of a larger matrix of zeros as long as nb_of_regions
    """    
    if Type == 'One':
        Projector = np.concatenate([Agg_mat for i in range(nb_of_regions)], axis=0)
    elif Type == 'Multi':
        h, w = Agg_mat.shape[0], Agg_mat.shape[1]
        blank = np.zeros((h*nb_of_regions, w*nb_of_regions))
        for r1 in range(nb_of_regions):
            for r2 in range(nb_of_regions):
                if r1==r2:
                    blank[r1*h:h*(r1+1),r2*w:w*(r2+1)] = Agg_mat
        Projector = blank
    return Projector    
#
#
#
# End of file