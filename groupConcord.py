#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 16 08:44:21 2017

@author: tybodsm
"""

import pandas as pd


def groupConcord(df, cols = None, pntCnt = False):
    '''
    
    Given two columns of many-to-many relationships, creates a m:1
    relationship from each column to a new, grouped code.
    
    Parameters
    ----------
    df : pandas DataFrame object
        The dataframe containing the columns you want mapped together,
        each row representing a mapping from one variable to the other.
    
    cols : list, default None
        List of column names for grouping. Currently can only contain 2
        columns. If blank, assumes the first 2 columns of the DataFrame.

    pntCnt : Boolean, default False
        If True, prints the iteration of the groupby function and the number 
        of observations still ungrouped and that as a fraction of the total.
        
        
    Examples
    --------
    >>> df = pd.DataFrame([[1,2,8],[1,3,7],[1,3,9],[1,4,5],[2,12,0],[2,14,0],[3,3,0],[4,20,0]],columns= ['a','b','c'])
    >>> groupConcord(df, cols = ['a','b'])
    
    Out:
           a   b  groupID
        0  1   2        1
        1  1   3        1
        2  1   4        1
        3  2  12        2
        4  2  14        2
        5  3   3        1
        6  4  20        4
    
    Returns
    -------
    pandas DataFrame object
    
    '''
    #Prepare the set and check inputs
    if cols == None:
        cols = list(df.columns[0:2])
        
    if not isinstance(df,pd.DataFrame):
        raise TypeError("Only takes a pandas dataframe")
        
    if len(cols) > 2:
        raise ValueError("Can only group 2 columns")
    

    
    #get unique mappings and gen the initial groupings
    dfu = df[cols].drop_duplicates().reset_index(drop = True).copy()
    dfu['groupID'] = dfu[cols[0]].rank(method = 'dense').apply(int)
    
    #indicator for if the group is finished
    dfu['notDone'] =  _notGrouped(dfu, cols)
    
    if pntCnt:
        itr = _pntCnt(1, dfu)
    
    #Iterate over the grouping process for the subset of non-grouped
    while dfu.notDone.any():
        dfr = dfu[dfu['notDone']].copy()
        
        dfr['groupID'] = dfr.groupby(cols[1]).groupID.transform('min')
        dfr['groupID'] = dfr.groupby(cols[0]).groupID.transform('min')
        
        dfu['notDone'].update(_notGrouped(dfr, cols))
        dfu['groupID'].update(dfr['groupID'])
        
        if pntCnt:
            itr = _pntCnt(itr, dfu)
    
    return dfu[(cols+['groupID'])].copy()
    
    
    
def _notGrouped(df, cols):
    ''' Produces a column indicating whether a code is finished being grouped '''
    out =   (
                (df
                     .groupby(cols[1])
                     .groupID
                     .transform('nunique') > 1.0
                )
                .groupby(df['groupID'])
                .transform('max')
            )
    return out

def _pntCnt(itr, df):
    ''' Prints info on the grouping process '''
    ug = df['notDone'].sum()
    ug_frac = df['notDone'].sum() / df.shape[0]
    
    print(
            'iteration: {}'.format(itr),
            'ungrouped rows: {}'.format(ug),
            'ungrouped fraction: {:.1%}'.format(ug_frac),
            '',
        sep = '\n')
    return (itr + 1)
    