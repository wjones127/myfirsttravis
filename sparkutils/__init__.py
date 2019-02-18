import pyspark.sql.functions as F

def col_min(df, col):
    '''Get the minimum value of a column

    Parameters
    --------------
    df : pyspark.sql.DataFrame
        The dataframe containing the column
    col : pyspark.sql.Column or str
        The column or name of column to get min of 
    '''
    return df.select(F.min(col)).collect()[0][0]

