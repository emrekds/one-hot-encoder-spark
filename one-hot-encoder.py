class OneHotEncoder:
    def __init__(self):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

    def fit_transform(self, dataframe, drop_last=True, drop_categoric=True):
        '''
        Find categoric columns from given dataframe.
        Return one hot encoded pyspark dataframe.
        
        Parameters
            dataframe : pyspark dataframe
            drop_last : bool
                Drops last element of categorical column.
            drop_categorical : bool
                Drops categorical columns after encoding.
                
        '''
        df = dataframe
        categoric_columns = [col[0] for col in df.dtypes if col[1].startswith('str')]
        for i in categoric_columns:
            uniques = list(df.select(i).distinct().toPandas()[i].unique())
            if drop_last:
                uniques = uniques[:-1]
            for j in uniques:
                new_column_ = when(col(i) == j, 1).otherwise(0)
                df = df.withColumn(f'{i}_{j}', new_column_)
            if drop_categoric:
                df = df.drop(i)
        return df
