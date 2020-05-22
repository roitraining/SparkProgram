# you should make sure you have spark in your python path as below
# export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
# but if you don't it will append it automatically for this session

import platform, os, sys
from os.path import dirname

sys.path.append('/home/student/ROI/Spark')

if not 'SPARK_HOME' in os.environ and not os.environ['SPARK_HOME'] in sys.path:
    sys.path.append(os.environ['SPARK_HOME']+'/python')

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *

def initspark(appname = "Test", servername = "local", cassandra="127.0.0.1", mongo="mongodb://127.0.0.1/classroom"):
    print ('initializing pyspark')
    conf = SparkConf().set("spark.cassandra.connection.host", cassandra).setAppName(appname).setMaster(servername)
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName(appname) \
    .config("spark.mongodb.input.uri", mongo) \
    .config("spark.mongodb.output.uri", mongo) \
    .enableHiveSupport().getOrCreate()
    sc.setLogLevel("ERROR")
    print ('pyspark initialized')
    return sc, spark, conf

def display(df, limit = 10):
    from IPython.display import display    
    display(df.limit(limit).toPandas())

def drop_columns(df, collist):
    return df.select([c for c in df.columns if c not in collist])

def auto_numeric_features(df, exceptlist = ()):
    numeric_features = [t[0] for t in df.dtypes if t[0] not in exceptlist and t[1] in ['int', 'double']]
    return numeric_features

def auto_categorical_features(df, suffix = ('ID', 'CODE', 'FLAG'), exceptlist = ()):
    if suffix:
        categorical_features = [c for c in df.columns if c not in exceptlist and c.upper().endswith(tuple(map(str.upper, suffix)))]
    else:
        categorical_features = [t[0] for t in df.dtypes if t[0] not in exceptlist and t[1] not in ['int', 'double']]

    return categorical_features

def describe_numeric_features(df, numeric_features):
    return df.select(numeric_features).describe()

def scatter_matrix(df, numeric_features):
    import pandas as pd
    numeric_data = df.select(numeric_features).toPandas()
    axs = pd.plotting.scatter_matrix(numeric_data, figsize=(8, 8));
    n = len(numeric_data.columns)
    for i in range(n):
        v = axs[i, 0]
        v.yaxis.label.set_rotation(0)
        v.yaxis.label.set_ha('right')
        v.set_yticks(())
        h = axs[n-1, i]
        h.xaxis.label.set_rotation(90)
        h.set_xticks(())

def beta_coefficients(model):
    import matplotlib.pyplot as plt
    import numpy as np
    beta = np.sort(model.coefficients)
    plt.plot(beta)
    plt.ylabel('Beta Coefficients')
    plt.show()

def roc_curve(model):
    from matplotlib import pyplot as plt
    summary = model.summary
    roc = summary.roc.toPandas()
    plt.plot(roc['FPR'],roc['TPR'])
    plt.ylabel('False Positive Rate')
    plt.xlabel('True Positive Rate')
    plt.title('ROC Curve')
    plt.show()
    print('Training set area Under ROC: {}'.format(summary.areaUnderROC))

def precision_recall(model):
    from matplotlib import pyplot as plt
    summary = model.summary
    pr = summary.pr.toPandas()
    plt.plot(pr['recall'],pr['precision'])
    plt.ylabel('Precision')
    plt.xlabel('Recall')
    plt.show()

def evaluate_ROC(predictions):
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    evaluator = BinaryClassificationEvaluator()
    return evaluator.evaluate(predictions)

def show_predictions(predictions, limit = 20):
    print('Test Area Under ROC {}'.format(evaluate_ROC(predictions)))
    predictions.select('label', 'rawPrediction', 'prediction', 'probability').show(limit)
    predictions.groupBy('prediction').count().show()

def collect_tuple(df):
    return [tuple(row) if len(tuple(row)) > 1 else tuple(row)[0] for row in df.collect()]

def collect_dict(df):
    return dict(collect_tuple(df))

def pretty_confusion(cm:'np.array', include_header = True, include_percent = False, commas = True):
    import numpy as np
    # find the length of the largest number for proper spacing
    digits = max(4, len(max(cm.reshape(np.size(cm, 0) ** 2, ).astype(str), key = len)))
    # if commas are supposed to be displayed, add extra space for them
    
    commadigits, commas = (digits // 3, ',') if commas else (0, '')
    digits += commadigits
        
    # add the row and column totals to the matrix
    cm = cm.astype(int)
    cm1 = np.concatenate((cm, cm.sum(axis = 1).reshape(np.size(cm, 0),1)), axis = 1)
    cm2 = np.concatenate((cm1, cm1.sum(axis = 0).reshape(1,np.size(cm1, 1))), axis = 0)
    size = np.size(cm, 0)
    
    # calculate the total width to repeat the underlines
    width = (digits + 3) * (size + 1) - 3

    if include_header:
        ret = '  +' + (width + 1) * '-' + '+\nA | ' + 'Predicted'.center(width) + '|\n'
        ret += 'c +' + '-' * width + '-+\n'
    else:
        ret = ''
    actual = 'tual' + ' ' * 100

    # build the format string for each line of the matrix
    for r in range(size + 1):
        if include_header:
            ret += actual[0] + ' |'
            actual = actual[1:]
        if r == size:
            ret += '=' * width + '=' + ('|' if include_header else '') + '\n' + (actual[0] + ' |' if include_header else '')
        ret += ('{:>' + str(digits) + commas + 'd} + ') * (size - 1) + \
                '{:>' + str(digits) + commas + 'd} = {:' + str(digits) + commas + 'd} '+ ('|\n' if include_header else '\n')
    if include_header:
        ret += '  +' + ((width + 1) * '-') + '+'

    # take the matrix, flatten it and unpack it into the placeholders of the format string
    ret = ret.format(*(cm2.reshape(len(cm2) ** 2)))
    
    # if the percentages are desired add that as a second return value
    if include_percent and size == 2:
        import numpy as np
        length = cm2[2,2]
        ret2 = f'''
  +-----------------------------------+
  | % Correct / % FP | % FN / % Wrong |
  |-----------------------------------|
  |          {100 *(cm[0][0] + cm[1][1])/length:>5.2f}   |        {100 * cm[0][1]/length:>5.2f}   |
  |          {100 * cm[1][0]/length:>5.2f}   |        {100 * (cm[1][0] + cm[0][1])/length:>5.2f}   |
  +-----------------------------------+
    '''    
        return (ret, ret2)

    return ret
# end pretty_confusion

#def cm_percent(cm, length, legend = True):
#    import numpy as np
#    x = np.ndarray(shape = (2,2), \
#                      buffer = np.array([100 *(cm[0][0] + cm[1][1])/length, \
#                      100 * cm[0][1]/length, 100 * cm[1][0]/length, \
#                      100 * (cm[1][0] + cm[0][1])/length]))
#    return x

def evaluate_model(model):
    try:
        beta_coefficients(model)
    except:
        pass
    try:    
        roc_curve(model)
    except:
        pass
    try:
        precision_recall(model)    
    except:
        pass

def evaluate_predictions(predictions, show = True):
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.mllib.evaluation import BinaryClassificationMetrics, MulticlassMetrics
    log = {}

    evaluator = BinaryClassificationEvaluator(metricName='areaUnderROC')
    log['auroc'] = evaluator.evaluate(predictions)  
    
    # Show Validation Score (AUPR)
    evaluator = BinaryClassificationEvaluator(metricName='areaUnderPR')
    log['aupr'] = evaluator.evaluate(predictions)

    # Metrics
    predictionRDD = predictions.select(['label', 'prediction']).rdd.map(lambda line: (line[1], line[0]))
    metrics = MulticlassMetrics(predictionRDD)
    

    # Overall statistics
    log['precision'] = metrics.precision()
    log['recall'] = metrics.recall()
    log['F1 Measure'] = metrics.fMeasure()
    
    # Statistics by class
    distinctPredictions = collect_tuple(predictions.select('prediction').distinct())
    for x in sorted(distinctPredictions):
        log[x] = {}
        log[x]['precision'] = metrics.precision(x)
        log[x]['recall'] = metrics.recall(x)
        log[x]['F1 Measure'] = metrics.fMeasure(x, beta = 1.0)

    # Confusion Matrix
    # log['cm'] = metrics.confusionMatrix().toArray()
    # log['cmpercent'] = cm_percent(log['cm'], predictions.count(), show)
    #log['cm'], log['cmpercent']
    log['cm'] = pretty_confusion(metrics.confusionMatrix().toArray(), include_percent = True)
    log['cmpercent'] = ''
    if type(x) is tuple:
        log['cm'], log['cmpercent'] = x

    if show:
        show_predictions(predictions)

        print('Confusion Matrix')
        #print (' TP', 'FN\n', 'FP', 'TN')
        print(log['cm'])
        #print (' PC', 'FN\n', 'FP', 'PW')
        print()
        print(log['cmpercent'])
        print ('')
        print("Area under ROC = {}".format(log['auroc']))
        print("Area under AUPR = {}".format(log['aupr']))
        print('\nOverall\ntprecision = {}\nrecall = {}\nF1 Measure = {}\n'.format( 
              log['precision'], log['recall'], log['F1 Measure']))

        for x in sorted(distinctPredictions):
            print('Label {}\ntprecision = {}\nrecall = {}\nF1 Measure = {}\n'.format( 
                  x, log[x]['precision'], log[x]['recall'], log[x]['F1 Measure']))

    return log    

def predict_and_evaluate(model, test, show = True, showModel = True):
    predictions = model.transform(test)
    if showModel:
        evaluate_model(model)
    log = evaluate_predictions(predictions, show)
    return (predictions, log)

def StringIndexEncode(df, columns, return_key_dict = False):
    from pyspark.ml.feature import StringIndexer
    df1 = df
    keydict = {}
    for col in columns:
        indexer = StringIndexer(inputCol = col, outputCol = col+'_Index')
        df1 = indexer.fit(df1).transform(df1)
        if return_key_dict:
           keydict[col] = dict(list(map(tuple, df1.select(col, col + '_Index').distinct().collect())))
        df1 = df1.drop(col) 

    if return_key_dict and len(keydict) > 0:
       return df1, keydict
    return df1

def OneHotEncode(df, columns):
    from pyspark.ml.feature import OneHotEncoderEstimator
    df1 = df
    for col in columns:
        encoder = OneHotEncoderEstimator(inputCols=[col + '_Index'], outputCols=[col+'_Vector'])
        df1 = encoder.fit(df1).transform(df1).drop(col + '_Index')
    print('---> ', type(df1[col+'_Vector']), df1.columns, df1.take(1))
    return df1

def AssembleFeatures(df, categorical_features, numeric_features, target_label = None, target_is_categorical = True):
    from pyspark.ml.feature import VectorAssembler

    assemblerInputs = [c + "_Vector" for c in categorical_features] + numeric_features
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
    if target_label:
        return assembler.transform(df).withColumnRenamed(target_label, 'label' if target_is_categorical else 'target').drop(*(numeric_features + [c + '_Vector' for c in categorical_features]))
    return assembler.transform(df).drop(*(numeric_features + [c + '_Vector' for c in categorical_features]))
    
def MakeMLDataFrame(df, categorical_features, numeric_features, target_label = None, target_is_categorical = True, return_key_dict = False):
    if target_label:
       df0 = df.select(categorical_features + numeric_features + [target_label])
       if target_is_categorical: 
           df1 = StringIndexEncode(df0, categorical_features + [target_label], return_key_dict = return_key_dict)
           if type(df1) is tuple:
               keydict = df1[1]
               df1 = df1[0]
           df2 = OneHotEncode(df1, categorical_features)
           df3 = AssembleFeatures(df2, categorical_features, numeric_features, target_label + '_Index', target_is_categorical = True) #.select('features', 'label')
       else:
           df1 = StringIndexEncode(df0, categorical_features, return_key_dict = return_key_dict)
           if type(df1) is tuple:
               keydict = df1[1]
               df1 = df1[0]
           df2 = OneHotEncode(df1, categorical_features)
           df3 = AssembleFeatures(df2, categorical_features, numeric_features, target_label, target_is_categorical = False).select('features', 'target')
    else:
       df0 = df.select(categorical_features + numeric_features)
       df1 = StringIndexEncode(df0, categorical_features, return_key_dict = return_key_dict)
#       if skip_string_indexer:
#           df1 = df0
#           for col in categorical_features:
#               df1 = df1.withColumnRenamed(col, col + '_Index')
#           print('** df1 columns', df1.columns)
#       else:
       if type(df1) is tuple:
           keydict = df1[1]
           df1 = df1[0]
       df2 = OneHotEncode(df1, categorical_features)
       df3 = AssembleFeatures(df2, categorical_features, numeric_features).select('features')
    if return_key_dict and len(keydict) > 0:
        return (df3, keydict)
    return df3

def MakeMLPipelineStages(df, categorical_features, numeric_features, target_label = None, target_is_categorical = True):
    from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, StringIndexerModel

    stages = []

    for c in categorical_features:
        stringIndexer = StringIndexer(inputCol = c, outputCol = c + '_Index')
        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[c + "_classVec"])
        stages += [stringIndexer, encoder]
        
    if target_is_categorical:
        label_stringIdx = StringIndexer(inputCol = target_label, outputCol = 'label')
        stages += [label_stringIdx]

    assemblerInputs = numeric_features + [c + "_classVec" for c in categorical_features] 
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
    stages += [assembler]
    return stages

def MakeMLPipeline(df, categorical_features, numeric_features, target_label = None, target_is_categorical = True):
    from pyspark.ml import Pipeline
    stages = MakeMLPipelineStages(df, categorical_features, numeric_features, target_label, target_is_categorical)
    return Pipeline(stages = stages)

#def MakeMLPipeline(df, categorical_features, numeric_features, target_label = None, target_is_categorical = True):
#    from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, StringIndexerModel
#    from pyspark.ml import Pipeline
#
#    stages = []
#
#    for c in categorical_features:
#        stringIndexer = StringIndexer(inputCol = c, outputCol = c + '_Index')
#        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[c + "_classVec"])
#        stages += [stringIndexer, encoder]
#        
#    if target_is_categorical:
#        label_stringIdx = StringIndexer(inputCol = target_label, outputCol = 'label')
#        stages += [label_stringIdx]
#
#    assemblerInputs = numeric_features + [c + "_classVec" for c in categorical_features] 
#    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
#    stages += [assembler]
#
#    pipeline = Pipeline(stages = stages)
#
#    #dfML = pipeline.fit(df).transform(df).select(['label', 'features'])
#    #dfx = dfx.select(['label', 'features'] + cols)
#    #catindexes = {x.getOutputCol() : x.labels for x in dfML.stages if isinstance(x, StringIndexerModel)}
#    return pipeline 

def evaluateCluster(model, df):
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    wssse = model.computeCost(dfML.select('features'))
    print("Within Set Sum of Squared Errors = " + str(wssse))

    evaluator = ClusteringEvaluator()

    predictions = model.transform(df)
    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))

    # Shows the result.
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

def plot_elbow(df, cluster_cnt = 10):
    import matplotlib as mp
    from matplotlib import pyplot as plt
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    import numpy as np
    CLUSTERS = range(2, cluster_cnt)
    scores = [KMeans().setK(c).setSeed(1).fit(df).computeCost(df)
              for c in CLUSTERS]
    print(scores)
    plt.plot(CLUSTERS, scores)
    plt.xlabel('Number of Clusters')
    plt.ylabel('Score')
    plt.title('Elbow Curve')
    plt.xticks(np.arange(2, cluster_cnt))


if __name__ == '__main__':
    sc, spark, conf = initspark()

