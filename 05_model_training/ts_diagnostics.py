# rolling window diagnostics
# modified from facebook prophet to be used with sklearn: 
# https://github.com/facebook/prophet/blob/main/python/prophet/diagnostics.py

from tqdm.auto import tqdm
from copy import deepcopy
import concurrent.futures

import numpy as np
import pandas as pd


def generate_cutoffs(X, horizon, initial, period):
    """Generate cutoff dates
    Parameters
    ----------
    df: pd.DataFrame with historical data.
    horizon: pd.Timedelta forecast horizon.
    initial: pd.Timedelta window of the initial forecast period.
    period: pd.Timedelta simulated forecasts are done with this period.
    Returns
    -------
    list of pd.Timestamp
    """
    # Last cutoff is 'latest date in data - horizon' date
    cutoff = X.index.max() - horizon
    if cutoff < X.index.min():
        raise ValueError('Less data than horizon.')
    result = [cutoff]
    while result[-1] >= min(X.index) + initial:
        cutoff -= period
        # If data does not exist in data range (cutoff, cutoff + horizon]
        if not (((X.index > cutoff) & (X.index <= cutoff + horizon)).any()):
            # Next cutoff point is 'last date before cutoff in data - horizon'
            if cutoff > X.index.min():
                closest_date = X[X.index <= cutoff].max().index
                cutoff = closest_date - horizon
            # else no data left, leave cutoff as is, it will be dropped.
        result.append(cutoff)
    result = result[:-1]
    if len(result) == 0:
        raise ValueError(
            'Less data than horizon after initial window. '
            'Make horizon or initial shorter.'
        )
    # logger.info('Making {} forecasts with cutoffs between {} and {}'.format(
    #     len(result), result[-1], result[0]
    # ))
    print('Making {} forecasts with cutoffs between {} and {}'.format(
        len(result), result[-1], result[0]
    ))
    return list(reversed(result))

def cross_validation(model, X, y, horizon, period=None, initial=None, parallel=None, cutoffs=None, disable_tqdm=False):
    """Cross-Validation for time series.
    Computes forecasts from historical cutoff points, which user can input.
    If not provided, begins from (end - horizon) and works backwards, making
    cutoffs with a spacing of period until initial is reached.
    When period is equal to the time interval of the data, this is the
    technique described in https://robjhyndman.com/hyndsight/tscv/ .
    Parameters
    ----------
    model: Prophet class object. Fitted Prophet model.
    horizon: string with pd.Timedelta compatible style, e.g., '5 days',
        '3 hours', '10 seconds'.
    period: string with pd.Timedelta compatible style. Simulated forecast will
        be done at every this period. If not provided, 0.5 * horizon is used.
    initial: string with pd.Timedelta compatible style. The first training
        period will include at least this much data. If not provided,
        3 * horizon is used.
    cutoffs: list of pd.Timestamp specifying cutoffs to be used during
        cross validation. If not provided, they are generated as described
        above.
    parallel : {None, 'processes', 'threads', 'dask', object}
    disable_tqdm: if True it disables the progress bar that would otherwise show up when parallel=None
        How to parallelize the forecast computation. By default no parallelism
        is used.
        * None : No parallelism.
        * 'processes' : Parallelize with concurrent.futures.ProcessPoolExectuor.
        * 'threads' : Parallelize with concurrent.futures.ThreadPoolExecutor.
            Note that some operations currently hold Python's Global Interpreter
            Lock, so parallelizing with threads may be slower than training
            sequentially.
        * 'dask': Parallelize with Dask.
           This requires that a dask.distributed Client be created.
        * object : Any instance with a `.map` method. This method will
          be called with :func:`single_cutoff_forecast` and a sequence of
          iterables where each element is the tuple of arguments to pass to
          :func:`single_cutoff_forecast`
          .. code-block::
             class MyBackend:
                 def map(self, func, *iterables):
                     results = [
                        func(*args)
                        for args in zip(*iterables)
                     ]
                     return results
    Returns
    -------
    A pd.DataFrame with the forecast, actual value and cutoff.
    """
      
    horizon = pd.Timedelta(horizon)
    period = pd.Timedelta(period)
    initial = pd.Timedelta(initial)
        

    # Compute Cutoffs
    cutoffs = generate_cutoffs(X, horizon, initial, period)
           
    if parallel:
        valid = {"threads", "processes"}

        if parallel == "threads":
            pool = concurrent.futures.ThreadPoolExecutor()
        elif parallel == "processes":
            pool = concurrent.futures.ProcessPoolExecutor()
       
        else:
            msg = ("'parallel' should be one of {} for an instance with a "
                   "'map' method".format(', '.join(valid)))
            raise ValueError(msg)

        iterables = ((df, model, cutoff, horizon, predict_columns)
                     for cutoff in cutoffs)
        iterables = zip(*iterables)

        logger.info("Applying in parallel with %s", pool)
        predicts = pool.map(single_cutoff_forecast, *iterables)
        
    else:
        predicts = [
            single_cutoff_forecast(model, X, y, cutoff, horizon) 
            for cutoff in (tqdm(cutoffs) if not disable_tqdm else cutoffs)
        ]

    return pd.concat(predicts, axis=0).reset_index(drop=True)

def single_cutoff_forecast(model, X, y, cutoff, horizon):
    """Forecast for single cutoff. Used in cross validation function
    when evaluating for multiple cutoffs either sequentially or in parallel .
    Parameters
    ----------
    df: pd.DataFrame.
        DataFrame with history to be used for single
        cutoff forecast.
    model: Prophet model object.
    cutoff: pd.Timestamp cutoff date.
        Simulated Forecast will start from this date.
    horizon: pd.Timedelta forecast horizon.
    predict_columns: List of strings e.g. ['ds', 'yhat'].
        Columns with date and forecast to be returned in output.
    Returns
    -------
    A pd.DataFrame with the forecast, actual value and cutoff.
    """

    # Train model
    history_c = X[X.index <= cutoff]
    history_c_y = y[y.index <= cutoff]
    if history_c.shape[0] < 2:
        raise Exception(
            'Less than two datapoints before cutoff. '
            'Increase initial window.')
        
    model.fit(history_c, history_c_y)
    
    # Calculate yhat
    index_predicted = (X.index > cutoff) & (X.index <= cutoff + horizon)
    # Get the columns for the future dataframe
    
   
    yhat = model.predict(X[index_predicted])
    

    # Merge yhat(predicts), y(X, original data) and cutoff
    return pd.concat([
        X[index_predicted],
        pd.DataFrame({'ds':X[index_predicted].index, 'y': y[index_predicted], 'yhat': yhat, 'cutoff': [cutoff] * len(yhat)})
    ], axis=1)

def performance_metrics(df, metrics=None, rolling_window=0.1, monthly=False):
    """Compute performance metrics from cross-validation results.
    Computes a suite of performance metrics on the output of cross-validation.
    By default the following metrics are included:
    'mse': mean squared error
    'rmse': root mean squared error
    'mae': mean absolute error
    'mape': mean absolute percent error
    'mdape': median absolute percent error
    'smape': symmetric mean absolute percentage error
    'coverage': coverage of the upper and lower intervals
    A subset of these can be specified by passing a list of names as the
    `metrics` argument.
    Metrics are calculated over a rolling window of cross validation
    predictions, after sorting by horizon. Averaging is first done within each
    value of horizon, and then across horizons as needed to reach the window
    size. The size of that window (number of simulated forecast points) is
    determined by the rolling_window argument, which specifies a proportion of
    simulated forecast points to include in each window. rolling_window=0 will
    compute it separately for each horizon. The default of rolling_window=0.1
    will use 10% of the rows in df in each window. rolling_window=1 will
    compute the metric across all simulated forecast points. The results are
    set to the right edge of the window.
    If rolling_window < 0, then metrics are computed at each datapoint with no
    averaging (i.e., 'mse' will actually be squared error with no mean).
    The output is a dataframe containing column 'horizon' along with columns
    for each of the metrics computed.
    Parameters
    ----------
    df: The dataframe returned by cross_validation.
    metrics: A list of performance metrics to compute. If not provided, will
        use ['mse', 'rmse', 'mae', 'mape', 'mdape', 'smape', 'coverage'].
    rolling_window: Proportion of data to use in each rolling window for
        computing the metrics. Should be in [0, 1] to average.
    monthly: monthly=True will compute horizons as numbers of calendar months 
        from the cutoff date, starting from 0 for the cutoff month.
    Returns
    -------
    Dataframe with a column for each metric, and column 'horizon'
    """
    valid_metrics = ['mse', 'rmse', 'mae', 'mape', 'mdape', 'smape', 'coverage']
    if metrics is None:
        metrics = valid_metrics
    if ('yhat_lower' not in df or 'yhat_upper' not in df) and ('coverage' in metrics):
        metrics.remove('coverage')
    if len(set(metrics)) != len(metrics):
        raise ValueError('Input metrics must be a list of unique values')
    if not set(metrics).issubset(set(valid_metrics)):
        raise ValueError(
            'Valid values for metrics are: {}'.format(valid_metrics)
        )
    df_m = df.copy()
    if monthly:
        df_m['horizon'] = df_m['ds'].dt.to_period('M').astype(int) - df_m['cutoff'].dt.to_period('M').astype(int)
    else:
        df_m['horizon'] = df_m['ds'] - df_m['cutoff']
    df_m.sort_values('horizon', inplace=True)
    if 'mape' in metrics and df_m['y'].abs().min() < 1e-8:
        logger.info('Skipping MAPE because y close to 0')
        metrics.remove('mape')
    if len(metrics) == 0:
        return None
    w = int(rolling_window * df_m.shape[0])
    if w >= 0:
        w = max(w, 1)
        w = min(w, df_m.shape[0])
    # Compute all metrics
    dfs = {}
    for metric in metrics:
        dfs[metric] = eval(metric)(df_m, w)
    res = dfs[metrics[0]]
    for i in range(1, len(metrics)):
        res_m = dfs[metrics[i]]
        assert np.array_equal(res['horizon'].values, res_m['horizon'].values)
        res[metrics[i]] = res_m[metrics[i]]
    return res


def rolling_mean_by_h(x, h, w, name):
    """Compute a rolling mean of x, after first aggregating by h.
    Right-aligned. Computes a single mean for each unique value of h. Each
    mean is over at least w samples.
    Parameters
    ----------
    x: Array.
    h: Array of horizon for each value in x.
    w: Integer window size (number of elements).
    name: Name for metric in result dataframe
    Returns
    -------
    Dataframe with columns horizon and name, the rolling mean of x.
    """
    # Aggregate over h
    df = pd.DataFrame({'x': x, 'h': h})
    df2 = (
        df.groupby('h').agg(['sum', 'count']).reset_index().sort_values('h')
    )
    xs = df2['x']['sum'].values
    ns = df2['x']['count'].values
    hs = df2.h.values

    trailing_i = len(df2) - 1
    x_sum = 0
    n_sum = 0
    # We don't know output size but it is bounded by len(df2)
    res_x = np.empty(len(df2))

    # Start from the right and work backwards
    for i in range(len(df2) - 1, -1, -1):
        x_sum += xs[i]
        n_sum += ns[i]
        while n_sum >= w:
            # Include points from the previous horizon. All of them if still
            # less than w, otherwise weight the mean by the difference
            excess_n = n_sum - w
            excess_x = excess_n * xs[i] / ns[i]
            res_x[trailing_i] = (x_sum - excess_x)/ w
            x_sum -= xs[trailing_i]
            n_sum -= ns[trailing_i]
            trailing_i -= 1

    res_h = hs[(trailing_i + 1):]
    res_x = res_x[(trailing_i + 1):]

    return pd.DataFrame({'horizon': res_h, name: res_x})
    


def rolling_median_by_h(x, h, w, name):
    """Compute a rolling median of x, after first aggregating by h.
    Right-aligned. Computes a single median for each unique value of h. Each
    median is over at least w samples.
    For each h where there are fewer than w samples, we take samples from the previous h,
    moving backwards. (In other words, we ~ assume that the x's are shuffled within each h.)
    Parameters
    ----------
    x: Array.
    h: Array of horizon for each value in x.
    w: Integer window size (number of elements).
    name: Name for metric in result dataframe
    Returns
    -------
    Dataframe with columns horizon and name, the rolling median of x.
    """
    # Aggregate over h
    df = pd.DataFrame({'x': x, 'h': h})
    grouped = df.groupby('h')
    df2 = grouped.size().reset_index().sort_values('h')
    hs = df2['h']

    res_h = []
    res_x = []
    # Start from the right and work backwards
    i = len(hs) - 1
    while i >= 0:
        h_i = hs[i]
        xs = grouped.get_group(h_i).x.tolist()

        # wrap in array so this works if h is pandas Series with custom index or numpy array
        next_idx_to_add = np.array(h == h_i).argmax() - 1
        while (len(xs) < w) and (next_idx_to_add >= 0):
            # Include points from the previous horizon. All of them if still
            # less than w, otherwise just enough to get to w.
            xs.append(x[next_idx_to_add])
            next_idx_to_add -= 1
        if len(xs) < w:
            # Ran out of points before getting enough.
            break
        res_h.append(hs[i])
        res_x.append(np.median(xs))
        i -= 1
    res_h.reverse()
    res_x.reverse()
    return pd.DataFrame({'horizon': res_h, name: res_x})


# The functions below specify performance metrics for cross-validation results.
# Each takes as input the output of cross_validation, and returns the statistic
# as a dataframe, given a window size for rolling aggregation.


def mse(df, w):
    """Mean squared error
    Parameters
    ----------
    df: Cross-validation results dataframe.
    w: Aggregation window size.
    Returns
    -------
    Dataframe with columns horizon and mse.
    """
    se = (df['y'] - df['yhat']) ** 2
    if w < 0:
        return pd.DataFrame({'horizon': df['horizon'], 'mse': se})
    return rolling_mean_by_h(
        x=se.values, h=df['horizon'].values, w=w, name='mse'
    )


def rmse(df, w):
    """Root mean squared error
    Parameters
    ----------
    df: Cross-validation results dataframe.
    w: Aggregation window size.
    Returns
    -------
    Dataframe with columns horizon and rmse.
    """
    res = mse(df, w)
    res['mse'] = np.sqrt(res['mse'])
    res.rename({'mse': 'rmse'}, axis='columns', inplace=True)
    return res


def mae(df, w):
    """Mean absolute error
    Parameters
    ----------
    df: Cross-validation results dataframe.
    w: Aggregation window size.
    Returns
    -------
    Dataframe with columns horizon and mae.
    """
    ae = np.abs(df['y'] - df['yhat'])
    if w < 0:
        return pd.DataFrame({'horizon': df['horizon'], 'mae': ae})
    return rolling_mean_by_h(
        x=ae.values, h=df['horizon'].values, w=w, name='mae'
    )


def mape(df, w):
    """Mean absolute percent error
    Parameters
    ----------
    df: Cross-validation results dataframe.
    w: Aggregation window size.
    Returns
    -------
    Dataframe with columns horizon and mape.
    """
    ape = np.abs((df['y'] - df['yhat']) / df['y'])
    if w < 0:
        return pd.DataFrame({'horizon': df['horizon'], 'mape': ape})
    return rolling_mean_by_h(
        x=ape.values, h=df['horizon'].values, w=w, name='mape'
    )


def mdape(df, w):
    """Median absolute percent error
    Parameters
    ----------
    df: Cross-validation results dataframe.
    w: Aggregation window size.
    Returns
    -------
    Dataframe with columns horizon and mdape.
    """
    ape = np.abs((df['y'] - df['yhat']) / df['y'])
    if w < 0:
        return pd.DataFrame({'horizon': df['horizon'], 'mdape': ape})
    return rolling_median_by_h(
        x=ape.values, h=df['horizon'], w=w, name='mdape'
    )


def smape(df, w):
    """Symmetric mean absolute percentage error
    based on Chen and Yang (2004) formula
    Parameters
    ----------
    df: Cross-validation results dataframe.
    w: Aggregation window size.
    Returns
    -------
    Dataframe with columns horizon and smape.
    """
    sape = np.abs(df['y'] - df['yhat']) / ((np.abs(df['y']) + np.abs(df['yhat'])) / 2)
    if w < 0:
        return pd.DataFrame({'horizon': df['horizon'], 'smape': sape})
    return rolling_mean_by_h(
        x=sape.values, h=df['horizon'].values, w=w, name='smape'
    )


def coverage(df, w):
    """Coverage
    Parameters
    ----------
    df: Cross-validation results dataframe.
    w: Aggregation window size.
    Returns
    -------
    Dataframe with columns horizon and coverage.
    """
    is_covered = (df['y'] >= df['yhat_lower']) & (df['y'] <= df['yhat_upper'])
    if w < 0:
        return pd.DataFrame({'horizon': df['horizon'], 'coverage': is_covered})
    return rolling_mean_by_h(
        x=is_covered.values, h=df['horizon'].values, w=w, name='coverage'
    )