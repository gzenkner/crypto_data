import pandas as pd
import plotly.graph_objects as go


class CoinAnalyzer():
    def __init__(self):

        pass

    def resample_timeseries(self, df, period='ME', column='Close'):
        """Y, M, W, D or YS or YE etc
        Must be of format : | value | ticker
        """    
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date')
        df.set_index('Date', inplace=True)
        all_tickers = df['index'].unique()
        monthly_close_all = pd.DataFrame()

        for ticker in all_tickers:
            filtered_df = df[df['index'] == ticker]
            monthly_close = filtered_df[column].resample(period).mean()
            monthly_close = monthly_close.rename(ticker)
            monthly_close.index = monthly_close.index.strftime('%Y-%m-%d')
            monthly_close_all = pd.concat([monthly_close_all, monthly_close], axis=1)
        
        return monthly_close_all

    def cross_correlation_matrix(self, df, method='pearson'):
        "method : {'pearson', 'kendall', 'spearman'} or callable"
        df = df.sort_index()
        return df.corr(method)
    

    def candlestick_plot(df):
        
        fig = go.Figure(
            data = [  
                go.Candlestick(
                    x=df.index.get_level_values('timestamp'),
                    open=df['open'],
                    high=df['high'],
                    low=df['low'],
                    close=df['close']
                )
                ]
            )

        fig.show()