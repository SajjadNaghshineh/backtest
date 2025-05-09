import pandas as pd

def backtest(df, symbol, start_session, end_session, balance, risk, rr, commision, periodic_result="yearly", multi_time_frame=False, lower_timeframe=None, forward_candle=1, start_period=None, end_period=None, result_to_csv=False, path=None):
    
    symbols = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'EURGBP', 'EURAUD', 'EURNZD', 'EURCAD', 'EURCHF', 'GBPNZD', 'GBPAUD', 'GBPCAD', 'GBPCHF', 'CADCHF', 'NZDCAD', 'AUDCAD', 'AUDNZD', 'AUDCHF']

    if symbol.upper() in symbols:
        pip_value = 100000
    elif "JPY" in symbol.upper():
        pip_value = 1000
    elif "XAUUSD" == symbol.upper():
        pip_value = 100
    else:
        raise ValueError("Symbol is not defined!!!")

    if multi_time_frame:
        df = multi_timeframe(symbol, df, lower_timeframe, start_period, end_period)

    df['status'] = 0
    df['pip_value'] = pip_value
    df['lot_size'] = 0
    df['lot_size'] = df['lot_size'].astype(float)
    
    buy_positions = df[df['signal'] == 1].index.values.tolist()
    sell_positions = df[df['signal'] == -1].index.values.tolist()

    all_positions = buy_positions + sell_positions
    all_positions = sorted(all_positions)

    open_close_positions = []
    count_positions = 0
    
    for i in all_positions:
        if df.at[i, 'signal'] == 1:
            new_df = df.iloc[i+forward_candle:]
            tp = df.at[i, 'buy_tp']
            sl = df.at[i, 'buy_sl']
            specific_datetime = df.at[i+forward_candle, 'time'].time()
            open_date = df.at[i, 'time']

            if specific_datetime <= end_session and specific_datetime >= start_session:
                count_positions += 1

                o = df.at[i+forward_candle, 'open'] + df.at[i+forward_candle, 'spread'] / df.at[i+forward_candle, 'pip_value']
                lot_size = volume_calculation(symbol, o, balance, risk)
                df.at[i, 'lot_size'] = lot_size

                for j in new_df.itertuples():
                    s = j.spread / j.pip_value
                    c = j.close
                    h = j.high
                    l = j.low
                    
                    if l <= sl and h >= tp:
                        df.at[i, 'status'] = -1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break
                    elif l <= sl and c >= tp:
                        df.at[i, 'status'] = -1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break
                    elif l <= sl and h < tp and c < tp:
                        df.at[i, 'status'] = -1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break
                    elif c <= sl and h < tp:
                        df.at[i, 'status'] = -1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break
                    elif h >= tp and c <= sl:
                        df.at[i, 'status'] = 1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break
                    elif c >= tp and l > sl:
                        df.at[i, 'status'] = 1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break
                    elif h >= tp and l > sl and c > sl:
                        df.at[i, 'status'] = 1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break

        elif df.at[i, 'signal'] == -1:
            new_df = df.iloc[i+forward_candle:]
            tp = df.at[i, 'sell_tp']
            sl = df.at[i, 'sell_sl']
            specific_datetime = df.at[i+forward_candle, 'time'].time()
            open_date = df.at[i, 'time']

            if specific_datetime <= end_session and specific_datetime >= start_session:
                count_positions += 1

                o = df.at[i+forward_candle, 'open']
                lot_size = volume_calculation(symbol, o, balance, risk)
                df.at[i, 'lot_size'] = lot_size

                for j in new_df.itertuples():
                    s = j.spread / j.pip_value
                    c = j.close + s
                    h = j.high + s
                    l = j.low + s
                    
                    if h >= sl and l <= tp:
                        df.at[i, 'status'] = -1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break
                    elif h >= sl and c <= tp:
                        df.at[i, 'status'] = -1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break
                    elif h >= sl and l > tp and c > tp:
                        df.at[i, 'status'] = -1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break
                    elif c >= sl and l > tp:
                        df.at[i, 'status'] = -1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break
                    elif l <= tp and c >= sl:
                        df.at[i, 'status'] = 1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break
                    elif c <= tp and h < sl:
                        df.at[i, 'status'] = 1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break
                    elif l <= tp and c < sl and h < sl:
                        df.at[i, 'status'] = 1
                        close_date = j.time
                        open_close_positions.append(close_date - open_date)
                        break

    df['time'] = pd.to_datetime(df['time'])

    if result_to_csv:
        result_df = df[df['status'] != 0]
        result_df.to_csv(f'{path}.csv')
    
    if periodic_result == 'yearly':
        df['year'] = df['time'].dt.year
        grouped = df.groupby('year')
        periodic_dfs = {f"{year}": group for year, group in grouped}

    elif periodic_result == 'monthly':
        df['year'] = df['time'].dt.year
        df['month'] = df['time'].dt.month
        grouped = df.groupby(['year', 'month'])
        periodic_dfs = {f"{year}-{month:02d}": group for (year, month), group in grouped}

    max_neg = 0

    for key, group in periodic_dfs.items():
        try:
            print(f"Date for {key}:", '-' * 50)
            
            tp = group[group['status'] == 1]
            sl = group[group['status'] == -1]
            reward = len(tp) * rr - len(sl)
            wine_rate = (len(tp) / (len(tp) + len(sl))) * 100

            pos_pct = reward * risk
            neg_pct = commision * group['lot_size'].sum() * 100 / balance
            pct = pos_pct - neg_pct

            if pct < 0 and pct < max_neg:
                max_neg = pct

            positions = len(group[group['status']!=0])

            count_sl = count_consecutive_occurrences(group[group['status']!=0]['status'], -1)
            count_tp = count_consecutive_occurrences(group[group['status']!=0]['status'], 1)
            print(f"Max consecutive 'sl': {count_sl}")
            print(f"Max consecutive 'tp': {count_tp}")

            print(f'profit: {round(pct, 2)}, wine_rate: {round(wine_rate, 2)}, positions: {positions}')

        except ZeroDivisionError:
            print('Zero Trade ...')

    open_close_positions.sort()
    # print(f"25%: {np.percentile(open_close_positions, 25)}, 50%: {np.percentile(open_close_positions, 50)}, 75%: {np.percentile(open_close_positions, 75)}")
    # print(open_close_positions[-100:])
    print(f"We had {open_close_positions.count(max(open_close_positions))}, {max(open_close_positions)}, lasted positions")

    count_sl = count_consecutive_occurrences(df[df['status']!=0]['status'], -1)
    count_tp = count_consecutive_occurrences(df[df['status']!=0]['status'], 1)
    print(f"Max consecutive 'sl': {count_sl}")
    print(f"Max consecutive 'tp': {count_tp}")

    tp = len(df[df['status'] == 1])
    sl = len(df[df['status'] == -1])
    reward = tp * rr - sl
    pos_pct = reward * risk
    neg_pct = commision * df['lot_size'].sum() * 100 / balance
    pct = pos_pct - neg_pct
    winerate = tp / (tp + sl) * 100

    print(f'profit: {round(pct, 2)}, winerate: {round(winerate, 2)}, total positions: {count_positions}, max negative: {round(max_neg, 2)}')

def volume_calculation(symbol, price, balance, risk):
    if symbol.upper().endswith("USD"):
        return balance * risk / 100 / 10 / 10
    dollar_risk = balance * (risk / 100)
    pip_value = 100000 * 0.01 / price
    lot_size = dollar_risk / (pip_value * 10)
    
    return round(lot_size, 2)

def multi_timeframe(symbol, higher_df, lower_timeframe, start, end):
    lower_df = pd.read_csv(f'files/{symbol}_{lower_timeframe}.csv')
    lower_df['time'] = pd.to_datetime(lower_df['time'])
    lower_df.set_index('time', inplace=True)
    lower_df = lower_df.loc[start:end]
    
    higher_df.drop(columns=['open', 'high', 'low', 'close', 'volume', 'spread'], inplace=True)
    higher_df.set_index('time', inplace=True)
    
    lower_df = pd.concat([lower_df, higher_df], axis=1)
    lower_df.sort_index(inplace=True)
    lower_df.reset_index(inplace=True)
    
    return lower_df

def count_consecutive_occurrences(data, target):
    max_count = 0
    current_count = 0

    for item in data:
        if item == target:
            current_count += 1
            max_count = max(max_count, current_count)
        else:
            current_count = 0

    return max_count





# import pandas as pd
# import numpy as np
# from scipy.signal import argrelextrema
# from collections import deque

# def getHigherLows(data: np.array, order=5, K=2):
#     """
#     Finds consecutive higher lows in price pattern.
#     Must not be exceeded within the number of periods indicated by the width
#     parameter for the value to be confirmed.
#     K determines how many consecutive lows need to be higher.
#     """
#     # Get lows
#     low_idx = argrelextrema(data, np.less, order=order)[0]
#     lows = data[low_idx]
#     # Ensure consecutive lows are higher than previous lows
#     extrema = []
#     ex_deque = deque(maxlen=K)
#     for i, idx in enumerate(low_idx):
#         if i == 0:
#             ex_deque.append(idx)
#             continue
#         if lows[i] < lows[i-1]:
#             ex_deque.clear()
            
#         ex_deque.append(idx)
#         if len(ex_deque) == K:
#             extrema.append(ex_deque.copy())
            
#     return extrema

# def getLowerHighs(data: np.array, order=5, K=2):
#     """
#     Finds consecutive lower highs in price pattern.
#     Must not be exceeded within the number of periods indicated by the width
#     parameter for the value to be confirmed.
#     K determines how many consecutive highs need to be lower.
#     """
#     # Get highs
#     high_idx = argrelextrema(data, np.greater, order=order)[0]
#     highs = data[high_idx]
#     # Ensure consecutive highs are lower than previous highs
#     extrema = []
#     ex_deque = deque(maxlen=K)
#     for i, idx in enumerate(high_idx):
#         if i == 0:
#             ex_deque.append(idx)
#             continue
#         if highs[i] > highs[i-1]:
#             ex_deque.clear()
            
#         ex_deque.append(idx)
#         if len(ex_deque) == K:
#             extrema.append(ex_deque.copy())
            
#     return extrema

# def getHigherHighs(data: np.array, order=5, K=2):
#     """
#     Finds consecutive higher highs in price pattern.
#     Must not be exceeded within the number of periods indicated by the width
#     parameter for the value to be confirmed.
#     K determines how many consecutive highs need to be higher.
#     """
#     # Get highs
#     high_idx = argrelextrema(data, np.greater, order=5)[0]
#     highs = data[high_idx]
#     # Ensure consecutive highs are higher than previous highs
#     extrema = []
#     ex_deque = deque(maxlen=K)
#     for i, idx in enumerate(high_idx):
#         if i == 0:
#             ex_deque.append(idx)
#             continue
#         if highs[i] < highs[i-1]:
#             ex_deque.clear()
            
#         ex_deque.append(idx)
#         if len(ex_deque) == K:
#             extrema.append(ex_deque.copy())
            
#     return extrema

# def getLowerLows(data: np.array, order=5, K=2):
#     """
#     Finds consecutive lower lows in price pattern.
#     Must not be exceeded within the number of periods indicated by the width
#     parameter for the value to be confirmed.
#     K determines how many consecutive lows need to be lower.
#     """
#     # Get lows
#     low_idx = argrelextrema(data, np.less, order=order)[0]
#     lows = data[low_idx]
#     # Ensure consecutive lows are lower than previous lows
#     extrema = []
#     ex_deque = deque(maxlen=K)
#     for i, idx in enumerate(low_idx):
#         if i == 0:
#             ex_deque.append(idx)
#             continue
#         if lows[i] > lows[i-1]:
#             ex_deque.clear()
            
#         ex_deque.append(idx)
#         if len(ex_deque) == K:
#             extrema.append(ex_deque.copy())
            
#     return extrema

# def backtest(df, symbol, start_session, end_session, balance, risk, rr, commision, periodic_result="yearly", multi_time_frame=False, lower_timeframe=None, forward_candle=1, start_period=None, end_period=None, result_to_csv=False, path=None):
    
#     symbols = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'EURGBP', 'EURAUD', 'EURNZD', 'EURCAD', 'EURCHF', 'GBPNZD', 'GBPAUD', 'GBPCAD', 'GBPCHF', 'CADCHF', 'NZDCAD', 'AUDCAD', 'AUDNZD', 'AUDCHF']

#     if symbol.upper() in symbols:
#         pip_value = 100000
#     elif "JPY" in symbol.upper():
#         pip_value = 1000
#     elif "XAUUSD" == symbol.upper():
#         pip_value = 100
#     else:
#         raise ValueError("Symbol is not defined!!!")

#     if multi_time_frame:
#         df = multi_timeframe(symbol, df, lower_timeframe, start_period, end_period)

#     df['status'] = 0
#     df['pip_value'] = pip_value
#     df['lot_size'] = 0
#     df['lot_size'] = df['lot_size'].astype(float)
#     df['structure'] = "no"

#     buy_positions = df[df['signal'] == 1].index.values.tolist()
#     sell_positions = df[df['signal'] == -1].index.values.tolist()

#     all_positions = buy_positions + sell_positions
#     all_positions = sorted(all_positions)

#     open_close_positions = []
#     count_positions = 0
    
#     for i in all_positions:
#         try:
#             past_df = df.iloc[i-1000:i]

#             hh = getHigherHighs(past_df['close'].values)
#             hl = getHigherLows(past_df['close'].values)
#             ll = getLowerLows(past_df['close'].values)
#             lh = getLowerHighs(past_df['close'].values)
            
#             for z in hh:
#                 past_df.at[past_df.index[z[-1]], 'structure'] = 'hh'
                
#             for z in hl:
#                 past_df.at[past_df.index[z[-1]], 'structure'] = 'hl'
                
#             for z in ll:
#                 past_df.at[past_df.index[z[-1]], 'structure'] = 'll'
                
#             for z in lh:
#                 past_df.at[past_df.index[z[-1]], 'structure'] = 'lh'
                
#             points = past_df[past_df['structure'] != 'no']['structure'].values.tolist()
            
#             if df.at[i, 'signal'] == 1:
#                 if points[-1] == "ll":
#                     pass
#                 else:
#                     continue

#                 new_df = df.iloc[i+forward_candle:]
#                 tp = df.at[i, 'buy_tp']
#                 sl = df.at[i, 'buy_sl']
#                 specific_datetime = df.at[i+forward_candle, 'time'].time()
#                 open_date = df.at[i, 'time']

#                 if specific_datetime <= end_session and specific_datetime >= start_session:
#                     count_positions += 1

#                     o = df.at[i+forward_candle, 'open'] + df.at[i+forward_candle, 'spread'] / df.at[i+forward_candle, 'pip_value']
#                     lot_size = volume_calculation(symbol, o, balance, risk)
#                     df.at[i, 'lot_size'] = lot_size

#                     for j in new_df.itertuples():
#                         s = j.spread / j.pip_value
#                         c = j.close
#                         h = j.high
#                         l = j.low
                        
#                         if l <= sl and h >= tp:
#                             df.at[i, 'status'] = -1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#                         elif l <= sl and c >= tp:
#                             df.at[i, 'status'] = -1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#                         elif l <= sl and h < tp and c < tp:
#                             df.at[i, 'status'] = -1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#                         elif c <= sl and h < tp:
#                             df.at[i, 'status'] = -1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#                         elif h >= tp and c <= sl:
#                             df.at[i, 'status'] = 1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#                         elif c >= tp and l > sl:
#                             df.at[i, 'status'] = 1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#                         elif h >= tp and l > sl and c > sl:
#                             df.at[i, 'status'] = 1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break

#             elif df.at[i, 'signal'] == -1:
#                 if points[-1] == "hh":
#                     pass
#                 else:
#                     continue

#                 new_df = df.iloc[i+forward_candle:]
#                 tp = df.at[i, 'sell_tp']
#                 sl = df.at[i, 'sell_sl']
#                 specific_datetime = df.at[i+forward_candle, 'time'].time()
#                 open_date = df.at[i, 'time']

#                 if specific_datetime <= end_session and specific_datetime >= start_session:
#                     count_positions += 1

#                     o = df.at[i+forward_candle, 'open']
#                     lot_size = volume_calculation(symbol, o, balance, risk)
#                     df.at[i, 'lot_size'] = lot_size

#                     for j in new_df.itertuples():
#                         s = j.spread / j.pip_value
#                         c = j.close + s
#                         h = j.high + s
#                         l = j.low + s
                        
#                         if h >= sl and l <= tp:
#                             df.at[i, 'status'] = -1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#                         elif h >= sl and c <= tp:
#                             df.at[i, 'status'] = -1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#                         elif h >= sl and l > tp and c > tp:
#                             df.at[i, 'status'] = -1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#                         elif c >= sl and l > tp:
#                             df.at[i, 'status'] = -1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#                         elif l <= tp and c >= sl:
#                             df.at[i, 'status'] = 1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#                         elif c <= tp and h < sl:
#                             df.at[i, 'status'] = 1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#                         elif l <= tp and c < sl and h < sl:
#                             df.at[i, 'status'] = 1
#                             close_date = j.time
#                             open_close_positions.append(close_date - open_date)
#                             break
#         except:
#             pass

#     df['time'] = pd.to_datetime(df['time'])
    
#     if result_to_csv:
#         result_df = df[df['status'] != 0]
#         result_df.to_csv(f'{path}.csv')

#     if periodic_result == 'yearly':
#         df['year'] = df['time'].dt.year
#         grouped = df.groupby('year')
#         periodic_dfs = {f"{year}": group for year, group in grouped}

#     elif periodic_result == 'monthly':
#         df['year'] = df['time'].dt.year
#         df['month'] = df['time'].dt.month
#         grouped = df.groupby(['year', 'month'])
#         periodic_dfs = {f"{year}-{month:02d}": group for (year, month), group in grouped}

#     max_neg = 0

#     for key, group in periodic_dfs.items():
#         try:
#             print(f"Date for {key}:", '-' * 50)
            
#             tp = group[group['status'] == 1]
#             sl = group[group['status'] == -1]
#             reward = len(tp) * rr - len(sl)
#             wine_rate = (len(tp) / (len(tp) + len(sl))) * 100

#             pos_pct = reward * risk
#             neg_pct = commision * group['lot_size'].sum() * 100 / balance
#             pct = pos_pct - neg_pct

#             if pct < 0 and pct < max_neg:
#                 max_neg = pct

#             positions = len(group[group['status']!=0])

#             count_sl = count_consecutive_occurrences(group[group['status']!=0]['status'], -1)
#             count_tp = count_consecutive_occurrences(group[group['status']!=0]['status'], 1)
#             print(f"Max consecutive 'sl': {count_sl}")
#             print(f"Max consecutive 'tp': {count_tp}")

#             print(f'profit: {round(pct, 2)}, wine_rate: {round(wine_rate, 2)}, positions: {positions}')

#         except ZeroDivisionError:
#             print('Zero Trade ...')

#     open_close_positions.sort()
#     # print(f"25%: {np.percentile(open_close_positions, 25)}, 50%: {np.percentile(open_close_positions, 50)}, 75%: {np.percentile(open_close_positions, 75)}")
#     # print(open_close_positions[-100:])
#     print(f"We had {open_close_positions.count(max(open_close_positions))}, {max(open_close_positions)}, lasted positions")

#     count_sl = count_consecutive_occurrences(df[df['status']!=0]['status'], -1)
#     count_tp = count_consecutive_occurrences(df[df['status']!=0]['status'], 1)
#     print(f"Max consecutive 'sl': {count_sl}")
#     print(f"Max consecutive 'tp': {count_tp}")

#     tp = len(df[df['status'] == 1])
#     sl = len(df[df['status'] == -1])
#     reward = tp * rr - sl
#     pos_pct = reward * risk
#     neg_pct = commision * df['lot_size'].sum() * 100 / balance
#     pct = pos_pct - neg_pct
#     winerate = tp / (tp + sl) * 100

#     print(f'profit: {round(pct, 2)}, winerate: {round(winerate, 2)}, total positions: {count_positions}, max negative: {round(max_neg, 2)}')

# def volume_calculation(symbol, price, balance, risk):
#     if symbol.upper().endswith("USD"):
#         return balance * risk / 100 / 10 / 10
#     dollar_risk = balance * (risk / 100)
#     pip_value = 100000 * 0.01 / price
#     lot_size = dollar_risk / (pip_value * 10)
    
#     return round(lot_size, 2)

# def multi_timeframe(symbol, higher_df, lower_timeframe, start, end):
#     lower_df = pd.read_csv(f'files/{symbol}_{lower_timeframe}.csv')
#     lower_df['time'] = pd.to_datetime(lower_df['time'])
#     lower_df.set_index('time', inplace=True)
#     lower_df = lower_df.loc[start:end]
    
#     higher_df.drop(columns=['open', 'high', 'low', 'close', 'volume', 'spread'], inplace=True)
#     higher_df.set_index('time', inplace=True)
    
#     lower_df = pd.concat([lower_df, higher_df], axis=1)
#     lower_df.sort_index(inplace=True)
#     lower_df.reset_index(inplace=True)
    
#     return lower_df

# def count_consecutive_occurrences(data, target):
#     max_count = 0
#     current_count = 0

#     for item in data:
#         if item == target:
#             current_count += 1
#             max_count = max(max_count, current_count)
#         else:
#             current_count = 0

#     return max_count
