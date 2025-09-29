#writefile export_tv_watchlist.py
import pandas as pd
from pathlib import Path
from typing import Callable, Iterable, Optional

import twstock  # 用來判斷上市/上櫃/興櫃
import pandas as pd
from pathlib import Path
from typing import Iterable, Optional


def get_tw_exchange_formatter(
    tpex_symbols: Optional[set[str]] = None
) -> Callable[[str], str]:
    """
    產生一個 formatter(symbol) -> 'TWSE:XXXX' / 'TPEX:XXXX' 的函式。

    邏輯：
    1) 若可用 twstock：上市 -> TWSE；上櫃/興櫃 -> TPEX。
    2) twstock 不可用或查無代碼：若在 tpex_symbols 中 -> TPEX，否則 TWSE。
    """
    if tpex_symbols is None:
        tpex_symbols = set()

    if twstock is not None:
        codes = twstock.codes

        def formatter(sym: str) -> str:
            code = codes.get(str(sym))
            if code is not None:
                market = getattr(code, "market", "")
                if market in ("上櫃", "興櫃"):
                    prefix = "TPEX:"
                else:
                    prefix = "TWSE:"
            else:
                prefix = "TPEX:" if str(sym) in tpex_symbols else "TWSE:"
            return f"{prefix}{sym}"

        return formatter

    def formatter(sym: str) -> str:
        prefix = "TPEX:" if str(sym) in tpex_symbols else "TWSE:"
        return f"{prefix}{sym}"

    return formatter


def export_tradingview_txt(
    df: pd.DataFrame,
    outfile: Optional[str] = None,
    exchange_prefix: Optional[str] = None,
    date: Optional[pd.Timestamp] = None,
    custom_formatter: Optional[Callable[[str], str]] = None,
) -> Path:
    """
    從布林 DataFrame 匯出 TradingView 可匯入的 txt（每行一個代號）。
    - index: 日期（DatetimeIndex 或可轉成日期的字串）
    - columns: 代號
    - 值: True/False
    """
    if date is None:
        date = pd.to_datetime("today").normalize()
    else:
        date = pd.to_datetime(date).normalize()

    if not isinstance(df.index, pd.DatetimeIndex):
        df = df.copy()
        df.index = pd.to_datetime(df.index).normalize()

    if date not in df.index:
        raise KeyError(f"指定日期 {date.date()} 不在 DataFrame 的索引中。")

    today_row = df.loc[date]
    true_symbols: Iterable[str] = today_row[today_row.astype(bool)].index.astype(str)

    if outfile is None:
        outfile = f"tws_{date.strftime('%Y%m%d')}.txt"

    out_path = Path(outfile).resolve()
    rs_folder ="/Users/caibinghong/Library/Mobile Documents/com~apple~CloudDocs/rs"
    out_path = Path(rs_folder) / outfile
    lines = []

    for sym in true_symbols:
        if custom_formatter is not None:
            line = custom_formatter(sym)
        elif exchange_prefix:
            line = f"{exchange_prefix}{sym}"
        else:
            # 未指定前綴與自訂 formatter 時，預設不加前綴
            line = sym
        lines.append(line)

    out_path.write_text("\n".join(lines), encoding="utf-8")
    return out_path

# Example usage (will not be executed when imported as a module)
if __name__ == "__main__":
    # Assuming 'pos' DataFrame is available in the environment where this script is run
    # You would need to load or define 'pos' before running this script directly.
    # For example:
    
    from finlab import data
    from dotenv import load_dotenv
    import os
    import finlab
    load_dotenv()
    finlab.login(os.getenv("FINLAB_TOKEN"))
    close = data.get('price:收盤價')
    rs=close/close.shift(5)
    vol = data.get('price:成交股數')
    當月營收 = data.get('monthly_revenue:當月營收')
    odd_vol=data.get('intraday_odd_lot_trade:成交股數')
    cond2 = (當月營收.average(3) > 當月營收.average(12))
    cond3 = rs>rs.quantile_row(0.9)
    cond4 = vol.average(5) > vol.average(20)*1.5
    pos= cond2 & cond3 &cond4


    fmt = get_tw_exchange_formatter()
    # Pass the 'pos' DataFrame to the function
    path = export_tradingview_txt(pos, custom_formatter=fmt)
    print(f"已輸出：{path}")