import pandas as pd
import os
import akshare as ak
from tqdm import tqdm
from typing import List, Union


class DataSaver:
    """
    获取数据，并保存至本地
    抽象基类
    """
    def __init__(self, root_path: str = "") -> None:
        if root_path:
            self.root = root_path
        else:
            self.root = os.getcwd()

    def reset_root(self, root_path: str) -> None:
        """
        重设数据存储根目录
        """
        self.root = root_path

class StockSaver(DataSaver):
    """
    获取、保存股票数据
    目前采用akshare接口
    """
    @staticmethod
    def _get_stkcd() -> List[str]:
        """
        采用ak.stock_zh_a_spot_em()接口
        返回所有A股上市公司的实时行情，并以此确认待请求的股票代码 
        """
        temp_df = ak.stock_zh_a_spot_em()
        stkcd_lst: list = list(temp_df['代码'].unique())

        return stkcd_lst

    def save_trade_data(
            self, 
            start_date: str,
            end_date: str,
            period: str = 'daily',
            adjust: str = 'hfq',
            target: Union[str, list] = 'all'
            ) -> list:
        """
        采用ak.stock_zh_a_hist()接口
        名称	    类型	   描述
        period	    str	    period='daily'; choice of {'daily', 'weekly', 'monthly'}
        start_date	str	    start_date='20210301'; 开始查询的日期
        end_date	str	    end_date='20210616'; 结束查询的日期
        adjust	    str	    默认返回不复权的数据; qfq: 返回前复权后的数据; hfq: 返回后复权后的数据
        target      list    默认调用 _get_stkcd()并请求所有公司的数据；若传入股票代码列表，则根据传入
                            列表，请求相关公司的数据
        """
        
        failed_lst = []  # 请求失败列表

        # 获取待请求股票代码的列表
        if target == 'all':
            stkcd_lst = self._get_stkcd()
        else:
            stkcd_lst = target

        #遍历每一股票代码，保存到本地
        save_dir = os.path.join(self.root, 'data', 'stock', 'trade', period)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        iters = tqdm(stkcd_lst)
        for stkcd in iters:
            file_name = f"{stkcd}_{start_date}_{end_date}.csv"

            try:
                stkcd_df = ak.stock_zh_a_hist(stkcd, period, start_date, end_date, adjust)
                stkcd_df.to_csv(os.path.join(save_dir, file_name), encoding='gbk', index=False)
            except:
                print(f"股票{stkcd}请求失败")
                failed_lst.append(stkcd)

            iters.set_description("Processing %s" % stkcd)
        
        #返回请求失败的股票列表，以供检查，与调用本方法再次请求
        print('下列股票日度数据请求失败，请重试：')
        return failed_lst