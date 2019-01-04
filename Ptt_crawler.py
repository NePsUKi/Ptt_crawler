import requests
from bs4 import BeautifulSoup
import pandas as pd
import sys
import time
import os
import re


class Ptt_crawler():
    
    def __init__(self):
        self.PTT_url = 'https://www.ptt.cc'
        self.save_path = ''
        self.filename = 'PTT_result.csv'
    
    def PTT_search(self, query, board, category, pages = 1, use_comment = True, ignore_arrow = True, max_amount = None, max_comment = None, progress_bar = True):
        '''
        query           : 搜尋關鍵字　　　　     type為string                       想搜尋的關鍵字，如果category為推文，則輸入-100~100
        board           : 看板名稱　　　　　     type為string                       ptt的看板名稱，如Gossiping、C_Chat、NBA、LoL、joke等
        category        : 搜尋類別　　　　　     type為string                       可以選擇搜尋「標題」或是「推文」
        pages           : 抓第幾頁的文章　       type為int          預設為1
        use_comment     : 是否儲存回文評論       type為bool         預設為True
        ignore_arrow    : 是否忽略回文類型為→    type為bool         預設為True
        max_amount      : 儲存的文章數量　　     type為int/None     預設為None       如果為預設，代表全部會儲存，但在ptt的網頁中，一頁的文章最多20篇，就算max_amount>20，也不會超過20篇文
        max_comment     : 儲存的回文評論數量     type為int/None     預設為None       如果為預設，代表全部的回文都會儲存
        progress_bar    : 是否顯示進度條　       type為bool         預設為True
        '''

        if category == '推文':
            reqs = requests.get('https://www.ptt.cc/bbs/%s/search?page=%d&q=recommend%%3A%s' % (board, pages, str(query)), cookies={'over18': '1'})
        elif category == '標題':
            reqs = requests.get('https://www.ptt.cc/bbs/%s/search?page=%d&q=%s' % (board, pages, str(query)), cookies={'over18': '1'})

        web_inner = BeautifulSoup(reqs.text, 'html.parser')

        # 沒有文章
        if(web_inner.find_all('div',{'class' : 'r-ent'}) == []):
            return '找不到符合搜尋字詞'

        result = []
        doc_url = None
        author = None
        date = None
        push_amount = None
        title = None
        inner_text = None
        comment_list = None

        doc_list = web_inner.find_all('div',{'class' : 'r-ent'})
        for i, doc in enumerate(doc_list):

            # 文章被刪除，沒有連結
            if (doc.find('div',{'class' : 'title'}).find('a') == None):
                continue

            author, date, title, push_amount = self.basic_data_processing(doc)

            url = doc.find('div',{'class' : 'title'}).find('a').get('href')
            doc_url = self.PTT_url + url
            res = requests.get(doc_url, cookies={'over18': '1'})
            inner = BeautifulSoup(res.text, 'html.parser')

            # 文章被刪除，空連結
            if (inner.find('div',{'id' : 'main-content','class' : 'bbs-screen bbs-content'}) == None):
                continue

            inner_text = self.inner_text_processing(res)

            if use_comment:
                comment_list = self.comment_processing(inner, ignore_arrow, max_comment)
                result.append([doc_url, board, category, query, author, date, push_amount, title, inner_text, comment_list])
            else:
                result.append([doc_url, board, category, query, author, date, push_amount, title, inner_text])
            
            if progress_bar:
                if max_amount == None:
                    self.progress_bar(i, len(doc_list))
                else:
                    self.progress_bar(len(result), max_amount)

            if(max_amount != None and max_amount == len(result)):
                break

        if result == []:
            return '找不到符合搜尋字詞或文章已被刪除'

        if use_comment:
            result_df = pd.DataFrame(result, columns=['doc_url', 'board', 'category', 'query',' author', 'date', 'push_amount', 'title', 'inner_text', 'comment'])
        else:
            result_df = pd.DataFrame(result, columns=['doc_url', 'board', 'category', 'query',' author', 'date', 'push_amount', 'title', 'inner_text'])
        '''
        doc_url      : 文章連結
        board        : 看板名稱
        category     : 搜尋類別
        query        : 搜尋關鍵字
        author       : 發文者
        date         : 發文日期
        push_amount  : 總推文數
        title        : 發文標題
        inner_text   : 文章內文
        comment      : 回文列表
        '''

        # 檔案儲存，預設儲存位址與名稱為工作根目錄 + PTT_result.csv
        result_df.to_csv(self.save_path + self.filename, encoding='utf_8_sig',index_label='No.')
        if progress_bar:
            self.progress_bar(1, 1)
        return result_df
    
    # 文章基本資料處理
    def basic_data_processing(self, doc):
        author = doc.find('div',{'class' : 'meta'}).find('div', {'class' : 'author'}).text.strip()
        date = doc.find('div',{'class' : 'meta'}).find('div', {'class' : 'date'}).text.strip()
        title = doc.find('div',{'class' : 'title'}).find('a').text.strip()
        push_amount = doc.find('div',{'class' : 'nrec'}).find('span')
        if push_amount == None:
            push_amount = '0'
        else:
            push_amount = push_amount.text.strip()
        return author, date, title, push_amount

    # 文章內文處理
    def inner_text_processing(self, res):
        inner_text = BeautifulSoup(res.text, 'html.parser')
        inner_text = inner_text.find('div',{'id' : 'main-content','class' : 'bbs-screen bbs-content'})
        for s in inner_text(['a', 'div']):
            s.extract()
        inner_text = inner_text.text.strip()
        return inner_text

    # 文章回文評論處理
    def comment_processing(self, inner, ignore_arrow, max_comment):
        push_list = inner.find('div',{'id' : 'main-content','class' : 'bbs-screen bbs-content'}).find_all('div',{'class' : 'push'})

        if push_list == []:
            return 'Empty'

        comment_list = []
        for push_comment in push_list:
            # 回文被刪除
            if(push_comment.find('span') == None):
                continue        
            push_tag = push_comment.find('span').text.strip()
            if (ignore_arrow and push_tag == '→'):
                continue
            push_user = push_comment.find('span', {'class' : 'f3 hl push-userid'}).text.strip()
            push_content = push_comment.find('span', {'class' : 'f3 push-content'}).text.strip()
            comment_list.append([push_tag, push_user, push_content[2:]])
            '''
            push_tag     : 推/噓/→
            push_user    : 回文者
            push_content : 回文內容
            '''

            if(max_comment != None and max_comment == len(comment_list)):
                break

        return comment_list
    
    # 設定儲存位置
    def set_save_path(self, path):
        self.save_path = ''
        dir_names = path.replace('\\','/').split('/')
        for i, dir_name in enumerate(dir_names):
            assert re.match(r'[\:\*\?\"\<\>\|]', dir_name) == None, '資料夾名稱不能包含:*?"<>|'
            self.save_path += dir_name
            if i == 0:
                if not re.fullmatch(r'(^[A-Za-z]:)|\.', self.save_path):
                    self.save_path = '.' + self.save_path
            self.save_path += '/'
            if not os.path.isdir(self.save_path):
                os.mkdir(self.save_path)



    # 設定檔案名稱，副檔名必須為.txt或.csv
    def set_filename(self, name):
        self.filename = name
        if (self.filename[-4:] != '.txt' or self.filename[-4:] != '.csv'):
            self.filename += '.csv'
    
    # 顯示進度條
    def progress_bar(self, progress, max_amount):
        sys.stdout.write('\r')
        sys.stdout.write("[%-100s] %.2f%%" % ('=' * (int(progress / max_amount * 100) - 1) + '>', progress / max_amount * 100))
        sys.stdout.flush()
        time.sleep(0.25)


# main function只是參考
if __name__ == "__main__":
    search = Ptt_crawler()
    search.set_save_path('./ptt_data')
    search.set_filename('ptt_document.csv')
    result = search.PTT_search('100','Gossiping', '推文', max_amount=10, max_comment=10)