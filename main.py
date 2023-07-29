import asyncio
import json
from datetime import datetime
import aiohttp
import gspread
from gspread_formatting import *
import re
import logging

logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logging.getLogger('aiohttp').setLevel(logging.ERROR)

class Sheet():
  sheet_key = '1U7ipYaFRE7toaiYvCZQmnsuEz7QYELNdZfZftuhmYu8'

  def __init__(self):
    conn_sheet = gspread.service_account(filename='vk_group.json')
    self.sh = conn_sheet.open_by_key(self.sheet_key)

  def get_params(self):
    self.google_sheet_lists, token, group_id, key_phrases = [], '', '', []
    for list_sheet in self.sh.worksheets():
      if 'настройки' in list_sheet.title.lower():
        sett = self.sh.worksheet(list_sheet.title)
        token = sett.acell('B1').value
        group_id = sett.acell('B2').value
        keys = sett.get_values('D2:E999')

        if '&expires_in' in token:
          token = token.split('&expires_in')[0]

        for k in keys:
          key_phrases.append({'key': k[0], 'list_name': k[1]})
      else:
        self.google_sheet_lists.append(list_sheet.title)

    return token, group_id, key_phrases, self.google_sheet_lists

  async def set_sheet_items(self, list_name, users_key, users_comments, new=True):
    rgb = [[212, 154, 98], [102, 214, 130], [106, 182, 189], [251, 183, 220], [61, 187, 198], [141, 180, 148], [204, 204, 204], [64, 128, 144], [32, 128, 80], [106, 176, 222], [153, 153, 153], [136, 140, 192]]

    try:
      users = []
      if new:
        try:
          worksheet = self.sh.add_worksheet(title=list_name, rows=1000, cols=20, index=1)
          worksheet.update('A1:G1', [['ID страницы', 'ФИО', 'Время', 'Текст', 'Платформа', 'IGG ID', 'Реакция']])
          worksheet.format("A1:G1", {
            "textFormat": {
              "fontSize": 12,
              "bold": True
            }
          })
        except Exception as e:
          logging.error("Exception occurred", exc_info=True)
          worksheet = self.sh.worksheet(list_name)

        self.google_sheet_lists.append(list_name)
      else:
        worksheet = self.sh.worksheet(list_name)

      print(list_name, len(users_comments))
      for user_key in users_comments:
        user = users_comments[user_key]
        user_list = [str(user['user_id']), user['name'], user['time'], user['text'], user['platform'], user['igg_id'], user['like']]

        users.append(user_list)
        if user['repeat'] > 0:
          if rgb:
            select_background = rgb[0]
            rgb.remove(select_background)
            s = [f'A{users_comments[y]["num"]}:G{users_comments[y]["num"]}' for y in users_comments if users_comments[y]['user_id'] == user['user_id']]
            for cell in s:
               worksheet.format(cell, {
                "backgroundColor": {
                  "red": select_background[0]/255,
                  "green": select_background[1]/255,
                  "blue": select_background[2]/255
                }
              })
            await asyncio.sleep(1)

      if users:
        worksheet.update('A2:G9999', users)
      else:
        if len(users_comments) > 0 and not users:
          await self.set_sheet_items(list_name, users_key, users_comments, new=new)

      if new:
        worksheet.format("A1:G9999", {
          "horizontalAlignment": "CENTER"
        })
        set_column_widths(worksheet, [('A:', 120), ('B:', 200), ('C:', 120), ('D:', 300), ('E:', 120), ('F:', 120)])
    except Exception as e:
      logging.error("Exception occurred", exc_info=True)

  async def update_sheet_items(self, list_name, users_comments):
    pass

class vkApp(Sheet):
  detect_android = ['android', 'андроид', 'andr', 'андр']
  detect_ios = ['ios', 'иос', 'aйос']

  def __init__(self):
    asyncio.run(self.get_posts())

  async def get_headers(self):
    return {'Authorization': f'Bearer {self.token}'}

  async def get_likes_for_post(self, post_id, like_count):
    try:
      users, offset, count = [], 0, 1000
      while True:
        async with aiohttp.ClientSession(headers=await self.get_headers()) as session:
          async with session.post('https://api.vk.com/method/wall.getLikes/', data={'v': '5.131', 'owner_id': self.group_id, 'post_id': post_id, 'count': 1000}) as res:
            r = await res.text()
            j = json.loads(r)

            if j.get('response'):
              if j['response'].get('users'):
                for user in j['response']['users']:
                  users.append(user['uid'])

            if count <= like_count:
              offset += 1000
              count += 1000
            else:
              break
        await asyncio.sleep(1)
      return users
    except Exception as e:
      logging.error("Exception occurred", exc_info=True)


  async def get_comments(self, post_id, comment_count, users_ids_like, list_name):
    num = 2
    try:
      igg_ids, users_ids, users_comments, offset, count = [], [], {}, 0, 100
      while True:
        async with aiohttp.ClientSession(headers=await self.get_headers()) as session:
          async with session.post('https://api.vk.com/method/wall.getComments/', data={'v': '5.131', 'owner_id': self.group_id, 'post_id': post_id, 'count': 100, 'offset': offset, 'extended': 1, 'fields': 1}) as res:
            r = await res.text()
            j = json.loads(r)

            if j.get('response'):
              if j['response'].get('items'):
                for comment in j['response']['items']:
                  dt = datetime.fromtimestamp(comment['date'])
                  is_like = comment['from_id'] in users_ids_like and 'Да' or 'Нет'
                  comment_date_public = f'{len(str(dt.day)) == 1 and f"0{dt.day}" or dt.day}/{len(str(dt.month)) == 1 and f"0{dt.month}" or dt.month}/{dt.year} {len(str(dt.hour)) == 1 and f"0{dt.hour}" or dt.hour}:{len(str(dt.minute)) == 1 and f"0{dt.minute}" or dt.minute}'
                  fullname = ''.join([f"{n['first_name']} {n['last_name']}" for n in j['response']['profiles'] if n['id'] == comment['from_id']])

                  is_android = [pl for pl in self.detect_android if pl in comment['text'].lower()]
                  is_ios = [pl for pl in self.detect_ios if pl in comment['text'].lower()]
                  is_iggid = re.findall(r'[0-9]{4,}', comment['text'])

                  if (is_android or is_ios) and is_iggid:
                    if is_iggid[0] not in igg_ids:
                      key = f"{comment['from_id']}:{is_iggid[0]}"
                      igg_ids.append(is_iggid[0])
                      platform = is_android and 'Android' or 'iOS'

                      t = comment['text'].replace('\n', ' ')
                      users_comments[key] = {'num': num, 'user_id': comment['from_id'], 'name': fullname, 'time': comment_date_public, 'text': t, 'platform': platform, 'igg_id': is_iggid[0], 'like': is_like, 'repeat': users_ids.count(comment['from_id'])}
                      users_ids.append(comment['from_id'])
                      num += 1


            if count <= comment_count:
              offset += 100
              count += 100
            else:
              break

        await asyncio.sleep(1)

      new = list_name not in self.google_sheet_lists
      await self.sheet.set_sheet_items(list_name, users_ids, users_comments, new=new)

    except Exception as e:
      logging.error("Exception occurred", exc_info=True)

  async def get_posts(self):
    while True:
      try:
        self.sheet = Sheet()
        self.token, self.group_id, self.key_phrases, self.google_sheet_lists = self.sheet.get_params()

        async with aiohttp.ClientSession(headers=await self.get_headers()) as session:
          async with session.post('https://api.vk.com/method/wall.get/', data={'v': '5.131', 'owner_id': self.group_id, 'count': 20}) as res:
            r = await res.text()
            j = json.loads(r)

            if j.get('response'):
              if j['response'].get('items'):
                for post in j['response']['items']:
                  is_phrase = [obj for obj in self.key_phrases if obj['key'].lower() in post['text'].lower()]

                  if is_phrase:
                    dt = datetime.fromtimestamp(post['date'])
                    sign = f'{len(str(dt.day)) == 1 and f"0{dt.day}" or dt.day}/{len(str(dt.month)) == 1 and f"0{dt.month}" or dt.month}/{dt.year}'
                    list_name = is_phrase[0]['list_name']+' '+sign

                    comment_count = post['comments']['count']
                    like_count = post['likes']['count']

                    users_ids_like = await self.get_likes_for_post(post['id'], like_count)
                    await self.get_comments(post['id'], comment_count, users_ids_like, list_name)
      except Exception as e:
        logging.error("Exception occurred", exc_info=True)

      await asyncio.sleep(60)


if __name__ == '__main__':
  vkApp()
