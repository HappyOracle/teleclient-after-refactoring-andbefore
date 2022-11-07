from asyncio.windows_events import NULL
import cmd
from telethon import TelegramClient
from array import array
from telethon import TelegramClient, sync, events
from telethon import TelegramClient, events
import asyncio
from asyncio import new_event_loop
import collections
import sys
import pyodbc
from numpy import *
import filecmp, os


# НОВАЯ ВЕРСИЯ экспериментальная

api_id = 17876697
api_hash = 'f57224fa791de500d30a17a4fbb650ec'
client = TelegramClient('anon', api_id, api_hash)
chat = 418820330  # 'me', 5103866250, 1491627223, 418820330, -741750752(test), -1001299652068(it)
client.start()

connGG = pyodbc.connect('Driver={SQL SERVER};'
                        'Server=10.110.10.29;'
                        'Database=DB-TG;'
                        'UID=sa;'
                        'PWD=Aa123456;'  # Aa123456
                        'autocommit=True')

cursor = connGG.cursor()


@client.on(events.NewMessage(chats=(chat)))
async def main(event):
    restart_for_cycle = True
    if chat <= 0:
        file = open(r"C:\Users\Administrator\PycharmProjects\Groupdata.txt", "a+", encoding='utf-16')
        while restart_for_cycle:  # True
            async for message in client.iter_messages(chat):
                from_user = await client.get_entity(message.from_id.user_id)
                print(from_user.username, message.from_id.user_id, message.chat_id, message.id, message.date,
                      message.fwd_from, message.peer_id, message.text)
                msg_text = "{username} {dialog_id} {chat} {id} {date} {fwd_from} {peer_id} {Message}: \n".format(username=from_user.username,
                    dialog_id=message.from_id.user_id, chat=message.chat_id, id=message.id, date=message.date,
                    fwd_from=message.fwd_from, peer_id=message.peer_id, Message=message.text)
                file.write(msg_text)
            if message.photo or message.file:
                print('File Name :' + str(message.file.name))
                path = await client.download_media(message.media)  # message.media, "youranypathher"
                print('File saved to', path)  # printed after download is done
            restart_for_cycle = False
        await asyncio.sleep(99999999999999999999999)
    if chat >= 0:
        file = open(r"C:\Users\Administrator\PycharmProjects\Userdata.txt", "a+", encoding='utf-16')
        while restart_for_cycle:  # True
            async for message in client.iter_messages(chat):
                print(message.chat_id, message.id, message.date, message.fwd_from, message.peer_id, message.text)
                msg_text = "{chat} {id} {date} {fwd_from} {peer_id} {Message}: \n".format(chat=message.chat_id,
                                                                                          id=message.id,
                                                                                          date=message.date,
                                                                                          fwd_from=message.fwd_from,
                                                                                          peer_id=message.peer_id,
                                                                                          Message=message.text)
                file.write(msg_text)
            if message.photo or message.file:
                print('File Name :' + str(message.file.name))
                path = await client.download_media(message.media)  # message.media, "youranypathher"
                print('File saved to', path)  # printed after download is done
            restart_for_cycle = False
        await asyncio.sleep(99999999999999999999999)


@client.on(events.NewMessage(chats=chat, pattern=rf'\.{cmd}', outgoing=True))
async def handler(event):
    while main(events):
        for output_channel in int(chat):
            await client.forward_messages(output_channel, event.message)
    async for message in client.iter_messages(chat):
        await events.MessageDeleted(print(list.message.id, 'Удаленный сообщение'))
    return


for message in client.iter_messages(chat):
    from_user = client.get_entity(message.from_id.user_id)
    messagefromiduserid = client.get_entity(message.from_id.user_id)
    messagechatid = message.chat_id
    recordId = 1
    messageId = message.id
    messageText = message.text
    isMessageDeleted = False
    isMessageForwarded = False
    isMessageIsFile = False
    messageFileContent = None
    #TG_peer_id = str(message.peer_id)  message.peer_id,  TG_peer_id,


    # Check if message is forwarded, if not null then its forwarded
    if message.fwd_from:
        isMessageForwarded = True

    # Check if message contains media
    if message.photo or message.file:
        isMessageIsFile = True
        # test data
        message.media = r'C:\Users\Administrator\PycharmProjects\LLLLLLLLLLLLLLLLLLLLLLLBLENDERLLLLLLLLLLLLLLLLLLLLL' + '\\'

        # test environment
        #cursor.execute("""\
        #    CREATE TABLE #validation (
        #        photo varbinary(max))
        #    """)

        # save binary file
        with open(message.media + 'vvvssseee.png', 'rb') as photo_file:
            photo_bytes = photo_file.read()
        cursor.execute("INSERT INTO Groupdata (TG_message_is_file_content) VALUES (?)", photo_bytes)
        #cursor.execute("INSERT INTO #validation (photo) VALUES (?)", photo_bytes)
        print(f'{len(photo_bytes)}-byte file written for')

        # retrieve binary data and save as new file
        retrieved_bytes = cursor.execute("SELECT photo FROM #validation").fetchval()
        with open(message.media + 'new3.jpg', 'wb') as new_jpg: #чтобы сохранял новые данные, нужно переименовать new2.jpg нужно написать так, чтобы i + 1 было.
            new_jpg.write(retrieved_bytes)
        print(f'{len(retrieved_bytes)} bytes retrieved and written to new file')
        # 5632 bytes retrieved and written to new file

        messageFileContent = bytes(message.media, encoding='utf-16') #VARBINARY(MAX) nvarchar(max) #bytes(message.media)
        #bytes((create_jsonlines(source)), encoding='utf8')




    # test data
    #photo_path = r'C:\Users\Administrator\PycharmProjects\LLLLLLLLLLLLLLLLLLLLLLLBLENDERLLLLLLLLLLLLLLLLLLLLL' + '\\'

    # test environment
    #cursor.execute("""\
    #CREATE TABLE #validation (
    #    photo varbinary(max))
    #""")

    # save binary file
    #with open(photo_path + 'asssa123.png', 'rb') as photo_file:
    #    photo_bytes = photo_file.read()
    #cursor.execute("INSERT INTO #validation (photo) VALUES (?)", photo_bytes)
    #print(f'{len(photo_bytes)}-byte file written for')


    # retrieve binary data and save as new file
    #retrieved_bytes = cursor.execute("SELECT photo FROM #validation WHERE email = ?", email).fetchval()
    #with open(photo_path + 'new.jpg', 'wb') as new_jpg:
    #    new_jpg.write(retrieved_bytes)
    #print(f'{len(retrieved_bytes)} bytes retrieved and written to new file')
    # 5632 bytes retrieved and written to new file




    # Get connection 2 code for blob file vrode rabotaet
    #connection = pyodbc.connect(...)
    #filename = 'MyFile.pdf' #os.listdir() и функцию glob.glob()
    #for filename in os.listdir("files"):
    #    with open(os.path.join("files", filename), 'r') as f:
    #        text = f.read()
    #        print(text) # для открытие каталог файлов в пути. А дальше пойдет код сравнение файлов с помощью фреймворка filecmp

    # преобразовываем списки файлов
    # каталогов dir1 и dir2 в множества
    #file_dir1 = set(os.listdir('path/to/dir1'))
    #file_dir2 = set(os.listdir('path/to/dir2'))

    # находим пересечение множеств, тем
    # самым получаем общие имена
    #common = list(file_dir1 & file_dir2)

    # Теперь проверяем список common
    # на файлы, чтобы не попался каталог
    #common_files = [
    #    file_name
    #    for file_name in common
    #    if os.path.isfile(os.path.join('path/to/dir1', file_name))
    #]

    # Сравниваем общие файлы каталогов
    #match, mismatch, errors = filecmp.cmpfiles(
    #    'path/to/dir1',
    #    'path/to/dir2',
    #    common_files,
    #)

    #insert = 'insert into FileTable (name, document) values (?,?)'

    # open file as binary and read into a variable
    #with open(filename, 'rb') as f:
    #    bindata = f.read()

    # build query
    #binparams = ('MyFile.pdf', pyodbc.Binary(bindata))

    # insert binary
    #connection.cursor().execute(insert, binparams)
    #connection.commit()
    #connection.close()
    #end_code


    # Insert new record to database
    cursor.execute("INSERT INTO Groupdata (TG_sender_id, TG_user_id, TG_chat_id, TG_message_id, TG_message_date, TG_fwd_from, TG_delete_message, TG_message_text, TG_message_is_file, TG_message_is_file_content) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    (from_user.username, message.from_id.user_id,  message.chat_id, messageId,  message.date, isMessageForwarded, isMessageDeleted, messageText, isMessageIsFile, messageFileContent)
    ) #message.peer_id,  TG_peer_id,  messageFileContent   TG_message_is_file_content

    # TG_Int_ID, TG_sender_id, TG_user_id,TG_chat_id,TG_message_id, TG_message_date, TG_fwd_from, TG_peer_id, TG_delete_message, TG_message_text, TG_message_is_file
    #from_user.username, message.from_id.user_id, message.chat_id, message.id, message.date, message.fwd_from, message.peer_id, message.text
# 7:7:7
    connGG.commit()

    getMaxIdQuery = "SELECT MAX(TG_Int_ID) AS max_id FROM Groupdata"  # проблема тут SELECT TABLE Int_ID FROM useranon  #"SELECT MAX(TG_Int_ID) AS max_id FROM Groupdata"
    cursor.execute(getMaxIdQuery)
    results = cursor.fetchall()
    for row in results:
        from_user.username = row[0],
        message.from_id.user_id = row[1],
        message.chat_id = row[2],
        messageId = row[3],
        message.date = row[4],
        isMessageForwarded = row[5],
        message.peer_id = row[6],
        isMessageDeleted = row[7],
        messageText = row[8],
        isMessageIsFile = row[9],
        messageFileContent = row[10],

    # Get next row ID (Int_ID) for the new record Так как Insert - идет как массив, то снизу идет распеределение
    #getMaxIdQuery = "SELECT MAX(TG_Int_ID) AS max_id FROM Groupdata"  # проблема тут SELECT TABLE Int_ID FROM useranon
    #cursor.execute(getMaxIdQuery)
    #results = cursor.fetchall()
    #for row in results:
    #    recordId = row[0],
    #    from_user.username = row[1],
    #    message.from_id.user_id = row[2],
    #    message.chat_id = row[3],
    #    messageId = row[4],
    #    message.date = row[5],
    #    isMessageForwarded = row[6],
    #    message.peer_id = row[7],
    #    isMessageDeleted = row[8],
    #    messageText = row[9],
    #    isMessageIsFile = row[10],
    #    messageFileContent = row[11],
        # recordId = int(row[0]) + 1 # int(row[0]) + 1


#fast_executemany необходимо заранее определить типы всех параметров, чтобы выделить массив данных параметров. Как показывает ваша трассировка, у pyODBC
# возникают проблемы с определением этих типов, потому что они неоднозначны, и следуют ошибки.
# Лучшее решение (которое также позволяет избежать отправки ненужных данных на сервер) — не делать этого.
# Вот весь ответ решение проблемы pyodbc.ProgrammingError: ('42000', '[42000] [Microsoft][ODBC SQL Server Driver][SQL Server]Error converting data type nvarchar to float. (8114) (SQLExecDirectW)')

connGG.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
connGG.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
connGG.setencoding(encoding='utf-8')

with client:
    client.loop.run_until_complete(main(events))

connGG.close()

"""
global comment 
"""


