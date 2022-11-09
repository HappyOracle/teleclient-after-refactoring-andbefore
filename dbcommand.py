import sys
import pyodbc


connGG = pyodbc.connect('Driver={SQL SERVER};'
                        'Server=10.110.10.29;'
                        'Database=DB-TG;'
                        'UID=sa;'
                        'PWD=Aa123456;'
                        'autocommit=True')


cursor = connGG.cursor()

userXs = "ALTER TABLE useranon DROP COLUMN Int_ID"
#useranon = "ALTER TABLE useranon ADD Int_ID INT IDENTITY(1,1)"

userconn = "SELECT * FROM useranon"
cursor.execute(userconn)


# Шаблон для внесение изменений.
#SQLinsert = '''INSERT INTO useranon (Int_ID, TG_message_id, TG_message_text, TG_message_is_deleted, TG_message_is_forwarded, TG_message_is_file, TG_message_file_content)
#                VALUES (?,?,?,?,?,?,?);'''


results = cursor.fetchall() #[0] fetchall
for row in results:
    Int_ID = row[0]# row(int[0])
    TG_message_id = row[1]
    TG_message_text = row[2]
    TG_message_is_deleted = row[3]
    TG_message_is_forwarded = row[4]
    TG_message_is_file = row[5]
    TG_message_file_content = row[6]

    values = (row[0], row[1], row[2], row[3], row[4], row[5], row[6])
    #print(Int_ID, TG_message_id, TG_message_text, TG_message_is_deleted, TG_message_is_forwarded, TG_message_is_file, TG_message_file_content)　# коммент
print(values)
#cursor.execute(SQLinsert, results()) # не работает
#cursor.execute("SELECT * from useranon") #работает, но что-то не так...
#connGG.commit()

# Примерный шаблон, то как должен выглядеть
#connGG.execute('''INSERT INTO useranon(Int_ID, TG_message_id, TG_message_text, TG_message_is_deleted, TG_message_is_forwarded, TG_message_is_file, TG_message_file_content)
#                VALUES (?,?,?,?,?,?,?);''', (12, 222, 'dead inside 1000-7', 1, 0, 0, 0))

#for group table
connGG.execute('''CREATE TABLE Groupdata(
            TG_Int_ID int,
            TG_sender_id float,
            TG_user_id float,
            TG_chat_id float,
            TG_message_id float,
            TG_message_date datetime,
            TG_fwd_from bit,
            TG_peer_id nvarchar (max),
            TG_delete_message bit,
            TG_message_text nvarchar(max),
            TG_message_is_file bit,
            TG_message_is_file_content varbinary(max)
            )
            ''')

#for user table
#connGG.execute('''CREATE TABLE Userdata(
#            TG_Int_ID int,
#            TG_user_id float,
##            TG_message_id float,
##            TG_message_date datetime,
#            TG_fwd_from bit,
#            TG_peer_id nvarchar (max),
#            TG_delete_message bit,
#            TG_message_text nvarchar(max),
#            TG_message_is_file bit,
#            TG_message_is_file_content varbinary(max)
#            )
#            ''')


#Main table
#connGG.execute('''
#                CREATE TABLE Mainlist2(
#                TG_Int_ID int primary key,
#                Telephone_Number float,
#                LF_Name nvarchar(255),
#                TG_user_id float,
#                )
#                ''')



connGG.commit()

cursor.execute('SELECT * FROM Mainlist')

connGG.close()
