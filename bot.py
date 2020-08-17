# - *- coding: utf- 8 - *-
#set global max_allowed_packet=67108864;

# Telegram bot for collect data(handwritten recognition)
# Copyright (c) 2020 NORLIST.kz

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import random
import mysql.connector
import os,json
import traceback

# Constants.
ADMIN_TELEGRAM_ID = 0 # Admin's ID in telegram

UPDATER_ID = ''

MAX_DB_CONNECT_ATTEMPTS = 3

#safe_close = lambda x: x.close(); x = None if (x) else x

def safe_close(x):
    return x.close(); x = None if (x) else x

# db = mysql.connector.connect(
#  host="localhost",
#  user="root",
#  passwd="P@ssw0rd2020",
#  port="3306",
#  database="users",
#)


#cursor.execute("CREATE TABLE user_words (id INT AUTO_INCREMENT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), user_id VARCHAR(255), word VARCHAR(255), nickname VARCHAR(255))")
#cursor.execute("insert into user_words(first_name, last_name, user_id, word, nickname) select first_name, last_name, user_id, word, username from users")
#cursor.execute("CREATE DATABASE users")
#cursor.execute("CREATE TABLE users (first_name VARCHAR(255), last_name VARCHAR(255), user_id VARCHAR(255), word VARCHAR(255), username VARCHAR(255))")
#cursor.execute("TRUNCATE TABLE users")
#db.close()
#exit()
#if os.path.exists("data_statistic_file.json")==True:
#    os.remove("data_statistic_file.json"))



'''with open('filename.txt', 'r', encoding="utf-8") as f:
    for line in f:
        for s in line.split(','):
            if s != ' \n' and s != '\n':
                all_words.append(s)
print(all_words)'''

all_words = []
with open('words_with_kazakh_symbols.csv', 'r') as f:
    for line in f:
        for s in line.split('\n'):
            if len(s) > 2:
                all_words.append(s.strip())



'''if os.path.exists("data_statistic_file.json")==False:
    with open("data_statistic_file.json", "w") as w_file:
        text={"Words":"Count"}
        data = json.dump(text, w_file, ensure_ascii=False)'''

# Connect to DB several times.
# Returns cursor.
def reconnect():

    for i in range(MAX_DB_CONNECT_ATTEMPTS):
        try:
            db = mysql.connector.connect(
                host="localhost",
                user="root",
                passwd="P@ssw0rd2020",
                port="3306",
                database="users",
                connect_timeout=1000
                )
            cursor = db.cursor()
            break
        except Exception as ex:
            print(ex)
            bot.send_message(chat_id = ADMIN_TELEGRAM_ID , text = "MYSQL error connection!")

    if (db is None or cursor is None):
        raise Exception(f'Could not connect ot DB. MAX_DB_CONNECT_ATTEMPTS = {MAX_DB_CONNECT_ATTEMPTS}')

    return db, cursor

def log_error(f):
    def inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            print(f'Ошибка: {e}')
            raise e

    return inner

@log_error
def callback_alarm(bot, update):
    # Selects new word for user and saves result to DB.

    try:
        # DBG
        print('Get user from context...')
        user = update.message.from_user

        # DBG
        print('user: ', user)
        db, cursor = reconnect()
        assert(db and cursor)

        # Get all words that user posted already. 
        cursor.execute("SELECT word FROM user_words WHERE user_id=" + str(user['id']) + ";")

        #assert(cursor)    
        user_words = list(cursor)

        # Randomly get new word for user. 
        user_new_word = random.choice(list(set(all_words) - set(user_words)))
        # DBG
        print('user_new_word: ', user_new_word)
        
        # Save/update users new word in DB (creates new record for new user).
        sql = "INSERT INTO users_last_word (user_id, last_word) VALUES (%s, %s) ON DUPLICATE KEY UPDATE last_word = %s;"
        val = (user['id'], user_new_word, user_new_word)
        cursor.execute(sql, val)
        db.commit()
        # DBG
        print('INSERTED INTO users_last_word: ', user_new_word)
        

        # Send word to user.
        bot.send_message(chat_id = update.message.chat_id, text = user_new_word)
        print('Message sent to user: ', user['id'])

    except Exception as ex:
        print('Exception in callback_alarm: ', ex)
    finally:
        # Release resources.
        safe_close(cursor)
        safe_close(db)


#-------------------------------------------



@log_error
def callback_start(bot, update, job_queue):
    img = open('rahmet.jpg', 'rb')
    bot.send_message(chat_id=update.message.chat_id, text='Қош келдіңіз! Көмегіңіз үшін рақмет!\nБотты қолдану ережелері:\n\t-сөзді тек ақ параққа қолмен жазыңыз;\n\t-суреттi жарық жерде түсіріңіз;\n\t-бір сөзге - бір сурет;\n\t-суретте тек бір ғана сөз болуы керек (бөгде заттар болмауы керек);\n\t-егер фотосурет төңкерілген болса, оны түзетуге тырысыңыз;\n\t-cурет бұлыңғыр болмауы керек;\n\t-cөзді дәл жазыңыз (бас әріптер мен кіші әріптер маңызды);\n\t-Үлгі:')

    bot.sendPhoto(chat_id=update.message.chat_id, photo = img)

    bot.send_message(chat_id=update.message.chat_id, text='Командалар:\n /next - Жаңа сөз алу үшін;\n /total - Өз нәтижеңізді көру үшін;\n\n Жалғастыру үшін /next басыңыз!')

@log_error
def callback_timer(bot, update, job_queue):
    #file_id = update.message.photo
    # newFile = bot.getFile(file_id)
    #print(file_id)
    print('Try to queue NEXT...')
    
    try:
        job_queue.run_once(callback_alarm, 1, context=update)
    except Exception as e:
        print("Error on callback_timer:", e)



@log_error
def stop_timer(bot, update, job_queue):
    bot.send_message(chat_id=update.message.chat_id, text='Stopped!')
    job_queue.stop()


# Custom error when user sends photo without getting a word.
class PhotoWithoutWordError(Exception):
    pass

@log_error
def image_handler(bot, update):
    try:
        file_id = update.message.photo[-1]
        newFile = bot.getFile(file_id)
        
        # Select last user word from DB.
        user = update.message.from_user

        # Get user last word from DB.
        db, cursor = reconnect()
        assert(db and cursor)        
        
        cursor.execute("SELECT last_word FROM users_last_word WHERE user_id = " + str(user['id'])+ ";")
        rs_last_word = cursor.fetchone()
        
        # If user sent photo without getting word.
        if (rs_last_word is None):
            raise PhotoWithoutWordError

        user_last_word = rs_last_word[0]
        print('user_last_word: ', user_last_word)

        # Save image.
        img_path = "image/"+str(user['id'])+"_" + user_last_word.replace(' ', '_') + ".jpg"
        ann_path = "json_file/"+str(user['id'])+"_" + user_last_word.replace(' ', '_') + ".json"
        newFile.download(custom_path = img_path)
        assert(os.path.exists(img_path))
        print('image saved: ', img_path)

        # Save annotation.
        data = {"name": str(user['id'])+"_" +user_last_word.replace(' ', '_'), "description": user_last_word}
        with open(ann_path, "w", encoding="utf-8") as write_file:
            json.dump(data, write_file, ensure_ascii=False)

        # Save new word in users list in DB.
        sql = "INSERT INTO user_words (first_name, last_name, user_id, word, nickname) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE user_id=%s, word=%s"
        val = (user['first_name'], user['last_name'], user['id'], user_last_word, user['username'], user['id'], user_last_word)
        cursor.execute(sql, val)
        db.commit()
        print('commited: ', user_last_word)

        # Send message to user.  
        bot.sendMessage(chat_id=update.message.chat_id, text = "*" + user_last_word + "* сөзі сақталды!\n Егер жөндегіңіз келсе, фотоны қайта жіберіңіз.\n Жалғастыру үшін /next басыңыз!", parse_mode = 'Markdown')
        print('Message sent to user: ', user['id'])

    except PhotoWithoutWordError as pex:
        bot.sendMessage(chat_id = update.message.chat_id, text="Алдымен /next басыңыз!")
    except Exception as ex:
        print('Exception in image_handler', ex)
    finally:
        # Release resources.
        safe_close(cursor)
        safe_close(db)
        newFile = None

@log_error
def total(bot, update):

    try:
        user = update.message.from_user
        
        db, cursor = reconnect()
        assert(db and cursor)
        
        cursor.execute("SELECT count(*) FROM user_words WHERE user_id = " + str(user['id'])+ ";")
        rs_total_count = cursor.fetchone()
        assert(rs_total_count is not None)

        # Send message to user.  
        bot.sendMessage(chat_id = update.message.chat_id, text = "Сіз барлығы " + str(rs_total_count[0]) + " сөз жібердіңіз.\nҮлкен рахмет!")
        print('Message sent to user: ', user['id'])

    except Exception as ex:
        print('Exception in image_handler', ex)
    finally:
        # Release resources.
        rs_total_count = None
        safe_close(cursor)
        safe_close(db)


@log_error
def admin(bot, update):
    try:
        db, cursor = reconnect()
        assert(db and cursor)

        cursor.execute("SELECT first_name, last_name, count(*) AS cnt FROM user_words GROUP BY first_name, last_name, user_id ORDER BY cnt DESC;")
    
        user_stat_txt = ''
        total_words = 0
        
        for rs in cursor.fetchall():
            user_stat_txt += str(rs[0] or '') + ' ' + str(rs[1] or '') + ': ' + str(rs[2]) + '\n'
            total_words += rs[2]
        
        user_stat_txt += '\n*Total*: ' + str(total_words) + '.\n'
        # Send message to user.  
        bot.sendMessage(chat_id = update.message.chat_id, text = user_stat_txt)
        print('Message sent to admin.')

    except Exception as ex:
        print('Exception in admin', ex)
        print(traceback.format_exc())
    finally:
        # Release resources.
        rs = None
        safe_close(cursor)
        safe_close(db)


updater = Updater(UPDATER_ID)

updater.dispatcher.add_handler(CommandHandler('start', callback_start, pass_job_queue=True))
#updater.dispatcher.add_handler(CommandHandler('next', callback_timer, pass_job_queue=True))
updater.dispatcher.add_handler(CommandHandler('next', callback_alarm))
updater.dispatcher.add_handler(CommandHandler('stop', stop_timer, pass_job_queue=True))
updater.dispatcher.add_handler(CommandHandler('admin', admin))
updater.dispatcher.add_handler(CommandHandler('total', total))
updater.dispatcher.add_handler(MessageHandler(Filters.photo, image_handler))


updater.start_polling()







#ALTER TABLE users.users MODIFY COLUMN word VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL;
#UPDATE mysql.user SET authentication_string=PASSWORD('root') WHERE user='root';

#CREATE TABLE pet (first_name VARCHAR(20), last_name VARCHAR(20), user_id INT(10), word VARCHAR(20), username VARCHAR(20));
